/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fusesource.amqpjms.jms;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.net.ssl.SSLContext;

import org.fusesource.amqpjms.jms.exceptions.JmsConnectionFailedException;
import org.fusesource.amqpjms.jms.exceptions.JmsExceptionSupport;
import org.fusesource.amqpjms.jms.message.JmsInboundMessageDispatch;
import org.fusesource.amqpjms.jms.message.JmsMessage;
import org.fusesource.amqpjms.jms.message.JmsOutboundMessageDispatch;
import org.fusesource.amqpjms.jms.meta.JmsConnectionId;
import org.fusesource.amqpjms.jms.meta.JmsConnectionInfo;
import org.fusesource.amqpjms.jms.meta.JmsConsumerId;
import org.fusesource.amqpjms.jms.meta.JmsResource;
import org.fusesource.amqpjms.jms.meta.JmsSessionId;
import org.fusesource.amqpjms.jms.util.IdGenerator;
import org.fusesource.amqpjms.jms.util.ThreadPoolUtils;
import org.fusesource.amqpjms.provider.BlockingProvider;
import org.fusesource.amqpjms.provider.ProviderListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a JMS Connection
 */
public class JmsConnection implements Connection, TopicConnection, QueueConnection, ProviderListener {

    private static final Logger LOG = LoggerFactory.getLogger(JmsConnection.class);

    private JmsConnectionInfo connectionInfo;

    private final IdGenerator clientIdGenerator;
    private boolean clientIdSet;
    private ExceptionListener exceptionListener;
    private final List<JmsSession> sessions = new CopyOnWriteArrayList<JmsSession>();
    private final Map<JmsConsumerId, JmsMessageDispatcher> dispatchers =
        new ConcurrentHashMap<JmsConsumerId, JmsMessageDispatcher>();
    private final AtomicBoolean connected = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean closing = new AtomicBoolean();
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean failed = new AtomicBoolean();
    private final Object connectLock = new Object();
    private IOException firstFailureError;
    private JmsPrefetchPolicy prefetchPolicy = new JmsPrefetchPolicy();

    private final ThreadPoolExecutor executor;

    private URI brokerURI;
    private URI localURI;
    private SSLContext sslContext;
    private BlockingProvider provider;
    private final Set<JmsConnectionListener> connectionListeners =
        new CopyOnWriteArraySet<JmsConnectionListener>();

    private final AtomicLong sessionIdGenerator = new AtomicLong();

    protected JmsConnection(String connectionId, BlockingProvider provider, IdGenerator clientIdGenerator) throws JMSException {

        // This executor can be used for dispatching asynchronous tasks that might block or result
        // in reentrant calls to this Connection that could block.  The thread in this executor
        // will also serve as a means of preventing JVM shutdown should a client application
        // not have it's own mechanism for doing so.
        executor = new ThreadPoolExecutor(1, 1, 5, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "AmqpJMS Connection Executor: ");
                return thread;
            }
        });

        this.provider = provider;
        this.provider.setProviderListener(this);
        this.clientIdGenerator = clientIdGenerator;
        this.connectionInfo = new JmsConnectionInfo(new JmsConnectionId(connectionId));
    }

    /**
     * @throws JMSException
     * @see javax.jms.Connection#close()
     */
    @Override
    public void close() throws JMSException {
        if (closed.compareAndSet(false, true)) {
            try {
                for (JmsSession session : this.sessions) {
                    session.shutdown();
                }
                this.sessions.clear();

                if (provider != null) {
                    try {
                        provider.destroy(connectionInfo);
                    } catch (IOException e) {
                        LOG.trace("Cuaght exception while closing remote connection: {}", e.getMessage());
                    }

                    provider.close();
                    provider = null;
                }
            } catch (Exception e) {
                throw JmsExceptionSupport.create(e);
            } finally {
                try {
                    if (executor != null) {
                        ThreadPoolUtils.shutdown(executor);
                    }
                } catch (Throwable e) {
                    LOG.warn("Error shutting down thread pool: " + executor + ". This exception will be ignored.", e);
                }
            }
        }
    }

    /**
     * Called to free all Connection resources.
     */
    protected void shutdown() throws JMSException {

        // TODO - Once ConnectionConsumer is added we must shutdown those as well.

        for (JmsSession session : this.sessions) {
            session.shutdown();
        }

        if (isConnected()) {
            if (!failed.get() && !closing.get()) {
                destroyResource(connectionInfo);
            }
            connected.set(false);
        }

        if (clientIdSet) {
            connectionInfo.setClientId(null);
            clientIdSet = false;
        }

        started.set(false);
    }

    /**
     * @param destination
     * @param messageSelector
     * @param sessionPool
     * @param maxMessages
     * @return ConnectionConsumer
     * @throws JMSException
     * @see javax.jms.Connection#createConnectionConsumer(javax.jms.Destination,
     *      java.lang.String, javax.jms.ServerSessionPool, int)
     */
    @Override
    public ConnectionConsumer createConnectionConsumer(Destination destination, String messageSelector,
                                                       ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        checkClosedOrFailed();
        connect();
        throw new JMSException("Not supported");
    }

    /**
     * @param topic
     * @param subscriptionName
     * @param messageSelector
     * @param sessionPool
     * @param maxMessages
     * @return ConnectionConsumer
     * @throws JMSException
     *
     * @see javax.jms.Connection#createDurableConnectionConsumer(javax.jms.Topic,
     *      java.lang.String, java.lang.String, javax.jms.ServerSessionPool, int)
     */
    @Override
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName,
                                                              String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        checkClosedOrFailed();
        connect();
        throw new JMSException("Not supported");
    }

    /**
     * @param transacted
     * @param acknowledgeMode
     * @return Session
     * @throws JMSException
     * @see javax.jms.Connection#createSession(boolean, int)
     */
    @Override
    public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException {
        checkClosedOrFailed();
        connect();
        int ackMode = getSessionAcknowledgeMode(transacted, acknowledgeMode);
        JmsSession result = new JmsSession(this, getNextSessionId(), ackMode);
        addSession(result);
        if (started.get()) {
            result.start();
        }
        return result;
    }

    /**
     * @return clientId
     * @see javax.jms.Connection#getClientID()
     */
    @Override
    public String getClientID() throws JMSException {
        checkClosedOrFailed();
        return this.connectionInfo.getClientId();
    }

    /**
     * @return connectionInfoData
     * @see javax.jms.Connection#getMetaData()
     */
    @Override
    public ConnectionMetaData getMetaData() throws JMSException {
        checkClosedOrFailed();
        return JmsConnectionMetaData.INSTANCE;
    }

    /**
     * @param clientID
     * @throws JMSException
     * @see javax.jms.Connection#setClientID(java.lang.String)
     */
    @Override
    public synchronized void setClientID(String clientID) throws JMSException {
        checkClosedOrFailed();

        if (this.clientIdSet) {
            throw new IllegalStateException("The clientID has already been set");
        }
        if (clientID == null) {
            throw new IllegalStateException("Cannot have a null clientID");
        }
        if (connected.get()) {
            throw new IllegalStateException("Cannot set the client id once connected.");
        }

        this.connectionInfo.setClientId(clientID);
        this.clientIdSet = true;
    }

    /**
     * @throws JMSException
     * @see javax.jms.Connection#start()
     */
    @Override
    public void start() throws JMSException {
        checkClosedOrFailed();
        connect();
        if (this.started.compareAndSet(false, true)) {
            try {
                for (JmsSession s : this.sessions) {
                    s.start();
                }
            } catch (Exception e) {
                throw JmsExceptionSupport.create(e);
            }
        }
    }

    /**
     * @throws JMSException
     * @see javax.jms.Connection#stop()
     */
    @Override
    public void stop() throws JMSException {
        checkClosedOrFailed();
        connect();
        if (this.started.compareAndSet(true, false)) {
            try {
                for (JmsSession s : this.sessions) {
                    s.stop();
                }
            } catch (Exception e) {
                throw JmsExceptionSupport.create(e);
            }
        }
    }

    /**
     * @param topic
     * @param messageSelector
     * @param sessionPool
     * @param maxMessages
     * @return ConnectionConsumer
     * @throws JMSException
     * @see javax.jms.TopicConnection#createConnectionConsumer(javax.jms.Topic,
     *      java.lang.String, javax.jms.ServerSessionPool, int)
     */
    @Override
    public ConnectionConsumer createConnectionConsumer(Topic topic, String messageSelector,
                                                       ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        checkClosedOrFailed();
        connect();
        return null;
    }

    /**
     * @param transacted
     * @param acknowledgeMode
     * @return TopicSession
     * @throws JMSException
     * @see javax.jms.TopicConnection#createTopicSession(boolean, int)
     */
    @Override
    public TopicSession createTopicSession(boolean transacted, int acknowledgeMode) throws JMSException {
        checkClosedOrFailed();
        connect();
        int ackMode = getSessionAcknowledgeMode(transacted, acknowledgeMode);
        JmsTopicSession result = new JmsTopicSession(this, getNextSessionId(), ackMode);
        addSession(result);
        if (started.get()) {
            result.start();
        }
        return result;
    }

    /**
     * @param queue
     * @param messageSelector
     * @param sessionPool
     * @param maxMessages
     * @return ConnectionConsumer
     * @throws JMSException
     * @see javax.jms.QueueConnection#createConnectionConsumer(javax.jms.Queue,
     *      java.lang.String, javax.jms.ServerSessionPool, int)
     */
    @Override
    public ConnectionConsumer createConnectionConsumer(Queue queue, String messageSelector,
                                                       ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        checkClosedOrFailed();
        connect();
        return null;
    }

    /**
     * @param transacted
     * @param acknowledgeMode
     * @return QueueSession
     * @throws JMSException
     * @see javax.jms.QueueConnection#createQueueSession(boolean, int)
     */
    @Override
    public QueueSession createQueueSession(boolean transacted, int acknowledgeMode) throws JMSException {
        checkClosedOrFailed();
        connect();
        int ackMode = getSessionAcknowledgeMode(transacted, acknowledgeMode);
        JmsQueueSession result = new JmsQueueSession(this, getNextSessionId(), ackMode);
        addSession(result);
        if (started.get()) {
            result.start();
        }
        return result;
    }

    /**
     * @param ex
     */
    public void onException(Exception ex) {
        onException(JmsExceptionSupport.create(ex));
    }

    /**
     * @param ex
     */
    public void onException(JMSException ex) {
        ExceptionListener l = this.exceptionListener;
        if (l != null) {
            l.onException(JmsExceptionSupport.create(ex));
        }
    }

    protected int getSessionAcknowledgeMode(boolean transacted, int acknowledgeMode) throws JMSException {
        int result = acknowledgeMode;
        if (!transacted && acknowledgeMode == Session.SESSION_TRANSACTED) {
            throw new JMSException("acknowledgeMode SESSION_TRANSACTED cannot be used for an non-transacted Session");
        }
        if (transacted) {
            result = Session.SESSION_TRANSACTED;
        }
        return result;
    }

    protected void removeSession(JmsSession session) throws JMSException {
        this.sessions.remove(session);
    }

    protected void addSession(JmsSession s) {
        this.sessions.add(s);
    }

    protected void addDispatcher(JmsConsumerId consumerId, JmsMessageDispatcher dispatcher) {
        dispatchers.put(consumerId, dispatcher);
    }

    protected void removeDispatcher(JmsConsumerId consumerId) {
        dispatchers.remove(consumerId);
    }

    private void connect() throws JMSException {
        synchronized(this.connectLock) {
            if (isConnected() || closed.get()) {
                return;
            }

            if (connectionInfo.getClientId() == null || connectionInfo.getClientId().trim().isEmpty()) {
                connectionInfo.setClientId(clientIdGenerator.generateId());
            }

            this.connectionInfo = createResource(connectionInfo);
            this.connected.set(true);

            // TODO - Advisory Support.
            //
            // Providers should have an interface for adding a listener for temporary
            // destination advisory messages for create / destroy so we can track them
            // and throw exceptions when producers try to send to deleted destinations.
        }
    }

    void deleteDestination(JmsDestination destination) throws JMSException {
        checkClosedOrFailed();
        connect();

        try {

            for (JmsSession session : this.sessions) {
                if (session.isDestinationInUse(destination)) {
                    throw new JMSException("A consumer is consuming from the temporary destination");
                }
            }

            // Provider delete if supported.

            // TODO if we track temporary destinations and this happens to be one
            //      we need to clean up our internal state.
            if (destination.isTemporary()) {
            }
        } catch (Exception e) {
            throw JmsExceptionSupport.create(e);
        }
    }

    protected void checkClosedOrFailed() throws JMSException {
        checkClosed();
        if (failed.get()) {
            throw new JmsConnectionFailedException(firstFailureError);
        }
    }

    protected void checkClosed() throws IllegalStateException {
        if (this.closed.get()) {
            throw new IllegalStateException("The Connection is closed");
        }
    }

    protected JmsSessionId getNextSessionId() {
        return new JmsSessionId(connectionInfo.getConnectionId(), sessionIdGenerator.incrementAndGet());
    }

    @SuppressWarnings("unchecked")
    <T extends JmsResource> T createResource(T resource) throws JMSException {
        checkClosedOrFailed();

        try {
            return (T) provider.create(resource);
        } catch (Exception ex) {
            throw JmsExceptionSupport.create(ex);
        }
    }

    void destroyResource(JmsResource resource) throws JMSException {
        connect();

        // TODO - We don't currently have a way to say that an operation
        //        should be done asynchronously.  For a session dispose
        //        we only care that the request hits the wire, not that
        //        any response comes back.

        try {
            provider.destroy(resource);
        } catch (Exception ioe) {
            throw JmsExceptionSupport.create(ioe);
        }
    }

    void send(JmsOutboundMessageDispatch envelope) throws JMSException {
        checkClosedOrFailed();
        connect();

        // TODO - We don't currently have a way to say that an operation
        //        should be done asynchronously.  For a session dispose
        //        we only care that the request hits the wire, not that
        //        any response comes back.

        try {
            provider.send(envelope);
        } catch (Exception ioe) {
            throw JmsExceptionSupport.create(ioe);
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // Property setters and getters
    ////////////////////////////////////////////////////////////////////////////

    /**
     * @return ExceptionListener
     * @see javax.jms.Connection#getExceptionListener()
     */
    @Override
    public ExceptionListener getExceptionListener() throws JMSException {
        checkClosedOrFailed();
        return this.exceptionListener;
    }

    /**
     * @param listener
     * @see javax.jms.Connection#setExceptionListener(javax.jms.ExceptionListener)
     */
    @Override
    public void setExceptionListener(ExceptionListener listener) throws JMSException {
        checkClosedOrFailed();
        this.exceptionListener = listener;
    }

    /**
     * Adds a JmsConnectionListener so that a client can be notified of events in
     * the underlying protocol provider.
     *
     * @param listener
     *        the new listener to add to the collection.
     */
    public void addConnectionListener(JmsConnectionListener listener) {
        this.connectionListeners.add(listener);
    }

    /**
     * Removes a JmsConnectionListener that was previously registered.
     *
     * @param listener
     *        the listener to remove from the collection.
     */
    public void removeTransportListener(JmsConnectionListener listener) {
        this.connectionListeners.remove(listener);
    }

    public boolean isForceAsyncSend() {
        return connectionInfo.isForceAsyncSend();
    }

    public void setForceAsyncSend(boolean forceAsyncSend) {
        connectionInfo.setForceAsyncSends(forceAsyncSend);
    }

    public String getTopicPrefix() {
        return connectionInfo.getTopicPrefix();
    }

    public void setTopicPrefix(String topicPrefix) {
        connectionInfo.setTopicPrefix(topicPrefix);
    }

    public String getTempTopicPrefix() {
        return connectionInfo.getTempTopicPrefix();
    }

    public void setTempTopicPrefix(String tempTopicPrefix) {
        connectionInfo.setTempTopicPrefix(tempTopicPrefix);
    }

    public String getTempQueuePrefix() {
        return connectionInfo.getTempQueuePrefix();
    }

    public void setTempQueuePrefix(String tempQueuePrefix) {
        connectionInfo.setTempQueuePrefix(tempQueuePrefix);
    }

    public String getQueuePrefix() {
        return connectionInfo.getQueuePrefix();
    }

    public void setQueuePrefix(String queuePrefix) {
        connectionInfo.setQueuePrefix(queuePrefix);
    }

    public boolean isOmitHost() {
        return connectionInfo.isOmitHost();
    }

    public void setOmitHost(boolean omitHost) {
        connectionInfo.setOmitHost(omitHost);
    }

    public JmsPrefetchPolicy getPrefetchPolicy() {
        return prefetchPolicy;
    }

    public void setPrefetchPolicy(JmsPrefetchPolicy prefetchPolicy) {
        this.prefetchPolicy = prefetchPolicy;
    }

    public long getDisconnectTimeout() {
        return connectionInfo.getDisconnectTimeout();
    }

    public void setDisconnectTimeout(long disconnectTimeout) {
        connectionInfo.setDisconnectTimeout(disconnectTimeout);
    }

    public URI getBrokerURI() {
        return brokerURI;
    }

    public void setBrokerURI(URI brokerURI) {
        this.brokerURI = brokerURI;
    }

    public URI getLocalURI() {
        return localURI;
    }

    public void setLocalURI(URI localURI) {
        this.localURI = localURI;
    }

    public SSLContext getSslContext() {
        return sslContext;
    }

    public void setSslContext(SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    public String getUsername() {
        return this.connectionInfo.getUsername();
    }

    public void setUsername(String username) {
        this.connectionInfo.setUsername(username);;
    }

    public String getPassword() {
        return this.connectionInfo.getPassword();
    }

    public void setPassword(String password) {
        this.connectionInfo.setPassword(password);
    }

    BlockingProvider getProvider() {
        return provider;
    }

    void setProvider(BlockingProvider provider) {
        this.provider = provider;
    }

    boolean isConnected() {
        return this.connected.get();
    }

    @Override
    public void onMessage(JmsInboundMessageDispatch envelope) {

        JmsMessage incoming = envelope.getMessage();
        // Ensure incoming Messages are in readonly mode.
        if (incoming != null) {
            incoming.setReadOnlyBody(true);
            incoming.setReadOnlyProperties(true);
        }

        JmsMessageDispatcher dispatcher = dispatchers.get(envelope.getConsumerId());
        if (dispatcher != null) {
            dispatcher.onMessage(envelope);
        }
        for (JmsConnectionListener listener : connectionListeners) {
            listener.onMessage(envelope);
        }
    }

    @Override
    public void onConnectionInterrupted() {
        // TODO Auto-generated method stub
        for (JmsConnectionListener listener : connectionListeners) {
            listener.onConnectionInterrupted();
        }
    }

    @Override
    public void onConnectionRecovery(BlockingProvider provider) {
        // TODO - Recover Advisory Consumer ?
        //        Recover Temporary Destinations ?
        for (JmsSession session : sessions) {
            session.onConnectionRecovery(provider);
        }
    }

    @Override
    public void onConnectionRestored() {
        // TODO Auto-generated method stub
        for (JmsConnectionListener listener : connectionListeners) {
            listener.onConnectionRestored();
        }
    }

    @Override
    public void onConnectionFailure(final IOException ex) {
        onAsyncException(ex);
        if (!closing.get() && !closed.get()) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    providerFailed(ex);
                    if (provider != null) {
                        try {
                            provider.close();
                        } catch (Throwable error) {
                            LOG.debug("Error while closing failed Provider: {}", error.getMessage());
                        }
                    }

                    try {
                        shutdown();
                    } catch (JMSException e) {
                        LOG.warn("Exception during connection cleanup, " + e, e);
                    }

                    for (JmsConnectionListener listener : connectionListeners) {
                        listener.onConnectionFailure(ex);
                    }
                }
            });
        }
    }

    /**
     * Handles any asynchronous errors that occur from the JMS framework classes.
     *
     * If any listeners are registered they will be notified of the error from a thread
     * in the Connection's Executor service.
     *
     * @param error
     *        The exception that triggered this error.
     */
    public void onAsyncException(Throwable error) {
        if (!closed.get() && !closing.get()) {
            if (this.exceptionListener != null) {

                if (!(error instanceof JMSException)) {
                    error = JmsExceptionSupport.create(error);
                }
                final JMSException jmsError = (JMSException)error;

                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        JmsConnection.this.exceptionListener.onException(jmsError);
                    }
                });
            } else {
                LOG.debug("Async exception with no exception listener: " + error, error);
            }
        }
    }

    protected void providerFailed(IOException error) {
        failed.set(true);
        if (firstFailureError == null) {
            firstFailureError = error;
        }
    }
}
