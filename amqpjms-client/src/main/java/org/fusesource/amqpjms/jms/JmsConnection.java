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

import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;

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

/**
 * Implementation of a JMS Connection
 */
public class JmsConnection implements Connection, TopicConnection, QueueConnection {

    private String clientId;
    private final int clientNumber = 0;
    private boolean clientIdSet;
    private ExceptionListener exceptionListener;
    //private final List<JmsSession> sessions = new CopyOnWriteArrayList<JmsSession>();
    private final AtomicBoolean connected = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean started = new AtomicBoolean();
    String queuePrefix = "/queue/";
    String topicPrefix = "/topic/";
    String tempQueuePrefix = "/temp-queue/";
    String tempTopicPrefix = "/temp-topic/";

    boolean forceAsyncSend;
    boolean omitHost;

    final URI brokerURI;
    final URI localURI;
    final String userName;
    final String password;
    final SSLContext sslContext;
    //StompChannel channel;
    long disconnectTimeout = 10000;

    //StompJmsPrefetch prefetch = new StompJmsPrefetch();

    /**
     * @param brokerURI
     * @param localURI
     * @param userName
     * @param password
     * @throws JMSException
     */
    protected JmsConnection(URI brokerURI, URI localURI, String userName, String password, SSLContext sslContext) throws JMSException {
        this.brokerURI = brokerURI;
        this.localURI = localURI;
        this.userName = userName;
        this.password = password;
        this.sslContext = sslContext;
    }

    /**
     * @throws JMSException
     * @see javax.jms.Connection#close()
     */
    @Override
    public synchronized void close() throws JMSException {
        if (closed.compareAndSet(false, true)) {
//            try {
//                for (Session s : this.sessions) {
//                    s.close();
//                }
//                this.sessions.clear();
//                if (channel != null) {
//                    channel.close();
//                    channel = null;
//                }
//            } catch (Exception e) {
//                throw StompJmsExceptionSupport.create(e);
//            }
        }
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
        checkClosed();
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
     * @see javax.jms.Connection#createDurableConnectionConsumer(javax.jms.Topic,
     *      java.lang.String, java.lang.String, javax.jms.ServerSessionPool,
     *      int)
     */
    @Override
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName,
                                                              String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        checkClosed();
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
    public synchronized Session createSession(boolean transacted, int acknowledgeMode) throws JMSException {
        checkClosed();
        connect();
        int ackMode = getSessionAcknowledgeMode(transacted, acknowledgeMode);
//        StompJmsSession result = new StompJmsSession(this, ackMode, forceAsyncSend);
//        addSession(result);
//        if (started.get()) {
//            result.start();
//        }
//        return result;
        return null;
    }

    /**
     * @return clientId
     * @see javax.jms.Connection#getClientID()
     */
    @Override
    public String getClientID() {
        return this.clientId;
    }

    /**
     * @return ExceptionListener
     * @see javax.jms.Connection#getExceptionListener()
     */
    @Override
    public ExceptionListener getExceptionListener() {
        return this.exceptionListener;
    }

    /**
     * @return ConnectionMetaData
     * @see javax.jms.Connection#getMetaData()
     */
    @Override
    public ConnectionMetaData getMetaData() {
        return JmsConnectionMetaData.INSTANCE;
    }

    /**
     * @param clientID
     * @throws JMSException
     * @see javax.jms.Connection#setClientID(java.lang.String)
     */
    @Override
    public synchronized void setClientID(String clientID) throws JMSException {
        if (this.clientIdSet) {
            throw new IllegalStateException("The clientID has already been set");
        }
        if (clientID == null) {
            throw new IllegalStateException("Cannot have a null clientID");
        }
        if( connected.get() ) {
            throw new IllegalStateException("Cannot set the client id once connected.");
        }
        this.clientId = clientID;
        this.clientIdSet = true;
    }

    /**
     * @param listener
     * @see javax.jms.Connection#setExceptionListener(javax.jms.ExceptionListener)
     */
    @Override
    public void setExceptionListener(ExceptionListener listener) {
        this.exceptionListener = listener;
    }

    /**
     * @throws JMSException
     * @see javax.jms.Connection#start()
     */
    @Override
    public void start() throws JMSException {
        checkClosed();
        connect();
        if (this.started.compareAndSet(false, true)) {
//            try {
//                for (StompJmsSession s : this.sessions) {
//                    s.start();
//                }
//            } catch (Exception e) {
//                throw JmsExceptionSupport.create(e);
//            }
        }
    }

    /**
     * @throws JMSException
     * @see javax.jms.Connection#stop()
     */
    @Override
    public void stop() throws JMSException {
        checkClosed();
        connect();
        if (this.started.compareAndSet(true, false)) {
            try {
//                for (StompJmsSession s : this.sessions) {
//                    s.stop();
//                }
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
        checkClosed();
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
        checkClosed();
        connect();
        int ackMode = getSessionAcknowledgeMode(transacted, acknowledgeMode);
//        StompJmsTopicSession result = new StompJmsTopicSession(this, ackMode, forceAsyncSend);
//        addSession(result);
//        if (started.get()) {
//            result.start();
//        }
//        return result;
        return null;
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
        checkClosed();
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
        checkClosed();
        connect();
        int ackMode = getSessionAcknowledgeMode(transacted, acknowledgeMode);
//        StompJmsQueueSession result = new StompJmsQueueSession(this, ackMode, forceAsyncSend);
//        addSession(result);
//        if (started.get()) {
//            result.start();
//        }
//        return result;
        return null;
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

//    protected synchronized StompChannel createChannel() throws JMSException {
//        StompChannel rc = new StompChannel();
//        rc.setBrokerURI(brokerURI);
//        rc.setLocalURI(localURI);
//        rc.setUserName(userName);
//        rc.setPassword(password);
//        rc.setClientId(clientId);
//        rc.setOmitHost(omitHost);
//        rc.setSslContext(sslContext);
//        rc.setDisconnectTimeout(disconnectTimeout);
//        rc.setExceptionListener(this.exceptionListener);
//        rc.setChannelId(clientId + "-" + clientNumber++);
//        return rc;
//    }
//
//    protected StompChannel getChannel() throws JMSException {
//        StompChannel rc;
//        synchronized (this) {
//            if(channel == null) {
//                channel = createChannel();
//            }
//            rc = channel;
//        }
//        rc.connect();
//        return rc;
//    }
//
//    protected StompChannel createChannel(StompJmsSession s) throws JMSException {
//        checkClosed();
//        StompChannel rc;
//        synchronized (this) {
//            if(channel != null) {
//                rc = channel;
//                channel = null;
//            } else {
//                rc = createChannel();
//            }
//        }
//        rc.connect();
//        rc.setListener(s);
//        return rc;
//    }
//
//    protected void removeSession(StompJmsSession s, StompChannel channel) throws JMSException {
//        synchronized (this) {
//            this.sessions.remove(s);
//            if( channel!=null && this.channel==null ) {
//                // just in case some one is in a loop creating/closing sessions.
//                this.channel = channel;
//                channel = null;
//            }
//        }
//        if(channel!=null) {
//            channel.setListener(null);
//            channel.close();
//        }
//    }
//
//    protected void addSession(StompJmsSession s) {
//        this.sessions.add(s);
//    }

    protected void checkClosed() throws IllegalStateException {
        if (this.closed.get()) {
            throw new IllegalStateException("The MessageProducer is closed");
        }
    }

    private void connect() throws JMSException {
        if (connected.compareAndSet(false, true)) {
//            getChannel();
        }
    }

    public boolean isForceAsyncSend() {
        return forceAsyncSend;
    }

    /**
     * If set to true then all mesage sends are done async.
     * @param forceAsyncSend
     */
    public void setForceAsyncSend(boolean forceAsyncSend) {
        this.forceAsyncSend = forceAsyncSend;
    }

    public String getTopicPrefix() {
        return topicPrefix;
    }

    public void setTopicPrefix(String topicPrefix) {
        this.topicPrefix = topicPrefix;
    }

    public String getTempTopicPrefix() {
        return tempTopicPrefix;
    }

    public void setTempTopicPrefix(String tempTopicPrefix) {
        this.tempTopicPrefix = tempTopicPrefix;
    }

    public String getTempQueuePrefix() {
        return tempQueuePrefix;
    }

    public void setTempQueuePrefix(String tempQueuePrefix) {
        this.tempQueuePrefix = tempQueuePrefix;
    }

    public String getQueuePrefix() {
        return queuePrefix;
    }

    public void setQueuePrefix(String queuePrefix) {
        this.queuePrefix = queuePrefix;
    }

    public boolean isOmitHost() {
        return omitHost;
    }

    public void setOmitHost(boolean omitHost) {
        this.omitHost = omitHost;
    }

//    StompJmsTempQueue isTempQueue(String value) throws JMSException {
//        connect();
//        return serverAdaptor().isTempQueue(this, value);
//    }
//
//    StompServerAdaptor serverAdaptor() throws JMSException {
//        return getChannel().getServerAdaptor();
//    }
//
//    StompJmsTempTopic isTempTopic(String value) throws JMSException {
//        connect();
//        return serverAdaptor().isTempTopic(this, value);
//    }
//
//    public StompJmsPrefetch getPrefetch() {
//        return prefetch;
//    }
//
//    public void setPrefetch(StompJmsPrefetch prefetch) {
//        this.prefetch = prefetch;
//    }

    public long getDisconnectTimeout() {
        return disconnectTimeout;
    }

    public void setDisconnectTimeout(long disconnectTimeout) {
        this.disconnectTimeout = disconnectTimeout;
    }
}
