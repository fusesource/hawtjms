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
package org.fusesource.amqpjms.provider.failover;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.fusesource.amqpjms.jms.message.JmsInboundMessageDispatch;
import org.fusesource.amqpjms.jms.message.JmsOutboundMessageDispatch;
import org.fusesource.amqpjms.jms.meta.JmsResource;
import org.fusesource.amqpjms.jms.util.IOExceptionSupport;
import org.fusesource.amqpjms.provider.AsyncProvider;
import org.fusesource.amqpjms.provider.BlockingProvider;
import org.fusesource.amqpjms.provider.DefaultBlockingProvider;
import org.fusesource.amqpjms.provider.DefaultProviderListener;
import org.fusesource.amqpjms.provider.ProviderConstants.ACK_TYPE;
import org.fusesource.amqpjms.provider.ProviderFactory;
import org.fusesource.amqpjms.provider.ProviderListener;
import org.fusesource.amqpjms.provider.ProviderRequest;
import org.fusesource.amqpjms.provider.amqp.AmqpConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Provider Facade that provides services for detection dropped Provider connections
 * and attempting to reconnect to a different remote peer.  Upon establishment of a new
 * connection the FailoverProvider will initiate state recovery of the active JMS
 * framework resources.
 */
public class FailoverProvider extends DefaultProviderListener implements BlockingProvider {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpConnection.class);

    private static final int INFINITE = -1;

    private ProviderListener listener;
    private AsyncProvider provider;
    private final URI originalURI;
    private final Map<String, String> extraOptions;

    private final ExecutorService serializer;
    private final ScheduledExecutorService connectionHub;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean failed = new AtomicBoolean();
    private final LinkedList<FailoverRequest<?>> requests = new LinkedList<FailoverRequest<?>>();
    private final DefaultProviderListener closedListener = new DefaultProviderListener();

    // Current state of connection / reconnection
    private boolean firstConnection = true;
    private long reconnectAttempts;
    private long reconnectDelay = TimeUnit.SECONDS.toMillis(5);
    private IOException failureCause;

    // Configuration values.
    private long initialReconnectDelay = 0L;
    private long maxReconnectDelay = TimeUnit.SECONDS.toMillis(30);
    private boolean useExponentialBackOff = true;
    private double backOffMultiplier = 2d;
    private int maxReconnectAttempts = INFINITE;
    private int startupMaxReconnectAttempts = INFINITE;
    private int warnAfterReconnectAttempts = 10;

    public FailoverProvider(URI uri) {
        this(uri, null);
    }

    public FailoverProvider(URI uri, Map<String, String> extraOptions) {
        this.originalURI = uri;
        if (extraOptions != null) {
            this.extraOptions = extraOptions;
        } else {
            this.extraOptions = Collections.emptyMap();
        }

        this.serializer = Executors.newSingleThreadExecutor(new ThreadFactory() {

            @Override
            public Thread newThread(Runnable runner) {
                Thread serial = new Thread(runner);
                serial.setDaemon(true);
                serial.setName("FailoverProvider: serialization thread");
                return serial;
            }
        });

        // All Connection attempts happen in this schedulers thread.  Once a connection
        // is established it will hand the open connection back to the serializer thread
        // for state recovery.
        this.connectionHub = Executors.newScheduledThreadPool(1, new ThreadFactory() {

            @Override
            public Thread newThread(Runnable runner) {
                Thread serial = new Thread(runner);
                serial.setDaemon(true);
                serial.setName("FailoverProvider: connect thread");
                return serial;
            }
        });
    }

    @Override
    public void connect() throws IOException {
        checkClosed();
        LOG.debug("Performing initial connection attempt");
        triggerReconnectionAttempt();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            final ProviderRequest<Void> request = new ProviderRequest<Void>();
            serializer.execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        for (FailoverRequest<?> request : requests) {
                            request.onFailure(new IOException("Closed"));
                        }

                        if (provider != null) {
                            provider.close();
                        }
                    } catch (Exception e) {
                        LOG.debug("Caught exception while closing proton connection");
                    } finally {

                        if (connectionHub != null) {
                            connectionHub.shutdown();
                        }

                        if (serializer != null) {
                            serializer.shutdown();
                        }

                        request.onSuccess(null);
                    }
                }
            });

            // TODO - Add a close timeout.
            try {
                request.getResponse();
            } catch (IOException e) {
                LOG.warn("Error caught while closing Provider: ", e.getMessage());
            }
        }
    }

    @Override
    public JmsResource create(final JmsResource resource) throws IOException {
        checkClosed();
        final FailoverRequest<JmsResource> request = new FailoverRequest<JmsResource>() {
            @Override
            public void doTask() throws IOException {
                provider.create(resource, this);
            }
        };

        serializer.execute(request);
        return request.getResponse();
    }

    @Override
    public void destroy(final JmsResource resource) throws IOException {
        checkClosed();
        final FailoverRequest<Void> request = new FailoverRequest<Void>() {
            @Override
            public void doTask() throws IOException {
                provider.destroy(resource, this);
            }
        };

        serializer.execute(request);
        request.getResponse();
    }

    @Override
    public void send(final JmsOutboundMessageDispatch envelope) throws IOException {
        checkClosed();
        final FailoverRequest<Void> request = new FailoverRequest<Void>() {
            @Override
            public void doTask() throws IOException {
                provider.send(envelope, this);
            }
        };

        serializer.execute(request);
        request.getResponse();
    }

    @Override
    public void acknowledge(final JmsInboundMessageDispatch envelope, final ACK_TYPE ackType) throws IOException {
        checkClosed();
        final FailoverRequest<Void> request = new FailoverRequest<Void>() {
            @Override
            public void doTask() throws IOException {
                provider.acknowledge(envelope, ackType, this);
            }
        };

        serializer.execute(request);
        request.getResponse();
    }

    @Override
    public void receoveryComplate() throws IOException {
        checkClosed();
        // TODO Auto-generated method stub
    }

    @Override
    public void setProviderListener(ProviderListener listener) {
        this.listener = listener;
    }

    @Override
    public ProviderListener getProviderListener() {
        return listener;
    }

    @Override
    public URI getRemoteURI() {
        AsyncProvider provider = this.provider;
        if (provider != null) {
            return provider.getRemoteURI();
        }
        return null;
    }

    protected void checkClosed() throws IOException {
        if (closed.get()) {
            throw new IOException("The Provider is already closed");
        }
    }

    /**
     * This method is always called from within the FailoverProvider's serialization thread.
     *
     * When a failure is encountered either from an outgoing request or from an error fired
     * from the underlying Provider instance this method is called to determine if a reconnect
     * is allowed and if so a new reconnect cycle is triggered on the connection thread.
     *
     * @param cause
     */
    private void handleProviderFailure(final IOException cause) {
        LOG.debug("handling Provider failure: {}", cause.getMessage());

        this.provider.setProviderListener(closedListener);
        this.provider.close();
        this.provider = null;

        if (reconnectAllowed()) {
            triggerReconnectionAttempt();
        } else {
            ProviderListener listener = this.listener;
            if (listener != null) {
                listener.onConnectionFailure(cause);
            }
        }
    }

    /**
     * Called from the reconnection thread.  This method enqueues a new task that
     * will attempt to recover connection state, once successful normal operations
     * will resume.  If an error occurs while attempting to recover the JMS framework
     * state then a reconnect cycle is again triggered on the connection thread.
     *
     * @param provider
     *        The newly connect Provider instance that will become active.
     */
    private void initializeNewConnection(final AsyncProvider provider) {
        this.serializer.execute(new Runnable() {
            @Override
            public void run() {
                if (firstConnection) {
                    firstConnection = false;
                    FailoverProvider.this.provider = provider;

                    for (FailoverRequest<?> request : requests) {
                        request.run();
                    }
                } else {
                    try {
                        listener.onConnectionRecovery(new DefaultBlockingProvider(provider));
                        FailoverProvider.this.provider = provider;
                        listener.onConnectionRestored();

                        reconnectDelay = initialReconnectDelay;
                        reconnectAttempts = 0;
                    } catch (Throwable e) {
                        LOG.info("Failed while replaying state: {}", e.getMessage());
                        triggerReconnectionAttempt();
                    }
                }
            }
        });
    }

    /**
     * Called when the Provider was either first created or when a connection failure has
     * been connected.  A reconnection attempt is immediately executed on the connection
     * thread.  If a new Provider is able to be created and connected then a recovery task
     * is scheduled on the main serializer thread.  If the connect attempt fails another
     * attempt is scheduled based on the configured delay settings until a max attempts
     * limit is hit, if one is set.
     */
    private void triggerReconnectionAttempt() {
        if (closed.get() || failed.get()) {
            return;
        }

        connectionHub.execute(new Runnable() {
            @Override
            public void run() {
                if (provider != null || closed.get() || failed.get()) {
                    return;
                }

                Throwable failure = null;

                reconnectAttempts++;
                try {
                    AsyncProvider provider = ProviderFactory.createAsync(originalURI);
                    initializeNewConnection(provider);
                    return;
                } catch (Throwable e) {
                    LOG.info("Connection attempt to: {} failed.", originalURI);
                    failure = e;
                }

                int reconnectLimit = reconnectAttemptLimit();

                if (reconnectLimit != INFINITE && reconnectAttempts >= reconnectLimit) {
                    LOG.error("Failed to connect after: " + reconnectAttempts + " attempt(s)");
                    failed.set(true);
                    failureCause = IOExceptionSupport.create(failure);
                    if (listener != null) {
                        listener.onConnectionFailure(failureCause);
                    };

                    return;
                }

                int warnInterval = getWarnAfterReconnectAttempts();
                if (warnInterval > 0 && (reconnectAttempts % warnInterval) == 0) {
                    LOG.warn("Failed to connect after: {} attempt(s) continuing to retry.", reconnectAttempts);
                }

                long delay = nextReconnectDelay();
                connectionHub.schedule(this, delay, TimeUnit.MILLISECONDS);
            }
        });
    }

    private boolean reconnectAllowed() {
        return reconnectAttemptLimit() != 0;
    }

    private int reconnectAttemptLimit() {
        int maxReconnectValue = this.maxReconnectAttempts;
        if (firstConnection && this.startupMaxReconnectAttempts != INFINITE) {
            maxReconnectValue = this.startupMaxReconnectAttempts;
        }
        return maxReconnectValue;
    }

    private long nextReconnectDelay() {
        if (useExponentialBackOff) {
            // Exponential increment of reconnect delay.
            reconnectDelay *= backOffMultiplier;
            if (reconnectDelay > maxReconnectDelay) {
                reconnectDelay = maxReconnectDelay;
            }
        }

        return reconnectDelay;
    }

    //--------------- DefaultProviderListener overrides ----------------------//

    @Override
    public void onMessage(final JmsInboundMessageDispatch envelope) {
        if (closed.get()) {
            return;
        }
        serializer.execute(new Runnable() {
            @Override
            public void run() {
                if (!closed.get()) {
                    listener.onMessage(envelope);
                }
            }
        });
    }

    @Override
    public void onConnectionFailure(final IOException ex) {
        if (closed.get()) {
            return;
        }
        serializer.execute(new Runnable() {
            @Override
            public void run() {
                if (!closed.get() && !failed.get()) {
                    handleProviderFailure(ex);
                }
            }
        });
    }

    //--------------- Property Getters and Setters ---------------------------//

    long getInitialReconnectDealy() {
        return initialReconnectDelay;
    }

    void setInitialReconnectDealy(long initialReconnectDealy) {
        this.initialReconnectDelay = initialReconnectDealy;
    }

    long getMaxReconnectDelay() {
        return maxReconnectDelay;
    }

    void setMaxReconnectDelay(long maxReconnectDelay) {
        this.maxReconnectDelay = maxReconnectDelay;
    }

    int getMaxReconnectAttempts() {
        return maxReconnectAttempts;
    }

    void setMaxReconnectAttempts(int maxReconnectAttempts) {
        this.maxReconnectAttempts = maxReconnectAttempts;
    }

    int getStartupMaxReconnectAttempts() {
        return startupMaxReconnectAttempts;
    }

    void setStartupMaxReconnectAttempts(int startupMaxReconnectAttempts) {
        this.startupMaxReconnectAttempts = startupMaxReconnectAttempts;
    }

    /**
     * Gets the current setting controlling how many Connect / Reconnect attempts must occur
     * before a warn message is logged.  A value of {@code <= 0} indicates that there will be
     * no warn message logged regardless of how many reconnect attempts occur.
     *
     * @return the current number of connection attempts before warn logging is triggered.
     */
    public int getWarnAfterReconnectAttempts() {
        return warnAfterReconnectAttempts;
    }

    /**
     * Sets the number of Connect / Reconnect attempts that must occur before a warn message
     * is logged indicating that the transport is not connected.  This can be useful when the
     * client is running inside some container or service as it gives an indication of some
     * problem with the client connection that might not otherwise be visible.  To disable the
     * log messages this value should be set to a value @{code attempts <= 0}
     *
     * @param warnAfterReconnectAttempts
     *        The number of failed connection attempts that must happen before a warning is logged.
     */
    public void setWarnAfterReconnectAttempts(int warnAfterReconnectAttempts) {
        this.warnAfterReconnectAttempts = warnAfterReconnectAttempts;
    }

    public double getReconnectDelayExponent() {
        return backOffMultiplier;
    }

    public void setReconnectDelayExponent(double reconnectDelayExponent) {
        this.backOffMultiplier = reconnectDelayExponent;
    }

    public boolean isUseExponentialBackOff() {
        return useExponentialBackOff;
    }

    public void setUseExponentialBackOff(boolean useExponentialBackOff) {
        this.useExponentialBackOff = useExponentialBackOff;
    }

    //--------------- FailoverProvider Asynchronous Request --------------------//

    /**
     * For all requests that are dispatched from the FailoverProvider to a connected
     * Provider instance an instance of FailoverRequest is used to handle errors that
     * occur during processing of that request and trigger a reconnect.
     *
     * @param <T>
     */
    protected abstract class FailoverRequest<T> extends ProviderRequest<T> implements Runnable {

        @Override
        public void run() {
            if (provider == null) {
                if (failureWhenOffline()) {
                    onFailure(new IOException("Provider disconnected"));
                } else if (succeedsWhenOffline()) {
                    onSuccess(null);
                } else {
                    requests.addLast(this);
                }
            } else {
                try {
                    doTask();
                } catch (IOException e) {
                    LOG.debug("Caught exception while executing task: {}", e.getMessage());
                    triggerReconnectionAttempt();
                }
            }
        }

        /**
         * Called to execute the specific task that was requested.
         *
         * @throws IOException if an error occurs during task execution.
         */
        public abstract void doTask() throws IOException;

        /**
         * Should the request just succeed when the Provider is not connected.
         *
         * @return true if the request is marked as successful when not connected.
         */
        public boolean succeedsWhenOffline() {
            return false;
        }

        /**
         * When the transport is not connected should this request automatically fail.
         *
         * @return true if the task should fail when the Provider is not connected.
         */
        public boolean failureWhenOffline() {
            return false;
        }
    }
}
