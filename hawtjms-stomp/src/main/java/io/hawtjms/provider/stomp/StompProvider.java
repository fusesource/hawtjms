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
package io.hawtjms.provider.stomp;

import static io.hawtjms.provider.stomp.StompConstants.DISCONNECT;
import io.hawtjms.jms.JmsDestination;
import io.hawtjms.jms.message.JmsInboundMessageDispatch;
import io.hawtjms.jms.message.JmsMessageFactory;
import io.hawtjms.jms.message.JmsOutboundMessageDispatch;
import io.hawtjms.jms.meta.JmsConnectionInfo;
import io.hawtjms.jms.meta.JmsConsumerId;
import io.hawtjms.jms.meta.JmsConsumerInfo;
import io.hawtjms.jms.meta.JmsDefaultResourceVisitor;
import io.hawtjms.jms.meta.JmsProducerId;
import io.hawtjms.jms.meta.JmsProducerInfo;
import io.hawtjms.jms.meta.JmsResource;
import io.hawtjms.jms.meta.JmsSessionId;
import io.hawtjms.jms.meta.JmsSessionInfo;
import io.hawtjms.jms.meta.JmsTransactionInfo;
import io.hawtjms.provider.AbstractAsyncProvider;
import io.hawtjms.provider.AsyncResult;
import io.hawtjms.provider.ProviderConstants.ACK_TYPE;
import io.hawtjms.provider.ProviderRequest;
import io.hawtjms.transports.TcpTransport;
import io.hawtjms.transports.Transport;
import io.hawtjms.transports.TransportListener;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.buffer.Buffer;

/**
 * Async Provider implementation for the STOMP protocol.
 */
public class StompProvider extends AbstractAsyncProvider implements TransportListener {

    private static final Logger LOG = LoggerFactory.getLogger(StompProvider.class);

    private final StompCodec codec = new StompCodec();

    private Transport transport;
    private StompConnection connection;
    private long closeTimeout = JmsConnectionInfo.DEFAULT_CLOSE_TIMEOUT;

    public StompProvider(URI remoteURI) {
        super(remoteURI);
    }

    @Override
    public void connect() throws IOException {
        checkClosed();

        transport = createTransport(getRemoteURI());
        transport.connect();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            final ProviderRequest<Void> request = new ProviderRequest<Void>();
            serializer.execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        // TODO - We should wait, but for now lets just do it async.
                        StompFrame disconnect = new StompFrame(DISCONNECT);
                        transport.send(codec.encode(disconnect));
                    } catch (Exception e) {
                        LOG.debug("Caught exception while closing proton connection");
                    } finally {
                        if (transport != null) {
                            try {
                                transport.close();
                            } catch (Exception e) {
                                LOG.debug("Cuaght exception while closing down Transport: {}", e.getMessage());
                            }
                        }

                        request.onSuccess();
                    }
                }
            });

            try {
                if (closeTimeout < 0) {
                    request.getResponse();
                } else {
                    request.getResponse(closeTimeout, TimeUnit.MILLISECONDS);
                }
            } catch (IOException e) {
                LOG.warn("Error caught while closing Provider: ", e.getMessage());
            } finally {
                if (serializer != null) {
                    serializer.shutdown();
                }
            }
        }
    }

    @Override
    public void create(final JmsResource resource, final AsyncResult<Void> request) throws IOException, JMSException, UnsupportedOperationException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    resource.visit(new JmsDefaultResourceVisitor() {

                        @Override
                        public void processConsumerInfo(JmsConsumerInfo consumerInfo) throws Exception {
                            StompSession session = connection.getSession(consumerInfo.getParentId());
                            session.createConsumer(consumerInfo, request);
                        }

                        @Override
                        public void processProducerInfo(JmsProducerInfo producerInfo) throws Exception {
                            StompSession session = connection.getSession(producerInfo.getParentId());
                            session.createProducer(producerInfo, request);
                        }

                        @Override
                        public void processSessionInfo(JmsSessionInfo sessionInfo) throws Exception {
                            connection.createSession(sessionInfo, request);
                        }

                        @Override
                        public void processConnectionInfo(JmsConnectionInfo connectionInfo) throws Exception {
                            closeTimeout = connectionInfo.getCloseTimeout();

                            connection = new StompConnection(StompProvider.this, connectionInfo);
                            connection.connect(request);
                        }

                        @Override
                        public void processDestination(JmsDestination destination) throws Exception {
                            // The generated names from the JMS framework are valid so we
                            // just use those and apply the correct prefix on send etc.
                            request.onSuccess();
                        }

                        @Override
                        public void processTransactionInfo(JmsTransactionInfo transactionInfo) throws Exception {
                            request.onFailure(new JMSException("Not implemented"));
                        }
                    });

                } catch (Exception error) {
                    request.onFailure(error);
                }
            }
        });
    }

    @Override
    public void start(final JmsResource resource, final AsyncResult<Void> request) throws IOException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    resource.visit(new JmsDefaultResourceVisitor() {

                        @Override
                        public void processConsumerInfo(JmsConsumerInfo consumerInfo) throws Exception {
                            StompSession session = connection.getSession(consumerInfo.getParentId());
                            StompConsumer consumer = session.getConsumer(consumerInfo.getConsumerId());
                            consumer.start();
                            request.onSuccess();
                        }
                    });
                } catch (Exception error) {
                    request.onFailure(error);
                }
            }
        });
    }

    @Override
    public void destroy(final JmsResource resource, final AsyncResult<Void> request) throws IOException, JMSException, UnsupportedOperationException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    resource.visit(new JmsDefaultResourceVisitor() {

                        @Override
                        public void processConsumerInfo(JmsConsumerInfo consumerInfo) throws Exception {
                            StompSession session = connection.getSession(consumerInfo.getParentId());
                            StompConsumer consumer = session.getConsumer(consumerInfo.getConsumerId());
                            consumer.close(request);
                        }

                        @Override
                        public void processProducerInfo(JmsProducerInfo producerInfo) throws Exception {
                            StompSession session = connection.getSession(producerInfo.getParentId());
                            StompProducer producer = session.getProducer(producerInfo.getProducerId());
                            producer.close(request);
                        }

                        @Override
                        public void processSessionInfo(JmsSessionInfo sessionInfo) throws Exception {
                            StompSession session = connection.getSession(sessionInfo.getSessionId());
                            session.close(request);
                        }

                        @Override
                        public void processConnectionInfo(JmsConnectionInfo connectionInfo) throws Exception {
                            // TODO - Instruct Connection to close.
                            // TODO - If we send the disconnect frame here we need to wait for a timeout
                            //        period an cancel since broker might close socket early.
                            //        probably better not to do that here.
                            request.onSuccess();
                        }

                        @Override
                        public void processDestination(JmsDestination destination) throws Exception {
                            request.onFailure(new JMSException("STOMP does not support destination remove."));
                        }

                        @Override
                        public void processTransactionInfo(JmsTransactionInfo transactionInfo) throws Exception {
                            request.onFailure(new JMSException("Not implemented"));
                        }
                    });

                } catch (Exception error) {
                    request.onFailure(error);
                }
            }
        });
    }

    @Override
    public void send(final JmsOutboundMessageDispatch envelope, final AsyncResult<Void> request) throws IOException, JMSException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    JmsProducerId producerId = envelope.getProducerId();
                    StompProducer producer = connection.getProducer(producerId);
                    producer.send(envelope, request);
                } catch (Exception error) {
                    request.onFailure(error);
                }
            }
        });
    }

    @Override
    public void acknowledge(final JmsSessionId sessionId, final AsyncResult<Void> request) throws IOException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    StompSession amqpSession = connection.getSession(sessionId);
                    amqpSession.acknowledge(request);
                } catch (Exception error) {
                    request.onFailure(error);
                }
            }
        });
    }

    @Override
    public void acknowledge(final JmsInboundMessageDispatch envelope, final ACK_TYPE ackType, final AsyncResult<Void> request) throws IOException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    JmsConsumerId consumerId = envelope.getConsumerId();
                    StompConsumer consumer = connection.getConsumer(consumerId);
                    consumer.acknowledge(envelope, ackType, request);
                } catch (Exception error) {
                    request.onFailure(error);
                }
            }
        });
    }

    @Override
    public void commit(final JmsSessionId sessionId, final AsyncResult<Void> request) throws IOException, JMSException, UnsupportedOperationException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    StompSession session = connection.getSession(sessionId);
                    session.commit(request);
                } catch (Exception error) {
                    request.onFailure(error);
                }
            }
        });
    }

    @Override
    public void rollback(final JmsSessionId sessionId, final AsyncResult<Void> request) throws IOException, JMSException, UnsupportedOperationException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    StompSession session = connection.getSession(sessionId);
                    session.rollback(request);
                } catch (Exception error) {
                    request.onFailure(error);
                }
            }
        });
    }

    @Override
    public void recover(final JmsSessionId sessionId, final AsyncResult<Void> request) throws IOException, UnsupportedOperationException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    StompSession session = connection.getSession(sessionId);
                    session.recover();
                    request.onSuccess();
                } catch (Exception error) {
                    request.onFailure(error);
                }
            }
        });
    }

    @Override
    public void pull(final JmsConsumerId consumerId, long timeout, final AsyncResult<Void> request) throws IOException, UnsupportedOperationException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    StompSession session = connection.getSession(consumerId.getParentId());
                    StompConsumer consumer = session.getConsumer(consumerId);

                    /*
                     * Client will trying to perform a pull in order to allow a browser to
                     * check it's state and retrieve any pending messages.  We don't error
                     * in this case as we know in STOMP if we are done or not by an end of
                     * browse but other protocols might need the kick.
                     */
                    if (consumer.isBrowser()) {
                        request.onSuccess();
                    } else {
                        request.onFailure(new UnsupportedOperationException("STOMP consumer cannot pull messages."));
                    }
                } catch (Exception error) {
                    request.onFailure(error);
                }
            }
        });
    }

    @Override
    public void unsubscribe(final String subscription, final AsyncResult<Void> request) throws IOException, JMSException, UnsupportedOperationException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    // TODO - Can we do this with STOMP with just this info?
                    request.onSuccess();
                } catch (Exception error) {
                    request.onFailure(error);
                }
            }
        });
    }

    /**
     * Provides an extension point for subclasses to insert other types of transports such
     * as SSL etc.
     *
     * @param remoteLocation
     *        The remote location where the transport should attempt to connect.
     *
     * @return the newly created transport instance.
     */
    protected Transport createTransport(URI remoteLocation) {
        return new TcpTransport(this, remoteLocation);
    }

    /**
     * Encodes and sends the given STOMP frame using the provider's Transport.
     * This method must be called from an job running on the serializer thread.
     *
     * @param frame
     *        the STOMP frame instance to send.
     *
     * @throws IOException if an error occurs while encoding or sending the frame.
     */
    protected void send(StompFrame frame) throws IOException {
        ByteBuffer connect = codec.encode(frame);
        transport.send(connect);
    }

    @Override
    public void onData(Buffer incoming) {
        // Create our own copy since we will process later.
        final ByteBuffer source = ByteBuffer.wrap(incoming.getBytes());

        serializer.execute(new Runnable() {

            @Override
            public void run() {
                LOG.trace("Received from Broker {} bytes:", source.remaining());

                try {
                    do {
                        StompFrame frame = codec.decode(source);
                        if (frame != null) {
                            connection.processFrame(frame);
                        }
                    } while (source.hasRemaining());
                } catch (Exception e) {
                    LOG.warn("Caught exception while processing new data: {}", e.getMessage());
                    LOG.trace("Exception detail: ", e);
                    fireProviderException(e);
                }
            }
        });
    }

    /**
     * Callback method for the Transport to report connection errors.  When called
     * the method will queue a new task to fire the failure error back to the listener.
     *
     * @param error
     *        the error that causes the transport to fail.
     */
    @Override
    public void onTransportError(final Throwable error) {
        if (!closed.get()) {
            serializer.execute(new Runnable() {
                @Override
                public void run() {
                    LOG.info("Transport failed: {}", error.getMessage());
                    if (!closed.get()) {
                        fireProviderException(error);
                    }
                }
            });
        }
    }

    /**
     * Callback method for the Transport to report that the underlying connection
     * has closed.  When called this method will queue a new task that will check for
     * the closed state on this transport and if not closed then an exception is raied
     * to the registered ProviderListener to indicate connection loss.
     */
    @Override
    public void onTransportClosed() {
        if (!closed.get()) {
            serializer.execute(new Runnable() {
                @Override
                public void run() {
                    LOG.info("Transport connection remotely closed:");
                    if (!closed.get()) {
                        fireProviderException(new IOException("Connection remotely closed."));
                    }
                }
            });
        }
    }

    //------------- Property Getters / Setters -------------------------------//

    @Override
    public JmsMessageFactory getMessageFactory() {
        if (connection == null) {
            throw new RuntimeException("Message Factory is not accessible when not connected.");
        }
        return connection.getMessageFactory();
    }

    public long getCloseTimeout() {
        return this.closeTimeout;
    }

    public void setCloseTimeout(long closeTimeout) {
        this.closeTimeout = closeTimeout;
    }
}
