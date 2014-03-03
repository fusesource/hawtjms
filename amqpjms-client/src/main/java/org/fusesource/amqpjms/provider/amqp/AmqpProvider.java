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
package org.fusesource.amqpjms.provider.amqp;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.EngineFactory;
import org.apache.qpid.proton.engine.Sasl;
import org.apache.qpid.proton.engine.Transport;
import org.apache.qpid.proton.engine.impl.EngineFactoryImpl;
import org.apache.qpid.proton.engine.impl.ProtocolTracer;
import org.apache.qpid.proton.engine.impl.TransportImpl;
import org.apache.qpid.proton.framing.TransportFrame;
import org.fusesource.amqpjms.jms.message.JmsInboundMessageDispatch;
import org.fusesource.amqpjms.jms.message.JmsOutboundMessageDispatch;
import org.fusesource.amqpjms.jms.meta.JmsConnectionInfo;
import org.fusesource.amqpjms.jms.meta.JmsConsumerInfo;
import org.fusesource.amqpjms.jms.meta.JmsProducerId;
import org.fusesource.amqpjms.jms.meta.JmsProducerInfo;
import org.fusesource.amqpjms.jms.meta.JmsResource;
import org.fusesource.amqpjms.jms.meta.JmsResourceVistor;
import org.fusesource.amqpjms.jms.meta.JmsSessionInfo;
import org.fusesource.amqpjms.provider.AsyncProvider;
import org.fusesource.amqpjms.provider.ProviderConstants.ACK_TYPE;
import org.fusesource.amqpjms.provider.ProviderListener;
import org.fusesource.amqpjms.provider.ProviderRequest;
import org.fusesource.amqpjms.util.IOExceptionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.buffer.Buffer;

/**
 * An AMQP v1.0 Provider.
 *
 * The AMQP Provider is bonded to a single remote broker instance.  The provider will attempt
 * to connect to only that instance and once failed can not be recovered.  For clients that
 * wish to implement failover type connections a new AMQP Provider instance must be created
 * and state replayed from the JMS layer using the standard recovery process defined in the
 * JMS Provider API.
 *
 * All work within this Provider is serialized to a single Thread.  Any asynchronous exceptions
 * will be dispatched from that Thread and all in-bound requests are handled there as well.
 */
public class AmqpProvider implements AsyncProvider {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpConnection.class);

    private static final Logger TRACE_BYTES = LoggerFactory.getLogger(AmqpConnection.class.getPackage().getName() + ".BYTES");
    private static final Logger TRACE_FRAMES = LoggerFactory.getLogger(AmqpConnection.class.getPackage().getName() + ".FRAMES");

    private final URI remoteURI;
    private final Map<String, String> extraOptions;
    private AmqpConnection connection;
    private AmqpTransport transport;
    private ProviderListener listener;
    private boolean traceFrames;
    private boolean traceBytes;

    private final EngineFactory engineFactory = new EngineFactoryImpl();
    private final Transport protonTransport = engineFactory.createTransport();
    private final ExecutorService serializer;
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Create a new instance of an AmqpProvider bonded to the given remote URI.
     *
     * @param remoteURI
     *        The URI of the AMQP broker this Provider instance will connect to.
     */
    public AmqpProvider(URI remoteURI) {
        this(remoteURI, null);
    }

    /**
     * Create a new instance of an AmqpProvider bonded to the given remote URI.
     *
     * @param remoteURI
     *        The URI of the AMQP broker this Provider instance will connect to.
     */
    public AmqpProvider(URI remoteURI, Map<String, String> extraOptions) {
        this.remoteURI = remoteURI;
        if (extraOptions != null) {
            this.extraOptions = extraOptions;
        } else {
            this.extraOptions = Collections.emptyMap();
        }

        updateTracer();

        this.serializer = Executors.newSingleThreadExecutor(new ThreadFactory() {

            @Override
            public Thread newThread(Runnable runner) {
                Thread serial = new Thread(runner);
                serial.setDaemon(true);
                serial.setName("AmqpProvider: " + AmqpProvider.this.remoteURI.getHost());
                return serial;
            }
        });
    }

    @Override
    public void connect() throws IOException {
        checkClosed();

        transport = createTransport(remoteURI);
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
                        if (connection != null) {
                            connection.close(request);
                        }

                        pumpToProtonTransport();
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
    public URI getRemoteURI() {
        return remoteURI;
    }

    @Override
    public void create(final JmsResource resource, final ProviderRequest<JmsResource> request) throws IOException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    resource.visit(new JmsResourceVistor() {

                        @Override
                        public void processSessionInfo(JmsSessionInfo sessionInfo) throws Exception {
                            AmqpSession session = connection.createSession(sessionInfo);
                            session.open(request);
                        }

                        @Override
                        public void processProducerInfo(JmsProducerInfo producerInfo) throws Exception {
                            AmqpSession session = connection.getSession(producerInfo.getParentId());
                            AmqpProducer producer = session.createProducer(producerInfo);
                            producer.open(request);
                        }

                        @Override
                        public void processConsumerInfo(JmsConsumerInfo consumerInfo) throws Exception {
                            AmqpSession session = connection.getSession(consumerInfo.getParentId());
                            AmqpConsumer consumer = session.createConsumer(consumerInfo);
                            consumer.open(request);
                        }

                        @Override
                        public void processConnectionInfo(JmsConnectionInfo connectionInfo) throws Exception {
                            Connection protonConnection = engineFactory.createConnection();
                            protonTransport.bind(protonConnection);
                            Sasl sasl = protonTransport.sasl();
                            if (sasl != null) {
                                sasl.client();
                            }
                            connection = new AmqpConnection(AmqpProvider.this, protonConnection, sasl, connectionInfo);
                            connection.open(request);
                        }
                    });

                    pumpToProtonTransport();
                } catch (Exception error) {
                    request.onFailure(error);
                }
            }
        });
    }

    @Override
    public void destroy(final JmsResource resource, final ProviderRequest<Void> request) throws IOException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    resource.visit(new JmsResourceVistor() {

                        @Override
                        public void processSessionInfo(JmsSessionInfo sessionInfo) throws Exception {
                            AmqpSession session = connection.getSession(sessionInfo.getSessionId());
                            session.close(request);
                        }

                        @Override
                        public void processProducerInfo(JmsProducerInfo producerInfo) throws Exception {
                            AmqpSession session = connection.getSession(producerInfo.getParentId());
                            AmqpProducer producer = session.getProducer(producerInfo);
                            producer.close(request);
                        }

                        @Override
                        public void processConsumerInfo(JmsConsumerInfo consumerInfo) throws Exception {
                            AmqpSession session = connection.getSession(consumerInfo.getParentId());
                            AmqpConsumer consumer = session.getConsumer(consumerInfo);
                            consumer.close(request);
                        }

                        @Override
                        public void processConnectionInfo(JmsConnectionInfo connectionInfo) throws Exception {
                            connection.close(request);
                        }
                    });

                    pumpToProtonTransport();
                } catch (Exception error) {
                    request.onFailure(error);
                }
            }
        });
    }

    @Override
    public void send(final JmsOutboundMessageDispatch envelope, final ProviderRequest<Void> request) throws IOException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();

                    JmsProducerId producerId = envelope.getProducerId();
                    AmqpProducer producer = null;

                    if (producerId.getProviderHint() instanceof AmqpProducer) {
                        producer = (AmqpProducer) producerId.getProviderHint();
                    } else {
                        AmqpSession session = connection.getSession(producerId.getParentId());
                        producer = session.getProducer(producerId);
                    }

                    producer.send(envelope, request);
                    pumpToProtonTransport();
                } catch (Exception error) {
                    request.onFailure(error);
                }
            }
        });
    }

    @Override
    public void acknowledge(JmsInboundMessageDispatch envelope, ACK_TYPE ackType, final ProviderRequest<Void> request) throws IOException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();

                    // TODO - Ack

                    pumpToProtonTransport();
                    request.onSuccess(null);
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
    protected AmqpTransport createTransport(URI remoteLocation) {
        return new AmqpTcpTransport(this, remoteLocation);
    }

    protected void checkClosed() throws IOException {
        if (closed.get()) {
            throw new IOException("The Provider is already closed");
        }
    }

    private void updateTracer() {
        if (isTraceFrames()) {
            ((TransportImpl) protonTransport).setProtocolTracer(new ProtocolTracer() {
                @Override
                public void receivedFrame(TransportFrame transportFrame) {
                    TRACE_FRAMES.trace("RECV: {}", transportFrame.getBody());
                }

                @Override
                public void sentFrame(TransportFrame transportFrame) {
                    TRACE_FRAMES.trace("SENT: {}", transportFrame.getBody());
                }
            });
        }
    }

    void onAmqpData(Buffer input) {

        // Create our own copy since we will process later.
        final ByteBuffer source = ByteBuffer.wrap(input.getBytes());

        serializer.execute(new Runnable() {

            @Override
            public void run() {
                LOG.trace("Received from Broker {} bytes:", source.remaining());

                do {
                    ByteBuffer buffer = protonTransport.getInputBuffer();
                    int limit = Math.min(buffer.remaining(), source.remaining());
                    ByteBuffer duplicate = source.duplicate();
                    duplicate.limit(source.position() + limit);
                    buffer.put(duplicate);
                    protonTransport.processInput();
                    source.position(source.position() + limit);
                } while (source.hasRemaining());

                // Process the state changes from the latest data and then answer back
                // any pending updates to the Broker.
                processUpdates();
                pumpToProtonTransport();
            }
        });
    }

    /**
     * Callback method for the AmqpTransport to report connection errors.  When called
     * the method will queue a new task to fire the failure error back to the listener.
     *
     * @param error
     *        the error that causes the transport to fail.
     */
    void onTransportError(final Throwable error) {
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
     * Callback method for the AmqpTransport to report that the underlying connection
     * has closed.  When called this method will queue a new task that will check for
     * the closed state on this transport and if not closed then an exception is raied
     * to the registered ProviderListener to indicate connection loss.
     */
    void onTransportClosed() {
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

    void fireProviderException(Throwable ex) {
        ProviderListener listener = this.listener;
        if (listener != null) {
            listener.onConnectionFailure(IOExceptionSupport.create(ex));
        }
    }

    private void processUpdates() {
        connection.processUpdates();
        // TODO - Handle exceptions and fire back to the client when they happen.
    }

    private void pumpToProtonTransport() {
        try {
            boolean done = false;
            while (!done) {
                ByteBuffer toWrite = protonTransport.getOutputBuffer();
                if (toWrite != null && toWrite.hasRemaining()) {
                    // TODO - Get Bytes in a readable form
                    if (isTraceBytes()) {
                        TRACE_BYTES.info("Sending: {}", toWrite.toString());
                    }
                    transport.send(toWrite);
                    protonTransport.outputConsumed();
                } else {
                    done = true;
                }
            }
        } catch (IOException e) {
            fireProviderException(e);
        }
    }

    @Override
    public void setProviderListener(ProviderListener listener) {
        this.listener = listener;
    }

    @Override
    public ProviderListener getProviderListener() {
        return this.listener;
    }

    public void setTraceFrames(boolean trace) {
        this.traceFrames = trace;
    }

    public boolean isTraceFrames() {
        return this.traceFrames;
    }

    public void setTraceBytes(boolean trace) {
        this.traceBytes = trace;
    }

    public boolean isTraceBytes() {
        return this.traceBytes;
    }
}
