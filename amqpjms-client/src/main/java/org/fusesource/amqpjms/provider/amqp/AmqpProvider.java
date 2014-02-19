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
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.fusesource.amqpjms.jms.meta.JmsConnectionInfo;
import org.fusesource.amqpjms.jms.meta.JmsConsumerInfo;
import org.fusesource.amqpjms.jms.meta.JmsProducerInfo;
import org.fusesource.amqpjms.jms.meta.JmsResource;
import org.fusesource.amqpjms.jms.meta.JmsResourceVistor;
import org.fusesource.amqpjms.jms.meta.JmsSessionInfo;
import org.fusesource.amqpjms.jms.util.IOExceptionSupport;
import org.fusesource.amqpjms.provider.Provider;
import org.fusesource.amqpjms.provider.ProviderListener;
import org.fusesource.amqpjms.provider.ProviderResponse;

/**
 * An AMQP v1.0 Provider.
 *
 * The AMQP Provider is bonded to a single remote broker instance.  The provider will attempt
 * to connect to only that instance and once failed can not be recovered.  For clients that
 * wish to implement failover type connections a new AMQP Provider instance must be created
 * and state replayed from the JMS layer using the standard recovery process defined in the
 * JMS Provider API.
 */
public class AmqpProvider implements Provider {

    private final URI remoteURI;
    private final Map<String, String> extraOptions;
    private AmqpConnection connection;
    private AmqpTransport transport;
    private ProviderListener listener;

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
    }

    @Override
    public void connect() throws IOException {
        checkClosed();

        connection = new AmqpConnection();
        transport = createTransport(connection, remoteURI);
        transport.connect();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            // TODO close connection and any open AMQP resources.

            // TODO close down the transport connection.
        }
    }

    @Override
    public void receoveryComplate() throws IOException {
    }

    @Override
    public URI getRemoteURI() {
        return remoteURI;
    }

    @Override
    public ProviderResponse<JmsResource> create(JmsResource resource) throws IOException {
        final ProviderResponse<JmsResource> response = new ProviderResponse<JmsResource>();

        try {
            resource.visit(new JmsResourceVistor() {

                @Override
                public void processSessionInfo(JmsSessionInfo sessionInfo) throws Exception {
                    // TODO Auto-generated method stub

                }

                @Override
                public void processProducerInfo(JmsProducerInfo producerInfo) throws Exception {
                    // TODO Auto-generated method stub

                }

                @Override
                public void processConsumerInfo(JmsConsumerInfo consumerInfo) throws Exception {
                    // TODO Auto-generated method stub

                }

                @Override
                public void processConnectionInfo(JmsConnectionInfo connectionInfo) throws Exception {
                    connection.createConnection(connectionInfo, response);
                }
            });
        } catch (Exception error) {
            throw IOExceptionSupport.create(error);
        }

        return response;
    }

    @Override
    public ProviderResponse<Void> destroy(JmsResource resource) throws IOException {
        final ProviderResponse<Void> response = new ProviderResponse<Void>();

        try {
            resource.visit(new JmsResourceVistor() {

                @Override
                public void processSessionInfo(JmsSessionInfo sessionInfo) throws Exception {
                    // TODO Auto-generated method stub

                }

                @Override
                public void processProducerInfo(JmsProducerInfo producerInfo) throws Exception {
                    // TODO Auto-generated method stub

                }

                @Override
                public void processConsumerInfo(JmsConsumerInfo consumerInfo) throws Exception {
                    // TODO Auto-generated method stub

                }

                @Override
                public void processConnectionInfo(JmsConnectionInfo connectionInfo) throws Exception {
                    connection.destroyConnection(connectionInfo, response);
                }
            });
        } catch (Exception error) {
            throw IOExceptionSupport.create(error);
        }

        return response;
    }

    /**
     * Provides an extension point for subclasses to insert other types of transports such
     * as SSL etc.
     *
     * @param connection
     *        The connection that owns this newly created Transport.
     * @param remoteLocation
     *        The remote location where the transport should attempt to connect.
     *
     * @return the newly created transport instance.
     */
    protected AmqpTransport createTransport(AmqpConnection connection, URI remoteLocation) {
        return new AmqpTcpTransport(connection, remoteLocation);
    }

    protected void checkClosed() throws IOException {
        if (closed.get()) {
            throw new IOException("The Provider is already closed");
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
}
