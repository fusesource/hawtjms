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
package org.fusesource.amqpjms.provider;

import java.io.IOException;
import java.net.URI;

import org.fusesource.amqpjms.jms.message.JmsInboundMessageDispatch;
import org.fusesource.amqpjms.jms.message.JmsOutboundMessageDispatch;
import org.fusesource.amqpjms.jms.meta.JmsResource;
import org.fusesource.amqpjms.provider.ProviderConstants.ACK_TYPE;

/**
 * Provides a simple Provider Facade that allows a ProtocolProvider to be used
 * without any additional capabilities wrapped around it such as Failover or
 * Discovery.  All methods are executed as blocking operations.
 */
public class DefaultProvider implements Provider {

    private final ProtocolProvider protocol;

    public DefaultProvider(ProtocolProvider protocol) {
        this.protocol = protocol;
    }

    @Override
    public void connect() throws IOException {
        protocol.connect();
    }

    @Override
    public void close() {
        protocol.close();
    }

    @Override
    public void receoveryComplate() throws IOException {
    }

    @Override
    public URI getRemoteURI() {
        return protocol.getRemoteURI();
    }

    @Override
    public JmsResource create(JmsResource resource) throws IOException {
        ProviderRequest<JmsResource> request = new ProviderRequest<JmsResource>();
        protocol.create(resource, request);
        return request.getResponse();
    }

    @Override
    public void destroy(JmsResource resource) throws IOException {
        ProviderRequest<Void> request = new ProviderRequest<Void>();
        protocol.destroy(resource, request);
    }

    @Override
    public void send(JmsOutboundMessageDispatch envelope) throws IOException {
        ProviderRequest<Void> request = new ProviderRequest<Void>();
        protocol.send(envelope, request);
    }

    @Override
    public void acknowledge(JmsInboundMessageDispatch envelope, ACK_TYPE ackType) throws IOException {
        ProviderRequest<Void> request = new ProviderRequest<Void>();
        protocol.acknowledge(envelope, ackType, request);
    }

    @Override
    public void setProviderListener(ProviderListener listener) {
        protocol.setProviderListener(listener);
    }

    @Override
    public ProviderListener getProviderListener() {
        return protocol.getProviderListener();
    }
}
