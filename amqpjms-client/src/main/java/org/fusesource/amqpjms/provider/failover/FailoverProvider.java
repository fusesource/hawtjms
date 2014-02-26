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

import org.fusesource.amqpjms.jms.message.JmsOutboundMessageDispatch;
import org.fusesource.amqpjms.jms.meta.JmsResource;
import org.fusesource.amqpjms.provider.Provider;
import org.fusesource.amqpjms.provider.ProviderListener;
import org.fusesource.amqpjms.provider.ProviderRequest;

/**
 * A Provider Facade that provides services for detection dropped Provider connections
 * and attempting to reconnect to a different remote peer.  Upon establishment of a new
 * connection the FailoverProvider will initiate state recovery of the active JMS
 * framework resources.
 */
public class FailoverProvider implements Provider {

    private ProviderListener proxied;

    @Override
    public void connect() throws IOException {
        // TODO Auto-generated method stub
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
    }

    @Override
    public void receoveryComplate() throws IOException {
        // TODO Auto-generated method stub
    }

    @Override
    public URI getRemoteURI() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ProviderRequest<JmsResource> create(JmsResource resource) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ProviderRequest<Void> destroy(JmsResource resource) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ProviderRequest<Void> send(JmsOutboundMessageDispatch envelope) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setProviderListener(ProviderListener listener) {
        this.proxied = listener;
    }

    @Override
    public ProviderListener getProviderListener() {
        return proxied;
    }
}
