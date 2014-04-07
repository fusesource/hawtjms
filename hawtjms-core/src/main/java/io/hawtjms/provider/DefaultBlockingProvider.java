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
package io.hawtjms.provider;

import io.hawtjms.jms.message.JmsInboundMessageDispatch;
import io.hawtjms.jms.message.JmsMessageFactory;
import io.hawtjms.jms.message.JmsOutboundMessageDispatch;
import io.hawtjms.jms.meta.JmsConsumerId;
import io.hawtjms.jms.meta.JmsResource;
import io.hawtjms.jms.meta.JmsSessionId;
import io.hawtjms.provider.ProviderConstants.ACK_TYPE;

import java.io.IOException;
import java.net.URI;

import javax.jms.JMSException;

/**
 * Provides a simple Provider Facade that allows a ProtocolProvider to be used
 * without any additional capabilities wrapped around it such as Failover or
 * Discovery.  All methods are executed as blocking operations.
 */
public class DefaultBlockingProvider implements BlockingProvider {

    private final AsyncProvider next;

    public DefaultBlockingProvider(AsyncProvider protocol) {
        this.next = protocol;
    }

    @Override
    public void connect() throws IOException {
        next.connect();
    }

    @Override
    public void start() throws IOException, IllegalStateException {
        next.start();
    }

    @Override
    public void close() {
        next.close();
    }

    @Override
    public URI getRemoteURI() {
        return next.getRemoteURI();
    }

    @Override
    public void create(JmsResource resource) throws IOException, JMSException, UnsupportedOperationException {
        ProviderRequest<Void> request = new ProviderRequest<Void>();
        next.create(resource, request);
        request.getResponse();
    }

    @Override
    public void start(JmsResource resource) throws IOException, JMSException {
        ProviderRequest<Void> request = new ProviderRequest<Void>();
        next.start(resource, request);
        request.getResponse();
    }

    @Override
    public void destroy(JmsResource resource) throws IOException, JMSException, UnsupportedOperationException {
        ProviderRequest<Void> request = new ProviderRequest<Void>();
        next.destroy(resource, request);
        request.getResponse();
    }

    @Override
    public void send(JmsOutboundMessageDispatch envelope) throws IOException, JMSException {
        ProviderRequest<Void> request = new ProviderRequest<Void>();
        next.send(envelope, request);
        request.getResponse();
    }

    @Override
    public void acknowledge(JmsSessionId sessionId) throws IOException, JMSException {
        ProviderRequest<Void> request = new ProviderRequest<Void>();
        next.acknowledge(sessionId, request);
        request.getResponse();
    }

    @Override
    public void acknowledge(JmsInboundMessageDispatch envelope, ACK_TYPE ackType) throws IOException, JMSException {
        ProviderRequest<Void> request = new ProviderRequest<Void>();
        next.acknowledge(envelope, ackType, request);
        request.getResponse();
    }

    @Override
    public void commit(JmsSessionId sessionId) throws IOException, JMSException, UnsupportedOperationException {
        ProviderRequest<Void> request = new ProviderRequest<Void>();
        next.commit(sessionId, request);
        request.getResponse();
    }

    @Override
    public void rollback(JmsSessionId sessionId) throws IOException, JMSException, UnsupportedOperationException {
        ProviderRequest<Void> request = new ProviderRequest<Void>();
        next.rollback(sessionId, request);
        request.getResponse();
    }

    @Override
    public void recover(JmsSessionId sessionId) throws IOException, UnsupportedOperationException {
        ProviderRequest<Void> request = new ProviderRequest<Void>();
        next.recover(sessionId, request);
        request.getResponse();
    }

    @Override
    public void unsubscribe(String subscription) throws IOException, JMSException, UnsupportedOperationException {
        ProviderRequest<Void> request = new ProviderRequest<Void>();
        next.unsubscribe(subscription, request);
        request.getResponse();
    }

    @Override
    public void pull(JmsConsumerId consumerId, long timeout) throws IOException, UnsupportedOperationException {
        ProviderRequest<Void> request = new ProviderRequest<Void>();
        next.pull(consumerId, timeout, request);
        request.getResponse();
    }

    @Override
    public JmsMessageFactory getMessageFactory() {
        return next.getMessageFactory();
    }

    @Override
    public void setProviderListener(ProviderListener listener) {
        next.setProviderListener(listener);
    }

    @Override
    public ProviderListener getProviderListener() {
        return next.getProviderListener();
    }

    public AsyncProvider getNext() {
        return next;
    }
}
