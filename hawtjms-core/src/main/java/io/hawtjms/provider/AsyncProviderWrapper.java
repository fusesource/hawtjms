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
 * Allows one AsyncProvider instance to wrap around another and provide some additional
 * features beyond the normal AsyncProvider interface.
 */
public class AsyncProviderWrapper<E extends AsyncProvider> implements AsyncProvider {

    protected final E next;

    public AsyncProviderWrapper(E next) {
        this.next = next;
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
    public void create(JmsResource resource, AsyncResult<Void> request) throws IOException, JMSException, UnsupportedOperationException {
        next.create(resource, request);
    }

    @Override
    public void start(JmsResource resource, AsyncResult<Void> request) throws IOException {
        next.start(resource, request);
    }

    @Override
    public void destroy(JmsResource resourceId, AsyncResult<Void> request) throws IOException, JMSException, UnsupportedOperationException {
        next.destroy(resourceId, request);
    }

    @Override
    public void send(JmsOutboundMessageDispatch envelope, AsyncResult<Void> request) throws IOException, JMSException {
        next.send(envelope, request);
    }

    @Override
    public void acknowledge(JmsSessionId sessionId, AsyncResult<Void> request) throws IOException {
        next.acknowledge(sessionId, request);
    }

    @Override
    public void acknowledge(JmsInboundMessageDispatch envelope, ACK_TYPE ackType, AsyncResult<Void> request) throws IOException {
        next.acknowledge(envelope, ackType, request);
    }

    @Override
    public void commit(JmsSessionId sessionId, AsyncResult<Void> request) throws IOException, JMSException, UnsupportedOperationException {
        next.commit(sessionId, request);
    }

    @Override
    public void rollback(JmsSessionId sessionId, AsyncResult<Void> request) throws IOException, JMSException, UnsupportedOperationException {
        next.rollback(sessionId, request);
    }

    @Override
    public void recover(JmsSessionId sessionId, AsyncResult<Void> request) throws IOException, UnsupportedOperationException {
        next.recover(sessionId, request);
    }

    @Override
    public void unsubscribe(String subscription, AsyncResult<Void> request) throws IOException, JMSException, UnsupportedOperationException {
        next.unsubscribe(subscription, request);
    }

    @Override
    public void pull(JmsConsumerId consumerId, long timeout, AsyncResult<Void> request) throws IOException, UnsupportedOperationException {
        next.pull(consumerId, timeout, request);
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

    /**
     * @return the wrapped AsyncProvider.
     */
    public AsyncProvider getNext() {
        return this.next;
    }
}
