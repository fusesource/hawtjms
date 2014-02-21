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

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Session;
import org.fusesource.amqpjms.jms.meta.JmsConsumerId;
import org.fusesource.amqpjms.jms.meta.JmsConsumerInfo;
import org.fusesource.amqpjms.jms.meta.JmsProducerId;
import org.fusesource.amqpjms.jms.meta.JmsProducerInfo;
import org.fusesource.amqpjms.jms.meta.JmsResource;
import org.fusesource.amqpjms.jms.meta.JmsSessionId;
import org.fusesource.amqpjms.jms.meta.JmsSessionInfo;
import org.fusesource.amqpjms.provider.ProviderRequest;

public class AmqpSession {

    private final AmqpConnection connection;
    private final JmsSessionInfo info;
    private Session protonSession;

    private ProviderRequest<JmsResource> openRequest;
    private ProviderRequest<Void> closeRequest;

    private final Map<JmsConsumerId, AmqpConsumer> consumers = new HashMap<JmsConsumerId, AmqpConsumer>();
    private final Map<JmsProducerId, AmqpProducer> producers = new HashMap<JmsProducerId, AmqpProducer>();

    public AmqpSession(AmqpConnection connection, JmsSessionInfo info) {
        this.connection = connection;
        this.info = info;
    }

    public void open(ProviderRequest<JmsResource> request) {
        this.protonSession = connection.getProtonConnection().session();
        this.protonSession.setContext(this);
        this.protonSession.open();
        this.openRequest = request;
    }

    public boolean isOpen() {
        return this.protonSession.getRemoteState() == EndpointState.ACTIVE;
    }

    public void opened() {
        if (openRequest != null) {
            openRequest.onSuccess(info);
            openRequest = null;
        }
    }

    public void close(ProviderRequest<Void> request) {
        this.protonSession.close();
        this.closeRequest = request;
    }

    public boolean isClosed() {
        return this.protonSession.getRemoteState() == EndpointState.CLOSED;
    }

    public void closed() {
        if (closeRequest != null) {
            closeRequest.onSuccess(null);
            closeRequest = null;
        }
    }

    public AmqpProducer createProducer(JmsProducerInfo producerInfo, ProviderRequest<JmsResource> request) {
        return new AmqpProducer(this, producerInfo);
    }

    public AmqpProducer getProducer(JmsProducerInfo producerInfo) {
        // TODO - Hide producer in the hint field in the Id.
        return this.producers.get(producerInfo.getProducerId());
    }

    public AmqpConsumer createConsumer(JmsConsumerInfo consumerInfo, ProviderRequest<JmsResource> request) {
        return new AmqpConsumer(this, consumerInfo);
    }

    public AmqpConsumer getConsumer(JmsConsumerInfo consumerInfo) {
        // TODO - Hide producer in the hint field in the Id.
        return this.consumers.get(consumerInfo.getConsumerId());
    }

    /**
     * Called from the parent Connection to check for and react to state changes in the
     * underlying Proton connection which might indicate a sender / receiver / link state
     * has changed.
     */
    public void processUpdates() {

    }

    public AmqpConnection getConnection() {
        return this.connection;
    }

    public JmsSessionId getSessionId() {
        return this.info.getSessionId();
    }

    public Session getProtonSession() {
        return this.protonSession;
    }
}
