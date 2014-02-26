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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Session;
import org.fusesource.amqpjms.jms.meta.JmsConsumerId;
import org.fusesource.amqpjms.jms.meta.JmsConsumerInfo;
import org.fusesource.amqpjms.jms.meta.JmsProducerId;
import org.fusesource.amqpjms.jms.meta.JmsProducerInfo;
import org.fusesource.amqpjms.jms.meta.JmsSessionId;
import org.fusesource.amqpjms.jms.meta.JmsSessionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpSession extends AbstractAmqpResource<JmsSessionInfo, Session> {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpSession.class);

    private final AmqpConnection connection;

    private final Map<JmsConsumerId, AmqpConsumer> consumers = new HashMap<JmsConsumerId, AmqpConsumer>();
    private final Map<JmsProducerId, AmqpProducer> producers = new HashMap<JmsProducerId, AmqpProducer>();

    private final ArrayList<AmqpLink> pendingOpenLinks = new ArrayList<AmqpLink>();
    private final ArrayList<AmqpLink> pendingCloseLinks = new ArrayList<AmqpLink>();

    public AmqpSession(AmqpConnection connection, JmsSessionInfo info) {
        super(info, connection.getProtonConnection().session());
        this.connection = connection;

        this.info.getSessionId().setProviderHint(this);
    }

    @Override
    protected void doOpen() {
        this.connection.addToPendingOpenSessions(this);
    }

    @Override
    protected void doClose() {
        this.connection.addToPendingCloseSessions(this);
    }

    public AmqpProducer createProducer(JmsProducerInfo producerInfo) {
        return new AmqpProducer(this, producerInfo);
    }

    public AmqpProducer getProducer(JmsProducerInfo producerInfo) {
        return getProducer(producerInfo.getProducerId());
    }

    public AmqpProducer getProducer(JmsProducerId producerId) {
        if (producerId.getProviderHint() instanceof AmqpProducer) {
            return (AmqpProducer) producerId.getProviderHint();
        }
        return this.producers.get(producerId);
    }

    public AmqpConsumer createConsumer(JmsConsumerInfo consumerInfo) {
        return new AmqpConsumer(this, consumerInfo);
    }

    public AmqpConsumer getConsumer(JmsConsumerInfo consumerInfo) {
        return getConsumer(consumerInfo.getConsumerId());
    }

    public AmqpConsumer getConsumer(JmsConsumerId consumerId) {
        if (consumerId.getProviderHint() instanceof AmqpConsumer) {
            return (AmqpConsumer) consumerId.getProviderHint();
        }
        return this.consumers.get(consumerId);
    }

    /**
     * Called from the parent Connection to check for and react to state changes in the
     * underlying Proton connection which might indicate a sender / receiver / link state
     * has changed.
     */
    @Override
    public void processUpdates() {
        processPendingLinks();

        // Settle any pending deliveries.
        for (AmqpProducer producer : this.producers.values()) {
            producer.processUpdates();
        }

        for (AmqpConsumer consumer : this.consumers.values()) {
            consumer.processUpdates();
        }
    }

    private void processPendingLinks() {

        if (pendingOpenLinks.isEmpty() && pendingCloseLinks.isEmpty()) {
            return;
        }

        // TODO - revisit and clean this up, it's a bit ugly right now.

        Iterator<AmqpLink> linkIterator = pendingOpenLinks.iterator();
        while (linkIterator.hasNext()) {
            AmqpLink candidate = linkIterator.next();
            Link protonLink = candidate.getProtonLink();

            LOG.info("Checking link {} for open status: ", candidate);

            EndpointState linkRemoteState = protonLink.getRemoteState();
            if (linkRemoteState == EndpointState.ACTIVE || linkRemoteState == EndpointState.CLOSED) {
                if (linkRemoteState == EndpointState.ACTIVE && candidate.getRemoteTerminus() != null) {
                    candidate.opened();

                    if (candidate instanceof AmqpConsumer) {
                        AmqpConsumer consumer = (AmqpConsumer) candidate;
                        consumers.put(consumer.getConsumerId(), consumer);
                    } else {
                        AmqpProducer producer = (AmqpProducer) candidate;
                        producers.put(producer.getProducerId(), producer);
                    }

                } else {
                    // TODO - Can we derive an exception from here.
                    candidate.failed();
                }

                linkIterator.remove();
            }
        }

        linkIterator = pendingCloseLinks.iterator();
        while (linkIterator.hasNext()) {
            AmqpLink candidate = linkIterator.next();
            if (candidate.isClosed()) {
                candidate.closed();
                linkIterator.remove();
            }
        }
    }

    public AmqpProvider getProvider() {
        return this.connection.getProvider();
    }

    public AmqpConnection getConnection() {
        return this.connection;
    }

    public JmsSessionId getSessionId() {
        return this.info.getSessionId();
    }

    public Session getProtonSession() {
        return this.endpoint;
    }

    void addPedingLinkOpen(AmqpLink link) {
        this.pendingOpenLinks.add(link);
    }

    void addPedingLinkClose(AmqpLink link) {
        this.pendingCloseLinks.add(link);
    }
}
