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

import javax.jms.JMSException;

import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Session;
import org.fusesource.amqpjms.jms.meta.JmsConsumerId;
import org.fusesource.amqpjms.jms.meta.JmsConsumerInfo;
import org.fusesource.amqpjms.jms.meta.JmsProducerId;
import org.fusesource.amqpjms.jms.meta.JmsProducerInfo;
import org.fusesource.amqpjms.jms.meta.JmsResource;
import org.fusesource.amqpjms.jms.meta.JmsSessionId;
import org.fusesource.amqpjms.jms.meta.JmsSessionInfo;
import org.fusesource.amqpjms.provider.ProviderRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpSession implements AmqpResource {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpSession.class);

    private final AmqpConnection connection;
    private final JmsSessionInfo info;
    private Session protonSession;

    private ProviderRequest<JmsResource> openRequest;
    private ProviderRequest<Void> closeRequest;

    private final Map<JmsConsumerId, AmqpConsumer> consumers = new HashMap<JmsConsumerId, AmqpConsumer>();
    private final Map<JmsProducerId, AmqpProducer> producers = new HashMap<JmsProducerId, AmqpProducer>();

    private final ArrayList<AmqpLink> pendingOpenLinks = new ArrayList<AmqpLink>();
    private final ArrayList<AmqpLink> pendingCloseLinks = new ArrayList<AmqpLink>();

    public AmqpSession(AmqpConnection connection, JmsSessionInfo info) {
        this.connection = connection;
        this.info = info;
    }

    @Override
    public void open(ProviderRequest<JmsResource> request) {
        this.connection.addToPendingOpenSessions(this);
        this.protonSession = connection.getProtonConnection().session();
        this.protonSession.setContext(this);
        this.protonSession.open();
        this.openRequest = request;
    }

    @Override
    public boolean isOpen() {
        return this.protonSession.getRemoteState() == EndpointState.ACTIVE;
    }

    @Override
    public void opened() {
        if (openRequest != null) {
            openRequest.onSuccess(info);
            openRequest = null;
        }
    }

    @Override
    public void close(ProviderRequest<Void> request) {
        this.connection.addToPendingCloseSessions(this);
        this.protonSession.close();
        this.closeRequest = request;
    }

    @Override
    public boolean isClosed() {
        return this.protonSession.getRemoteState() == EndpointState.CLOSED;
    }

    @Override
    public void closed() {
        if (closeRequest != null) {
            closeRequest.onSuccess(null);
            closeRequest = null;
        }
    }

    @Override
    public void failed() {
        // TODO - Figure out a real exception to throw.
        if (openRequest != null) {
            openRequest.onFailure(new JMSException("Failed to create Session"));
            openRequest = null;
        }

        if (closeRequest != null) {
            closeRequest.onFailure(new JMSException("Failed to create Session"));
            closeRequest = null;
        }
    }

    public AmqpProducer createProducer(JmsProducerInfo producerInfo) {
        return new AmqpProducer(this, producerInfo);
    }

    public AmqpProducer getProducer(JmsProducerInfo producerInfo) {
        // TODO - Hide producer in the hint field in the Id.
        return this.producers.get(producerInfo.getProducerId());
    }

    public AmqpConsumer createConsumer(JmsConsumerInfo consumerInfo) {
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
        processPendingLinks();
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

    public AmqpConnection getConnection() {
        return this.connection;
    }

    public JmsSessionId getSessionId() {
        return this.info.getSessionId();
    }

    public Session getProtonSession() {
        return this.protonSession;
    }

    void addPedingLinkOpen(AmqpLink link) {
        this.pendingOpenLinks.add(link);
    }

    void addPedingLinkClose(AmqpLink link) {
        this.pendingCloseLinks.add(link);
    }
}
