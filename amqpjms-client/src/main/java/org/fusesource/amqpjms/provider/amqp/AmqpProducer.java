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

import java.util.UUID;

import javax.jms.JMSException;

import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Sender;
import org.fusesource.amqpjms.jms.meta.JmsProducerId;
import org.fusesource.amqpjms.jms.meta.JmsProducerInfo;
import org.fusesource.amqpjms.jms.meta.JmsResource;
import org.fusesource.amqpjms.provider.ProviderRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AMQP Producer object that is used to manage JMS MessageProducer semantics.
 */
public class AmqpProducer implements AmqpLink {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpProducer.class);

    private final AmqpSession session;
    private final JmsProducerInfo info;
    private Sender protonSender;

    private ProviderRequest<JmsResource> openRequest;
    private ProviderRequest<Void> closeRequest;

    public AmqpProducer(AmqpSession session, JmsProducerInfo info) {
        this.session = session;
        this.info = info;
    }

    @Override
    public void open(ProviderRequest<JmsResource> request) {
        String destnationName = info.getDestination().getName();
        String sourceAddress = UUID.randomUUID().toString();
        Source source = new Source();
        source.setAddress(sourceAddress);
        Target target = new Target();
        target.setAddress(destnationName);

        String senderName = destnationName + "<-" + sourceAddress;
        protonSender = session.getProtonSession().sender(senderName);
        protonSender.setSource(source);
        protonSender.setTarget(target);
        protonSender.setSenderSettleMode(SenderSettleMode.UNSETTLED);
        protonSender.setReceiverSettleMode(ReceiverSettleMode.FIRST);
        protonSender.setContext(this);
        protonSender.open();

        this.openRequest = request;
        this.session.addPedingLinkOpen(this);
    }

    @Override
    public boolean isOpen() {
        return this.protonSender.getRemoteState() == EndpointState.ACTIVE;
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
        this.session.addPedingLinkClose(this);
        this.protonSender.close();
        this.closeRequest = request;
    }

    @Override
    public boolean isClosed() {
        return this.protonSender.getRemoteState() == EndpointState.CLOSED;
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

    @Override
    public Object getRemoteTerminus() {
        return this.protonSender.getTarget();
    }

    @Override
    public Link getProtonLink() {
        return this.protonSender;
    }

    public AmqpSession getSession() {
        return this.session;
    }

    public Sender getProtonSender() {
        return this.protonSender;
    }

    public JmsProducerId getProducerId() {
        return this.info.getProducerId();
    }
}
