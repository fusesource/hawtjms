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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.jms.AutoOutboundTransformer;
import org.apache.qpid.proton.jms.EncodedMessage;
import org.apache.qpid.proton.jms.OutboundTransformer;
import org.fusesource.amqpjms.jms.message.JmsMessage;
import org.fusesource.amqpjms.jms.message.JmsOutboundMessageDispatch;
import org.fusesource.amqpjms.jms.meta.JmsProducerId;
import org.fusesource.amqpjms.jms.meta.JmsProducerInfo;
import org.fusesource.amqpjms.provider.ProviderRequest;
import org.fusesource.amqpjms.util.IOExceptionSupport;
import org.fusesource.hawtbuf.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AMQP Producer object that is used to manage JMS MessageProducer semantics.
 */
public class AmqpProducer extends AbstractAmqpResource<JmsProducerInfo, Sender> implements AmqpLink {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpProducer.class);

    private final AmqpSession session;
    private long nextTagId;
    private final Set<byte[]> tagCache = new LinkedHashSet<byte[]>();
    private final Set<Delivery> pending = new LinkedHashSet<Delivery>();

    private final OutboundTransformer outboundTransformer = new AutoOutboundTransformer(AmqpJMSVendor.INSTANCE);
    private final String MESSAGE_FORMAT_KEY = outboundTransformer.getPrefixVendor() + "MESSAGE_FORMAT";

    public AmqpProducer(AmqpSession session, JmsProducerInfo info) {
        super(info);
        this.session = session;

        // Add a shortcut back to this Producer for quicker lookup.
        this.info.getProducerId().setProviderHint(this);
    }

    public void send(JmsOutboundMessageDispatch envelope, ProviderRequest<Void> request) throws IOException {
        LOG.info("Producer sending message: {}", envelope.getMessage().getMessageId());

        byte[] tag = borrowTag();
        Delivery delivery = endpoint.delivery(tag);
        delivery.setContext(request);

        JmsMessage message = envelope.getMessage();
        message.setReadOnlyBody(true);

        if (!message.getProperties().containsKey(MESSAGE_FORMAT_KEY)) {
            message.setProperty(MESSAGE_FORMAT_KEY, 0);
        }

        Buffer sendBuffer = null;
        EncodedMessage amqp = null;

        try {
            amqp = outboundTransformer.transform(message);
        } catch (Exception e) {
            throw IOExceptionSupport.create(e);
        }

        if (amqp != null && amqp.getLength() > 0) {
            sendBuffer = new Buffer(amqp.getArray(), amqp.getArrayOffset(), amqp.getLength());
        }

        while (sendBuffer != null) {
            int sent = endpoint.send(sendBuffer.data, sendBuffer.offset, sendBuffer.length);
            if (sent > 0) {
                sendBuffer.moveHead(sent);
                if (sendBuffer.length == 0) {
                    endpoint.advance();
                    pending.add(delivery);
                    sendBuffer = null;
                }
            } else {
                LOG.warn("{} failed to send any data from current Message.", this);
            }
        }
    }

    private byte[] borrowTag() throws IOException {
        byte[] rc;
        if (tagCache != null && !tagCache.isEmpty()) {
            final Iterator<byte[]> iterator = tagCache.iterator();
            rc = iterator.next();
            iterator.remove();
        } else {
            rc = Long.toHexString(nextTagId++).getBytes("UTF-8");
        }
        return rc;
    }

    private void returnTag(byte[] data) {
        if (tagCache.size() < 1024) {
            tagCache.add(data);
        }
    }

    @Override
    public void processUpdates() {
        List<Delivery> toRemove = new ArrayList<Delivery>();

        for (Delivery delivery : pending) {
            DeliveryState state = delivery.getRemoteState();
            if (state == null) {
                continue;
            }

            if (Accepted.getInstance().equals(state)) {
                @SuppressWarnings("unchecked")
                ProviderRequest<Void> request = (ProviderRequest<Void>) delivery.getContext();
                request.onSuccess(null);
                returnTag(delivery.getTag());
            } else {
                // TODO - figure out how to handle not accepted.
                LOG.info("Message send failed: {}", state);
            }
        }

        pending.removeAll(toRemove);
    }

    @Override
    protected void doOpen() {
        String destnationName = session.getQualifiedName(info.getDestination());
        String sourceAddress = UUID.randomUUID().toString();
        Source source = new Source();
        source.setAddress(sourceAddress);
        Target target = new Target();
        target.setAddress(destnationName);
        target.setDynamic(info.getDestination().isTemporary());

        String senderName = destnationName + "<-" + sourceAddress;
        endpoint = session.getProtonSession().sender(senderName);
        endpoint.setSource(source);
        endpoint.setTarget(target);
        endpoint.setSenderSettleMode(SenderSettleMode.UNSETTLED);
        endpoint.setReceiverSettleMode(ReceiverSettleMode.FIRST);

        this.session.addPedingLinkOpen(this);
    }

    @Override
    protected void doClose() {
        this.session.addPedingLinkClose(this);
    }

    @Override
    public Object getRemoteTerminus() {
        return this.endpoint.getTarget();
    }

    @Override
    public Link getProtonLink() {
        return this.endpoint;
    }

    public AmqpSession getSession() {
        return this.session;
    }

    public Sender getProtonSender() {
        return this.endpoint;
    }

    public JmsProducerId getProducerId() {
        return this.info.getProducerId();
    }
}
