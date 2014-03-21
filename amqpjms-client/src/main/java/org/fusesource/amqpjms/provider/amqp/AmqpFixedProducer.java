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

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.transaction.TransactionalState;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.jms.AutoOutboundTransformer;
import org.apache.qpid.proton.jms.EncodedMessage;
import org.apache.qpid.proton.jms.OutboundTransformer;
import org.fusesource.amqpjms.jms.JmsDestination;
import org.fusesource.amqpjms.jms.message.JmsMessage;
import org.fusesource.amqpjms.jms.message.JmsOutboundMessageDispatch;
import org.fusesource.amqpjms.jms.meta.JmsProducerInfo;
import org.fusesource.amqpjms.provider.AsyncResult;
import org.fusesource.amqpjms.util.IOExceptionSupport;
import org.fusesource.hawtbuf.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AMQP Producer object that is used to manage JMS MessageProducer semantics.
 *
 * This Producer is fixed to a given JmsDestination and can only produce messages to it.
 */
public class AmqpFixedProducer extends AmqpProducer {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpFixedProducer.class);

    private long nextTagId;
    private boolean dynamic;
    private final Set<byte[]> tagCache = new LinkedHashSet<byte[]>();
    private final Set<Delivery> pending = new LinkedHashSet<Delivery>();

    private final OutboundTransformer outboundTransformer = new AutoOutboundTransformer(AmqpJMSVendor.INSTANCE);
    private final String MESSAGE_FORMAT_KEY = outboundTransformer.getPrefixVendor() + "MESSAGE_FORMAT";

    public AmqpFixedProducer(AmqpSession session, JmsProducerInfo info) {
        super(session, info);
    }

    @Override
    public void send(JmsOutboundMessageDispatch envelope, AsyncResult<Void> request) throws IOException {
        LOG.info("Producer sending message: {}", envelope.getMessage().getMessageId());

        byte[] tag = borrowTag();
        Delivery delivery = endpoint.delivery(tag);
        delivery.setContext(request);
        if (envelope.getTransactionId() != null) {
            Binary amqpTxId = (Binary) envelope.getTransactionId().getProviderHint();
            TransactionalState state = new TransactionalState();
            state.setTxnId(amqpTxId);
            state.setOutcome(Accepted.getInstance());
            delivery.disposition(state);
        }

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

            @SuppressWarnings("unchecked")
            AsyncResult<Void> request = (AsyncResult<Void>) delivery.getContext();

            if (state instanceof TransactionalState) {
                LOG.info("State of delivery is Transacted: {}", state);
            } else if (state instanceof Accepted) {
                toRemove.add(delivery);
                returnTag(delivery.getTag());
                request.onSuccess(null);
            } else if (state instanceof Rejected) {
                Exception remoteError = getRemoteError();
                toRemove.add(delivery);
                returnTag(delivery.getTag());
                request.onFailure(remoteError);
            } else {
                LOG.warn("Message send updated with unsupported state: {}", state);
            }
        }

        pending.removeAll(toRemove);

        // TODO - Check for and handle endpoint detached state.
    }

    @Override
    protected void doOpen() {
        JmsDestination destination = info.getDestination();

        String destnationName = session.getQualifiedName(destination);
        String sourceAddress = getProducerId().toString();
        Source source = new Source();
        source.setAddress(sourceAddress);
        Target target = new Target();
        target.setAddress(destnationName);
        target.setDynamic(isDynamic());

        String senderName = sourceAddress + ":" + destnationName != null ? destnationName : "Anonymous";
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
    public Link getProtonLink() {
        return this.endpoint;
    }

    public AmqpSession getSession() {
        return this.session;
    }

    public Sender getProtonSender() {
        return this.endpoint;
    }

    @Override
    public boolean isAnonymous() {
        return false;
    }

    public boolean isDynamic() {
        return dynamic;
    }

    public void setDynamic(boolean dynamic) {
        this.dynamic = dynamic;
    }
}
