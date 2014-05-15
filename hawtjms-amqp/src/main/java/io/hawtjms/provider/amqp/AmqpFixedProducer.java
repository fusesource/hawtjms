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
package io.hawtjms.provider.amqp;

import io.hawtjms.jms.JmsDestination;
import io.hawtjms.jms.message.JmsMessage;
import io.hawtjms.jms.message.JmsOutboundMessageDispatch;
import io.hawtjms.jms.meta.JmsProducerInfo;
import io.hawtjms.provider.AsyncResult;
import io.hawtjms.util.IOExceptionSupport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedList;
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
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.jms.AutoOutboundTransformer;
import org.apache.qpid.proton.jms.EncodedMessage;
import org.apache.qpid.proton.jms.OutboundTransformer;
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

    private final AmqpTransferTagGenerator tagGenerator = new AmqpTransferTagGenerator(true);
    private final Set<Delivery> pending = new LinkedHashSet<Delivery>();
    private final LinkedList<PendingSend> pendingSends = new LinkedList<PendingSend>();

    private final OutboundTransformer outboundTransformer = new AutoOutboundTransformer(AmqpJMSVendor.INSTANCE);
    private final String MESSAGE_FORMAT_KEY = outboundTransformer.getPrefixVendor() + "MESSAGE_FORMAT";

    public AmqpFixedProducer(AmqpSession session, JmsProducerInfo info) {
        super(session, info);
    }

    @Override
    public boolean send(JmsOutboundMessageDispatch envelope, AsyncResult<Void> request) throws IOException {

        // TODO - Handle the case where remote has no credit which means we can't send to it.
        //        We need to hold the send until remote credit becomes available but we should
        //        also have a send timeout option and filter timed out sends.
        if (endpoint.getCredit() <= 0) {
            LOG.trace("Holding Message send until credit is available.");
            // Once a message goes into a held mode we no longer can send it async, so
            // we clear the async flag if set to avoid the sender never getting notified.
            envelope.setSendAsync(false);
            this.pendingSends.addLast(new PendingSend(envelope, request));
            return false;
        } else {
            doSend(envelope, request);
            return true;
        }
    }

    private void doSend(JmsOutboundMessageDispatch envelope, AsyncResult<Void> request) throws IOException {
        LOG.trace("Producer sending message: {}", envelope.getMessage().getFacade().getMessageId());

        byte[] tag = tagGenerator.getNextTag();
        Delivery delivery = endpoint.delivery(tag);
        if (!envelope.isSendAsync()) {
            delivery.setContext(request);
        }
        if (session.isTransacted()) {
            Binary amqpTxId = session.getTransactionContext().getAmqpTransactionId();
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

    @Override
    public void processFlowUpdates() throws IOException {
        if (!pendingSends.isEmpty() && endpoint.getCredit() > 0) {
            while (endpoint.getCredit() > 0 && !pendingSends.isEmpty()) {
                LOG.trace("Dispatching previously held send");
                PendingSend held = pendingSends.pop();
                doSend(held.envelope, held.request);
            }
        }
    }

    @Override
    public void processDeliveryUpdates() {
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
                tagGenerator.returnTag(delivery.getTag());
                if (request != null) {
                    request.onSuccess();
                }
            } else if (state instanceof Rejected) {
                Exception remoteError = getRemoteError();
                toRemove.add(delivery);
                tagGenerator.returnTag(delivery.getTag());
                if (request != null) {
                    request.onFailure(remoteError);
                } else {
                    // TODO - Fire Error to provider listener indicating an error.
                }
            } else {
                LOG.warn("Message send updated with unsupported state: {}", state);
            }
        }

        pending.removeAll(toRemove);
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

        String senderName = sourceAddress + ":" + destnationName != null ? destnationName : "Anonymous";
        endpoint = session.getProtonSession().sender(senderName);
        endpoint.setSource(source);
        endpoint.setTarget(target);
        endpoint.setSenderSettleMode(SenderSettleMode.UNSETTLED);
        endpoint.setReceiverSettleMode(ReceiverSettleMode.FIRST);
    }

    @Override
    protected void doClose() {
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

    @Override
    public String toString() {
        return "AmqpFixedProducer { " + getProducerId() + " }";
    }

    private class PendingSend {

        public JmsOutboundMessageDispatch envelope;
        public AsyncResult<Void> request;

        public PendingSend(JmsOutboundMessageDispatch envelope, AsyncResult<Void> request) {
            this.envelope = envelope;
            this.request = request;
        }
    }
}
