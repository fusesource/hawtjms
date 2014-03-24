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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.jms.JMSException;

import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.jms.EncodedMessage;
import org.apache.qpid.proton.jms.InboundTransformer;
import org.apache.qpid.proton.jms.JMSMappingInboundTransformer;
import org.fusesource.amqpjms.jms.JmsDestination;
import org.fusesource.amqpjms.jms.message.JmsInboundMessageDispatch;
import org.fusesource.amqpjms.jms.message.JmsMessage;
import org.fusesource.amqpjms.jms.meta.JmsConsumerId;
import org.fusesource.amqpjms.jms.meta.JmsConsumerInfo;
import org.fusesource.amqpjms.jms.meta.JmsMessageId;
import org.fusesource.amqpjms.provider.AsyncResult;
import org.fusesource.amqpjms.provider.ProviderConstants.ACK_TYPE;
import org.fusesource.amqpjms.provider.ProviderListener;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.ByteArrayOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AMQP Consumer object that is used to manage JMS MessageConsumer semantics.
 */
public class AmqpConsumer extends AbstractAmqpResource<JmsConsumerInfo, Receiver> implements AmqpLink {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpConsumer.class);

    private static final Symbol COPY = Symbol.getSymbol("copy");
    private static final Symbol JMS_NO_LOCAL_SYMBOL = Symbol.valueOf("no-local");
    private static final Symbol JMS_SELECTOR_SYMBOL = Symbol.valueOf("jms-selector");

    private final AmqpSession session;
    private final InboundTransformer inboundTransformer =
        new JMSMappingInboundTransformer(AmqpJMSVendor.INSTANCE);;
    private final Map<JmsMessageId, Delivery> delivered = new LinkedHashMap<JmsMessageId, Delivery>();

    public AmqpConsumer(AmqpSession session, JmsConsumerInfo info) {
        super(info);
        this.session = session;

        // Add a shortcut back to this Consumer for quicker lookups
        this.info.getConsumerId().setProviderHint(this);
    }

    /**
     * Starts the consumer by setting the link credit to the given prefetch value.
     */
    public void start(AsyncResult<Void> request) {
        this.endpoint.flow(info.getPrefetchSize());
        request.onSuccess(null);
    }

    /**
     * Process all incoming deliveries that have been fully read.
     */
    @Override
    public void processUpdates() {
        Delivery incoming = null;
        do {
            incoming = endpoint.current();
            if (incoming != null && incoming.isReadable() && !incoming.isPartial()) {
                LOG.trace("{} has incoming Message(s).", this);
                processDelivery(incoming);
            } else {
                incoming = null;
            }
            endpoint.advance();
        } while (incoming != null);
    }

    @Override
    protected void doOpen() {
        JmsDestination destination  = info.getDestination();
        String subscription = session.getQualifiedName(destination);

        Source source = new Source();
        source.setAddress(subscription);
        Target target = new Target();

        configureSource(source);

        String receiverName = getConsumerId() + ":" + subscription;
        if (info.getSubscriptionName() != null && !info.getSubscriptionName().isEmpty()) {
            // In the case of Durable Topic Subscriptions the client must use the same
            // receiver name which is derived from the subscription name property.
            receiverName = info.getSubscriptionName();
        }

        endpoint = session.getProtonSession().receiver(receiverName);
        endpoint.setSource(source);
        endpoint.setTarget(target);
        endpoint.setSenderSettleMode(SenderSettleMode.UNSETTLED);
        endpoint.setReceiverSettleMode(ReceiverSettleMode.FIRST);

        this.session.addPedingLinkOpen(this);
    }

    protected void configureSource(Source source) {
        Map<Symbol, DescribedType> filters = new HashMap<Symbol, DescribedType>();

        if (info.isBrowser()) {
            source.setDistributionMode(COPY);
        }

        if (info.getSubscriptionName() != null && !info.getSubscriptionName().isEmpty()) {
            source.setExpiryPolicy(TerminusExpiryPolicy.NEVER);
            source.setDurable(TerminusDurability.UNSETTLED_STATE);
            source.setDistributionMode(COPY);
        } else {
            source.setDurable(TerminusDurability.NONE);
            source.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
        }

        if (info.isNoLocal()) {
            filters.put(JMS_NO_LOCAL_SYMBOL, AmqpJmsNoLocalType.NO_LOCAL);
        }

        if (info.getSelector() != null && !info.getSelector().trim().equals("")) {
            filters.put(JMS_SELECTOR_SYMBOL, new AmqpJmsSelectorType(info.getSelector()));
        }

        if (!filters.isEmpty()) {
            source.setFilter(filters);
        }
    }

    /**
     * Called to acknowledge all messages that have been marked as delivered but
     * have not yet been marked consumed.  Usually this is called as part of an
     * client acknowledge session operation.
     *
     * Only messages that have already been acknowledged as delivered by the JMS
     * framework will be in the delivered Map.  This means that the link credit
     * would already have been given for these so we just need to settle them.
     */
    public void acknowledge() {
        LOG.debug("Session Acknowledge for consumer: {}", info.getConsumerId());
        for (Delivery delivery : delivered.values()) {
            delivery.disposition(Accepted.getInstance());
            delivery.settle();
        }

        // TODO - currently Proton is not tracking it's own unsettled messages, so we
        //        have to track them.  We could remove the delivered collection once this
        //        is implemented in Proton.
        //
        // Iterator<Delivery> pending = endpoint.unsettled();
        // while (pending != null && pending.hasNext()) {
        //     Delivery delivery = pending.next();
        //     delivery.disposition(Accepted.getInstance());
        //     delivery.settle();
        // }

        delivered.clear();
    }

    /**
     * Called to acknowledge a given delivery.  Depending on the Ack Mode that
     * the consumer was created with this method can acknowledge more than just
     * the target delivery.
     *
     * @param envelope
     *        the delivery that is to be acknowledged.
     * @param ackType
     *        the type of acknowledgment to perform.
     */
    public void acknowledge(JmsInboundMessageDispatch envelope, ACK_TYPE ackType) {
        JmsMessageId messageId = envelope.getMessage().getMessageId();
        Delivery delivery = null;

        if (messageId.getProviderHint() instanceof Delivery) {
            delivery = (Delivery) messageId.getProviderHint();
        } else {
            delivery = delivered.get(messageId);
            if (delivery == null) {
                LOG.warn("Received Ack for unknown message: {}", envelope.getMessage().getMessageId());
                return;
            }
        }

        if (ackType.equals(ACK_TYPE.DELIVERED)) {
            LOG.debug("Delivered Ack of message: {}", messageId);
            delivered.put(messageId, delivery);
            if (info.getPrefetchSize() > 0) {
                endpoint.flow(1);
            }
        } else if (ackType.equals(ACK_TYPE.CONSUMED)) {
            // A Consumer may not always send a delivered ACK so we need to check to
            // ensure we don't add to much credit to the link.
            if (delivered.remove(messageId) == null) {
                if (info.getPrefetchSize() > 0) {
                    endpoint.flow(1);
                }
            }
            LOG.debug("Consumed Ack of message: {}", messageId);
            delivery.disposition(Accepted.getInstance());
            delivery.settle();
        } else {
            LOG.warn("Unsupporeted Ack Type for message: {}", messageId);
        }

        // TODO - handle the other ack modes, poisoned, redelivered
    }


    /**
     * For a consumer whose prefetch value is set to zero this method will attempt to solicite
     * a new message dispatch from the broker.
     *
     * @param timeout
     */
    public void pull(long timeout) {
        if (info.getPrefetchSize() == 0 && endpoint.getCredit() == 0) {
            // expand the credit window by one.
            endpoint.flow(1);
        }
    }

    protected void processDelivery(Delivery incoming) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Buffer buffer;

        try {
            int count;
            byte data[] = new byte[1024 * 4];
            while ((count = endpoint.recv(data, 0, data.length)) > 0) {
                stream.write(data, 0, count);
            }

            buffer = stream.toBuffer();
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
            }
        }

        EncodedMessage em = new EncodedMessage(incoming.getMessageFormat(), buffer.data, buffer.offset, buffer.length);
        JmsMessage message = null;
        try {
            message = (JmsMessage) inboundTransformer.transform(em);
        } catch (Exception e) {
            LOG.warn("Error on transform: {}", e.getMessage());
            // TODO - We could signal provider error but not sure we want to fail
            //        the connection just because we can't convert the message.
            deliveryFailed(incoming);
            return;
        }

        try {
            message.setJMSDestination(info.getDestination());
        } catch (JMSException e) {
            LOG.warn("Error on transform: {}", e.getMessage());
            // TODO - We could signal provider error but not sure we want to fail
            //        the connection just because we can't convert the message.
            deliveryFailed(incoming);
            return;
        }

        message.getMessageId().setProviderHint(incoming);

        JmsInboundMessageDispatch envelope = new JmsInboundMessageDispatch();
        envelope.setMessage(message);
        envelope.setConsumerId(info.getConsumerId());
        envelope.setProviderHint(incoming);

        ProviderListener listener = session.getProvider().getProviderListener();
        if (listener != null) {
            LOG.debug("Dispatching received message: {}", message.getMessageId());
            listener.onMessage(envelope);
        }
    }

    private void deliveryFailed(Delivery incoming) {
        Modified disposition = new Modified();
        disposition.setUndeliverableHere(true);
        disposition.setDeliveryFailed(true);
        incoming.disposition(disposition);
        incoming.settle();
        endpoint.flow(1);
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

    public JmsConsumerId getConsumerId() {
        return this.info.getConsumerId();
    }

    public Receiver getProtonReceiver() {
        return this.endpoint;
    }

    @Override
    public String toString() {
        return "AmqpConsumer: " + this.info.getConsumerId();
    }
}
