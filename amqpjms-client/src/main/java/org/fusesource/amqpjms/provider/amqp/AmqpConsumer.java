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

import java.util.LinkedHashMap;
import java.util.Map;

import javax.jms.JMSException;

import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
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

    @Override
    public void processUpdates() {

        Delivery incoming = endpoint.current();
        if (incoming != null && incoming.isReadable() && !incoming.isPartial()) {
            LOG.debug("{} has incoming Message(s).", this);

            ByteArrayOutputStream stream = new ByteArrayOutputStream();

            int count;
            byte data[] = new byte[1024 * 4];
            while ((count = endpoint.recv(data, 0, data.length)) > 0) {
                stream.write(data, 0, count);
            }

            endpoint.advance();
            Buffer buffer = stream.toBuffer();

            EncodedMessage em = new EncodedMessage(incoming.getMessageFormat(), buffer.data, buffer.offset, buffer.length);
            JmsMessage message = null;
            try {
                message = (JmsMessage) inboundTransformer.transform(em);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                LOG.warn("Error on transform: {}", e.getMessage());
            }

            LOG.info("Received incoming message: {}", message);

            try {
                message.setJMSDestination(info.getDestination());
            } catch (JMSException e) {
                // TODO Auto-generated catch block
                LOG.warn("Error on transform: {}", e.getMessage());
            }

            message.getMessageId().setProviderHint(incoming);

            JmsInboundMessageDispatch envelope = new JmsInboundMessageDispatch();
            envelope.setMessage(message);
            envelope.setConsumerId(info.getConsumerId());

            ProviderListener listener = session.getProvider().getProviderListener();
            if (listener != null) {
                LOG.trace("Dispatching received message: {}", message.getMessageId());
                listener.onMessage(envelope);
            }
        }
    }

    @Override
    protected void doOpen() {
        JmsDestination destination  = info.getDestination();
        String subscription = session.getQualifiedName(destination);

        Source source = new Source();
        source.setAddress(subscription);
        if (destination.isQueue()) {
            source.setDynamic(destination.isTemporary());
        }
        Target target = new Target();

        endpoint = session.getProtonSession().receiver(subscription);
        endpoint.setSource(source);
        endpoint.setTarget(target);
        endpoint.setSenderSettleMode(SenderSettleMode.UNSETTLED);
        endpoint.setReceiverSettleMode(ReceiverSettleMode.FIRST);

        this.session.addPedingLinkOpen(this);
    }

    @Override
    public void opened() {
        // TODO - this may be better applied in a start method.
        // this.endpoint.flow(info.getPrefetchSize());
        this.endpoint.flow(1);
        super.opened();
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
            LOG.warn("Received Ack for unknown message: {}", envelope.getMessage().getMessageId());
            return;
        }

        if (ackType.equals(ACK_TYPE.DELIVERED)) {
            LOG.info("Delivered Ack of message: {}", messageId);
            delivered.put(messageId, delivery);
            endpoint.flow(1);
        } else if (ackType.equals(ACK_TYPE.CONSUMED)) {
            // An Auto Ack consumer won't deliver Ack first it just marks the message
            // as consumed so we need to increase link credit here for that case.
            if (delivered.remove(messageId) == null) {
                endpoint.flow(1);
            }

            LOG.info("Consumed Ack of message: {}", messageId);
            delivery.disposition(Accepted.getInstance());
            delivery.settle();
        } else {
            LOG.warn("Unsupporeted Ack Type for message: {}", messageId);
        }

        // TODO - If not in hint search for it in pending.
        // TODO - based on ack mode we might need to ack all previous.
        // TODO - handle the other ack modes, poisoned, redelivered
    }

    @Override
    protected void doClose() {
        this.session.addPedingLinkClose(this);
    }

    @Override
    public Link getProtonLink() {
        return this.endpoint;
    }

    @Override
    public Object getRemoteTerminus() {
        return this.endpoint.getSource();
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
