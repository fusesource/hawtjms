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

import java.util.LinkedList;
import java.util.List;

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
import org.fusesource.amqpjms.jms.message.JmsInboundMessageDispatch;
import org.fusesource.amqpjms.jms.message.JmsMessage;
import org.fusesource.amqpjms.jms.message.JmsMessageId;
import org.fusesource.amqpjms.jms.meta.JmsConsumerId;
import org.fusesource.amqpjms.jms.meta.JmsConsumerInfo;
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
    private final List<Delivery> pending = new LinkedList<Delivery>();

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
        String subscription = session.getQualifiedName(info.getDestination());

        Source source = new Source();
        source.setAddress(subscription);
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
     */
    public void acknowledge() {

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
        LOG.debug("Processing ack for message: {}", messageId);
        if (messageId.getProviderHint() instanceof Delivery) {
            Delivery incoming = (Delivery) messageId.getProviderHint();

            // TODO - For now we are just acking right away.  Later we need to track
            //        pending acks and wait for the JMS consumer to ack.
            endpoint.flow(1);
            incoming.disposition(Accepted.getInstance());
            incoming.settle();
        }

        // TODO - If not in hint search for it in pending.
        // TODO - based on ack mode we might need to ack all previous.
        // TODO - delivered increments flow, but if no delivered then consume must do it.
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
