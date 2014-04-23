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
package io.hawtjms.provider.stomp;

import static io.hawtjms.provider.stomp.StompConstants.ACK;
import static io.hawtjms.provider.stomp.StompConstants.ACK_ID;
import static io.hawtjms.provider.stomp.StompConstants.ACK_MODE;
import static io.hawtjms.provider.stomp.StompConstants.DESTINATION;
import static io.hawtjms.provider.stomp.StompConstants.ID;
import static io.hawtjms.provider.stomp.StompConstants.MESSAGE_ID;
import static io.hawtjms.provider.stomp.StompConstants.SELECTOR;
import static io.hawtjms.provider.stomp.StompConstants.SUBSCRIBE;
import static io.hawtjms.provider.stomp.StompConstants.SUBSCRIPTION;
import static io.hawtjms.provider.stomp.StompConstants.UNSUBSCRIBE;
import io.hawtjms.jms.message.JmsInboundMessageDispatch;
import io.hawtjms.jms.message.JmsMessage;
import io.hawtjms.jms.meta.JmsConsumerId;
import io.hawtjms.jms.meta.JmsConsumerInfo;
import io.hawtjms.jms.meta.JmsMessageId;
import io.hawtjms.jms.meta.JmsSessionId;
import io.hawtjms.provider.AsyncResult;
import io.hawtjms.provider.ProviderConstants.ACK_TYPE;
import io.hawtjms.provider.ProviderListener;
import io.hawtjms.provider.stomp.adapters.StompServerAdapter;

import java.io.IOException;
import java.util.LinkedList;

import javax.jms.JMSException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * STOMP Consumer class used to manage the subscription process for
 * STOMP and handle the various ACK modes and mappings to JMS.
 */
public class StompConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(StompConsumer.class);

    protected final JmsConsumerInfo consumerInfo;
    protected final StompSession session;
    protected final StompConnection connection;
    protected final StompServerAdapter adapter;
    protected boolean started;

    protected final LinkedList<JmsInboundMessageDispatch> delivered =
        new LinkedList<JmsInboundMessageDispatch>();

    /**
     * Create a new STOMP Consumer that maps a STOMP subscription to a JMS Framework
     * MessageConsumer.
     *
     * @param session
     *        the session that acts as this consumer's parent.
     * @param consumerInfo
     *        the information object that defines this consumer instance.
     */
    public StompConsumer(StompSession session, JmsConsumerInfo consumerInfo) {
        this.consumerInfo = consumerInfo;
        this.session = session;
        this.connection = session.getConnection();
        this.adapter = connection.getServerAdapter();

        this.consumerInfo.getConsumerId().setProviderHint(this);
    }

    /**
     * Places the consumer in the started state.  Messages delivered prior to a consumer being
     * started should be held and only dispatched after start.
     */
    public void start() {
        this.started = true;
    }

    /**
     * Performs the actual subscription by creating an appropriate SUBSCRIBE
     * frame and sending it to the server.
     *
     * @param request
     *        the request that initiated this operation.
     *
     * @throws JMSException if the subscription requested is not supported or invalid.
     * @throws IOException if an error occurs while sending the frame.
     */
    public void subscribe(AsyncResult<Void> request) throws JMSException, IOException {
        StompFrame subscribe = new StompFrame(SUBSCRIBE);
        subscribe.setProperty(ID, consumerInfo.getConsumerId().toString());
        subscribe.setProperty(DESTINATION, adapter.toStompDestination(consumerInfo.getDestination()));
        // For either the Auto case or the Client Ack case we use client so we can control the flow
        // of messages based on prefetch and delivery.
        // TODO - We could add an Individual Ack Mode to the JMS frameworks and let the provider
        //        error out if that's not supported.
        subscribe.setProperty(ACK_MODE, "client");
        if (consumerInfo.getSelector() != null) {
            subscribe.setProperty(SELECTOR, consumerInfo.getSelector());
        }

        adapter.addSubscribeHeaders(subscribe, consumerInfo);
        connection.request(subscribe, request);
    }

    /**
     * Close the consumer by sending an UNSUBSCRIBE command to the remote peer and then
     * removing the consumer from the session's state information.
     *
     * @param request
     *        the request that initiated this operation/
     */
    public void close(AsyncResult<Void> request) throws IOException {
        session.removeConsumer(getConsumerId());
        StompFrame frame = new StompFrame(UNSUBSCRIBE);
        frame.setProperty(ID, consumerInfo.getConsumerId().toString());
        connection.request(frame, request);
    }

    /**
     * Handles any incoming messages for this consumer.  The Frame is converted into the
     * appropriate JmsMessage type and fired to the provider listener.
     *
     * @param message
     *        the incoming STOMP MESSAGE frame to dispatch.
     *
     * @throws JMSException if an error occurs while processing the frame.
     */
    public void processMessage(StompFrame message) throws JMSException {
        JmsMessage converted = adapter.convertToJmsMessage(message);

        JmsInboundMessageDispatch envelope = new JmsInboundMessageDispatch();
        envelope.setConsumerId(consumerInfo.getConsumerId());
        envelope.setMessage(converted);
        envelope.setProviderHint(message);

        connection.getProvider().getProviderListener().onMessage(envelope);
    }

    /**
     * Called to acknowledge all messages that have been marked as delivered but
     * have not yet been marked consumed.  Usually this is called as part of an
     * client acknowledge session operation.
     *
     * Only messages that have already been acknowledged as delivered by the JMS
     * framework will be in the delivered Map.
     *
     * @param request
     *        the request that awaits completion of this action.
     *
     * @throws IOException if an error occurs while sending the ACK frame.
     */
    public void acknowledge(AsyncResult<Void> request) throws IOException {
        LOG.trace("Session Acknowledge for consumer: {}", getConsumerId());

        // STOMP client Ack messages are cumulative so one frame is all we need.
        if (!delivered.isEmpty()) {
            JmsInboundMessageDispatch envelope = delivered.getLast();
            acknowledge(envelope, ACK_TYPE.CONSUMED, request);
            delivered.clear();
        } else {
            request.onSuccess();
        }
    }

    /**
     * Acknowledge the message that was delivered in the given envelope based on the
     * given acknowledgment type.
     *
     * @param envelope
     *        the envelope that contains the delivery information for the message.
     * @param ackType
     *        the type of acknowledge operation that should be performed.
     * @param request
     *        the asynchronous request awaiting completion of this operation.
     *
     * @throws IOException if an error occurs while writing the frame.
     */
    public void acknowledge(JmsInboundMessageDispatch envelope, ACK_TYPE ackType, AsyncResult<Void> request) throws IOException {
        StompFrame messageFrame = (StompFrame) envelope.getProviderHint();
        JmsMessageId messageId = envelope.getMessage().getFacade().getMessageId();

        if (ackType.equals(ACK_TYPE.DELIVERED)) {
            LOG.debug("Delivered Ack of message: {}", messageId);
            delivered.add(envelope);
            StompFrame credit = adapter.createCreditFrame(messageFrame);
            if (credit != null) {
                connection.send(credit);
            }
            request.onSuccess();
        } else if (ackType.equals(ACK_TYPE.CONSUMED)) {
            LOG.debug("Consumed Ack of message: {}", messageId);
            delivered.remove(envelope);
            StompFrame ack = new StompFrame(ACK);
            ack.setProperty(MESSAGE_ID, messageId.toString());
            ack.setProperty(SUBSCRIPTION, getConsumerId().toString());

            String ackHeader = messageFrame.getProperty(ACK_ID);
            if (ackHeader != null) {
                ack.setProperty(ID, ackHeader);
            }

            // TODO - Transaction.

            connection.request(ack, request);
            return;
        } else if (ackType.equals(ACK_TYPE.REDELIVERED)) {
            LOG.debug("Redelivered Ack of message: {}", messageId);
            request.onSuccess();
        } else if (ackType.equals(ACK_TYPE.POISONED)) {
            LOG.debug("Poisoned Ack of message: {}", messageId);
            request.onSuccess();
        } else {
            LOG.warn("Unsupporeted Ack Type for message: {}", messageId);
            request.onFailure(new JMSException("Failed to send ack for: " + messageId));
        }
    }

    public JmsConsumerId getConsumerId() {
        return this.consumerInfo.getConsumerId();
    }

    public JmsSessionId getSessionId() {
        return this.consumerInfo.getParentId();
    }

    public StompSession getSession() {
        return this.session;
    }

    public boolean isStarted() {
        return started;
    }

    public boolean isBrowser() {
        return false;
    }

    //---------- Internal helper methods -------------------------------------//

    protected void deliver(JmsInboundMessageDispatch envelope) {
        ProviderListener listener = connection.getProvider().getProviderListener();
        if (listener != null) {
            if (envelope.getMessage() != null) {
                LOG.debug("Dispatching received message: {}", envelope.getMessage().getFacade().getMessageId());
            } else {
                LOG.debug("Dispatching end of browse to: {}", envelope.getConsumerId());
            }
            listener.onMessage(envelope);
        } else {
            LOG.error("Provider listener is not set, message will be dropped.");
        }
    }
}
