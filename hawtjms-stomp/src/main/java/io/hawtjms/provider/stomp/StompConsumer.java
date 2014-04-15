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

import io.hawtjms.jms.message.JmsInboundMessageDispatch;
import io.hawtjms.jms.meta.JmsConsumerId;
import io.hawtjms.jms.meta.JmsConsumerInfo;
import io.hawtjms.jms.meta.JmsSessionId;
import io.hawtjms.provider.AsyncResult;
import io.hawtjms.provider.ProviderConstants.ACK_TYPE;

import java.io.IOException;

/**
 * STOMP Consumer class used to manage the subscription process for
 * STOMP and handle the various ACK modes and mappings to JMS.
 */
public class StompConsumer {

    private final JmsConsumerInfo consumerInfo;
    private final StompSession session;
    private boolean started;

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
     * Close the consumer by sending an UNSUBSCRIBE command to the remote peer and then
     * removing the consumer from the session's state information.
     * @param request
     */
    public void close(AsyncResult<Void> request) throws IOException {
        session.removeConsumer(getConsumerId());
        // TODO - send STOMP UNSUBSCRIBE and register to receive the response.
        request.onSuccess();
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
     */
    public void acknowledge(JmsInboundMessageDispatch envelope, ACK_TYPE ackType, AsyncResult<Void> request) {
        // TODO Auto-generated method stub
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
}
