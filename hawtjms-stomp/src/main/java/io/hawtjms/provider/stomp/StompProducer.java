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

import io.hawtjms.jms.message.JmsOutboundMessageDispatch;
import io.hawtjms.jms.meta.JmsProducerId;
import io.hawtjms.jms.meta.JmsProducerInfo;
import io.hawtjms.jms.meta.JmsSessionId;
import io.hawtjms.provider.AsyncResult;
import io.hawtjms.provider.stomp.message.StompJmsMessageFacade;

import java.io.IOException;

/**
 * Producer class that wraps the details of message send operations over
 * STOMP.
 */
public class StompProducer {

    private final JmsProducerInfo producerInfo;
    private final StompSession session;
    private final StompConnection connection;

    /**
     * Creates a new StompProducer instance with the given parent session and
     * configures the producer using the provided JmsProducerInfo.
     *
     * @param session
     *        this producers parent session instance.
     * @param producerInfo
     *        the producer information that defines this producer.
     */
    public StompProducer(StompSession session, JmsProducerInfo producerInfo) {
        this.session = session;
        this.connection = session.getConnection();
        this.producerInfo = producerInfo;

        this.producerInfo.getProducerId().setProviderHint(this);
    }

    /**
     * Removes this producer from its parent session.  Since the producer objects are
     * simply logical mappings there's nothing else that needs to be done here.
     *
     * @param request
     *        the asynchronous request instance awaiting completion of this action.
     */
    public void close(AsyncResult<Void> request) {
        session.removeProducer(getProducerId());
        request.onSuccess();
    }

    /**
     * Sends the given message to the indicated STOMP destination.
     *
     * @param envelope
     *        the envelope that contains the delivery information.
     * @param request
     *        the asynchronous request that will await the completed send operation.
     */
    public void send(JmsOutboundMessageDispatch envelope, AsyncResult<Void> request) throws IOException {
        StompJmsMessageFacade facade = (StompJmsMessageFacade) envelope.getMessage().getFacade();
        StompFrame sendFrame = facade.getStompMessage();

        // TODO - Get current TX Id and append it to the Frame.

        // Frame will be marshaled by the Providers Codec.
        connection.request(sendFrame, request);
    }

    public JmsProducerId getProducerId() {
        return this.producerInfo.getProducerId();
    }

    public JmsSessionId getSessionId() {
        return this.producerInfo.getParentId();
    }

    public StompSession getSession() {
        return this.session;
    }
}
