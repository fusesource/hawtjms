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

import io.hawtjms.jms.message.JmsOutboundMessageDispatch;
import io.hawtjms.jms.meta.JmsProducerId;
import io.hawtjms.jms.meta.JmsProducerInfo;
import io.hawtjms.provider.AsyncResult;
import io.hawtjms.util.IdGenerator;

import java.io.IOException;

import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Link;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the case of anonymous JMS MessageProducers.
 *
 * In order to simulate the anonymous producer we must create a sender for each message
 * send attempt and close it following a successful send.
 */
public class AmqpAnonymousProducer extends AmqpProducer {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpAnonymousProducer.class);
    private static final IdGenerator producerIdGenerator = new IdGenerator();

    private final String producerIdKey = producerIdGenerator.generateId();
    private long producerIdCount;

    /**
     * Creates the Anonymous Producer object.
     *
     * @param session
     *        the session that owns this producer
     * @param info
     *        the JmsProducerInfo for this producer.
     */
    public AmqpAnonymousProducer(AmqpSession session, JmsProducerInfo info) {
        super(session, info);
    }

    @Override
    public void send(JmsOutboundMessageDispatch envelope, AsyncResult<Void> request) throws IOException {

        LOG.trace("Started send chain for anonymous producer: {}", getProducerId());

        // Create a new ProducerInfo for the short lived producer that's created to perform the
        // send to the given AMQP target.
        JmsProducerInfo info = new JmsProducerInfo(getNextProducerId());
        info.setDestination(envelope.getDestination());

        // We open a Fixed Producer instance with the target destination.  Once it opens
        // it will trigger the open event which will in turn trigger the send event and
        // when that succeeds it will trigger a close which completes the send chain.
        AmqpFixedProducer producer = new AmqpFixedProducer(session, info);
        AnonymousOpenRequest open = new AnonymousOpenRequest(request, producer, envelope);
        producer.open(open);
    }

    @Override
    public void processUpdates() {
    }

    @Override
    public void open(AsyncResult<Void> request) {
        // Trigger an immediate open, we don't talk to the Broker until
        // a send occurs so we must not let the client block.
        request.onSuccess();
    }

    @Override
    public void close(AsyncResult<Void> request) {
        // Trigger an immediate close, the internal producers that are currently in a send
        // will track their own state and close as the send completes or fails.
        request.onSuccess(null);
    }

    @Override
    protected void doOpen() {
    }

    @Override
    protected void doClose() {
    }

    @Override
    public Link getProtonLink() {
        return null;
    }

    @Override
    public boolean isAnonymous() {
        return true;
    }

    @Override
    public EndpointState getLocalState() {
        return EndpointState.ACTIVE;
    }

    @Override
    public EndpointState getRemoteState() {
        return EndpointState.ACTIVE;
    }

    private JmsProducerId getNextProducerId() {
        return new JmsProducerId(producerIdKey, -1, producerIdCount++);
    }

    private abstract class AnonymousRequest<T> implements AsyncResult<T> {

        protected final AsyncResult<Void> sendResult;
        protected final AmqpProducer producer;
        protected final JmsOutboundMessageDispatch envelope;

        public AnonymousRequest(AsyncResult<Void> sendResult, AmqpProducer producer, JmsOutboundMessageDispatch envelope) {
            this.sendResult = sendResult;
            this.producer = producer;
            this.envelope = envelope;
        }

        @Override
        public boolean isComplete() {
            return sendResult.isComplete();
        }

        @Override
        public void onSuccess() {
            onSuccess(null);
        }

        /**
         * In all cases of the chain of events that make up the send for an anonymous
         * producer a failure will trigger the original send request to fail.
         */
        @Override
        public void onFailure(Throwable result) {
            LOG.debug("Send failed during {} step in chain: {}", this.getClass().getName(), getProducerId());
            sendResult.onFailure(result);
        }
    }

    private final class AnonymousOpenRequest extends AnonymousRequest<Void> {

        public AnonymousOpenRequest(AsyncResult<Void> sendResult, AmqpProducer producer, JmsOutboundMessageDispatch envelope) {
            super(sendResult, producer, envelope);
        }

        @Override
        public void onSuccess(Void result) {
            LOG.trace("Open phase of anonymous send complete: {} ", getProducerId());
            AnonymousSendRequest send = new AnonymousSendRequest(this);
            try {
                producer.send(envelope, send);
            } catch (IOException e) {
                sendResult.onFailure(e);
            }
        }
    }

    private final class AnonymousSendRequest extends AnonymousRequest<Void> {

        public AnonymousSendRequest(AnonymousOpenRequest open) {
            super(open.sendResult, open.producer, open.envelope);
        }

        @Override
        public void onSuccess(Void result) {
            LOG.trace("Send phase of anonymous send complete: {} ", getProducerId());
            AnonymousCloseRequest close = new AnonymousCloseRequest(this);
            producer.close(close);
        }
    }

    private final class AnonymousCloseRequest extends AnonymousRequest<Void> {

        public AnonymousCloseRequest(AnonymousSendRequest send) {
            super(send.sendResult, send.producer, send.envelope);
        }

        @Override
        public void onSuccess(Void result) {
            LOG.trace("Close phase of anonymous send complete: {} ", getProducerId());
            sendResult.onSuccess(null);
        }
    }
}
