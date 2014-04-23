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

import io.hawtjms.jms.meta.JmsConsumerId;
import io.hawtjms.jms.meta.JmsConsumerInfo;
import io.hawtjms.jms.meta.JmsProducerId;
import io.hawtjms.jms.meta.JmsProducerInfo;
import io.hawtjms.jms.meta.JmsSessionId;
import io.hawtjms.jms.meta.JmsSessionInfo;
import io.hawtjms.provider.AsyncResult;
import io.hawtjms.provider.ProviderRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.JMSException;

/**
 * Represents a logical Session instance for a STOMP connection.
 */
public class StompSession {

    private final StompConnection connection;
    private final JmsSessionInfo sessionInfo;
    private final Map<JmsProducerId, StompProducer> producers = new HashMap<JmsProducerId, StompProducer>();
    private final Map<JmsConsumerId, StompConsumer> consumers = new HashMap<JmsConsumerId, StompConsumer>();

    public StompSession(StompConnection connection, JmsSessionInfo sessionInfo) {
        this.connection = connection;
        this.sessionInfo = sessionInfo;
    }

    /**
     * @return the StompConnection that created this Session.
     */
    public StompConnection getConnection() {
        return this.connection;
    }

    /**
     * @return the assigned JmsSessionId that identifies this Session.
     */
    public JmsSessionId getSessionId() {
        return this.sessionInfo.getSessionId();
    }

    /**
     * Close this logical session ensuring that all resources associated with it are also
     * closed.
     *
     * @param request
     *        the AsyncResult that awaits the close of this session.
     *
     * @throws IOException if an error occurs while closing a consumer.
     */
    public void close(AsyncResult<Void> request) throws IOException {
        if (consumers.isEmpty()) {
            request.onSuccess();
        }

        AggregateResult pending = new AggregateResult(consumers.size(), request);
        for (StompConsumer consumer : consumers.values()) {
            consumer.close(pending);
        }
    }

    /**
     * Creates and returns a new StompProducer which represents a logical mapping of
     * a JmsProducer to a STOMP destination.  There is no information exchange between
     * client and peer on producer create so the producer create is ready immediately.
     *
     * @param producerInfo
     *        the information object describing the producer and its configuration.
     * @param request
     *        the asynchronous request that is waiting for this action to complete.
     */
    public void createProducer(JmsProducerInfo producerInfo, AsyncResult<Void> request) {
        StompProducer producer = new StompProducer(this, producerInfo);
        producers.put(producerInfo.getProducerId(), producer);
        request.onSuccess();
    }

    /**
     * Removes the producer associated with the given JmsProducerId from the producers map.
     *
     * @param producerId
     *        the producer Id that should be removed.
     */
    public void removeProducer(JmsProducerId producerId) {
        producers.remove(producerId);
    }

    /**
     * Creates and returns a new StompConsumer which maps a STOMP subscription to a
     * JMS framework MessageConsumer instance.
     *
     * @param consumerInfo
     *        the information object describing the consumer and its configuration.
     * @param request
     *        the asynchronous request that is waiting for this action to complete.
     *
     * @return a newly created StompConsumer instance.
     *
     * @throws IOException if an error occurs on sending the subscribe request.
     * @throws JMSException if there is an error creating the requested type of subscription.
     */
    public void createConsumer(JmsConsumerInfo consumerInfo, AsyncResult<Void> request) throws JMSException, IOException {
        if (consumerInfo.getPrefetchSize() == 0) {
            throw new JMSException("Cannot create a consumer with Zero Prefetch in STOMP");
        }

        StompConsumer consumer;
        if (consumerInfo.isBrowser()) {
            consumer = new StompQueueBrowser(this, consumerInfo);
        } else {
            consumer = new StompConsumer(this, consumerInfo);
        }
        consumers.put(consumerInfo.getConsumerId(), consumer);
        consumer.subscribe(request);
    }

    /**
     * Removes the producer associated with the given JmsProducerId from the producers map.
     *
     * @param consumerId
     *        the consumer Id that is to be removed from the consumers map.
     */
    public void removeConsumer(JmsConsumerId consumerId) {
        consumers.remove(consumerId);
    }

    /**
     * For all consumers in this session force an acknowledge of all unacknowledged messages.
     *
     * @param request
     *        the request that awaits the completion of this operation.
     *
     * @throws IOException if an error occurs sending any ACK frames.
     */
    public void acknowledge(AsyncResult<Void> request) throws IOException {
        AggregateResult pending = new AggregateResult(consumers.size(), request);
        for (StompConsumer consumer : consumers.values()) {
            consumer.acknowledge(pending);
        }
    }

    /**
     *
     */
    public void recover() {
        // TODO Auto-generated method stub
    }

    /**
     * @param request
     */
    public void rollback(AsyncResult<Void> request) {
        // TODO Auto-generated method stub
    }

    /**
     * @param request
     */
    public void commit(AsyncResult<Void> request) {
        // TODO Auto-generated method stub
    }

    public StompProducer getProducer(JmsProducerInfo producerInfo) {
        return getProducer(producerInfo.getProducerId());
    }

    public StompProducer getProducer(JmsProducerId producerId) {
        if (producerId.getProviderHint() instanceof StompProducer) {
            return (StompProducer) producerId.getProviderHint();
        }
        return this.producers.get(producerId);
    }

    public StompConsumer getConsumer(JmsConsumerInfo consumerInfo) {
        return getConsumer(consumerInfo.getConsumerId());
    }

    public StompConsumer getConsumer(JmsConsumerId consumerId) {
        if (consumerId.getProviderHint() instanceof StompConsumer) {
            return (StompConsumer) consumerId.getProviderHint();
        }
        return this.consumers.get(consumerId);
    }

    //---------- Internal Utilities ------------------------------------------//

    /*
     * Implements an accumulating result monitor that watches for all active consumers
     * at the time of the Session Acknowledge call to complete the acknowledge of all
     * their delivered messages.
     */
    private static class AggregateResult extends ProviderRequest<Void> {

        private final AtomicInteger consumers;

        public AggregateResult(int numConsumers, AsyncResult<Void> request) {
            super(request);
            this.consumers = new AtomicInteger(numConsumers);
        }

        @Override
        public void onSuccess(Void result) {
            if (consumers.decrementAndGet() == 0) {
                super.onSuccess(result);
            }
        }
    }
}
