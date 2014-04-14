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

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a logical Session instance for a STOMP connection.
 */
public class StompSession {

    private final JmsSessionInfo sessionInfo;
    private final Map<JmsProducerId, StompProducer> producers = new HashMap<JmsProducerId, StompProducer>();
    private final Map<JmsConsumerId, StompConsumer> consumers = new HashMap<JmsConsumerId, StompConsumer>();

    public StompSession(JmsSessionInfo sessionInfo) {
        this.sessionInfo = sessionInfo;
    }

    public JmsSessionId getSessionId() {
        return this.sessionInfo.getSessionId();
    }

    /**
     * Close this logical session ensuring that all resources associated with it are also
     * closed.
     *
     * @param request
     *        the AsyncResult that awaits the close of this session.
     */
    public void close(AsyncResult<Void> request) {
    }

    /**
     * Creates and returns a new StompProducer which represents a logical mapping of
     * a JmsProducer to a STOMP destination.  There is no information exchange between
     * client and peer on producer create so the producer create is ready immediately.
     *
     * @param producerInfo
     *        the information object describing the producer and its configuration.
     *
     * @return a newly created StompProducer instance.
     */
    public StompProducer createProducer(JmsProducerInfo producerInfo) {
        StompProducer producer = new StompProducer(this, producerInfo);
        producers.put(producerInfo.getProducerId(), producer);
        return producer;
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
     *
     * @return a newly created StompConsumer instance.
     */
    public StompConsumer createConsumer(JmsConsumerInfo consumerInfo) {
        return null;
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

    public StompProducer getProducer(JmsProducerId producerId) {
        return this.producers.get(producerId);
    }

    public StompConsumer getConsumer(JmsConsumerId consumerId) {
        return this.consumers.get(consumerId);
    }
}
