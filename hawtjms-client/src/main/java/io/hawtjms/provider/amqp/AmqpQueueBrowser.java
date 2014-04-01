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

import io.hawtjms.jms.message.JmsInboundMessageDispatch;
import io.hawtjms.jms.meta.JmsConsumerInfo;
import io.hawtjms.provider.AsyncResult;

import org.apache.qpid.proton.amqp.messaging.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Queue Browser implementation for AMQP
 */
public class AmqpQueueBrowser extends AmqpConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpQueueBrowser.class);

    /**
     * @param session
     * @param info
     */
    public AmqpQueueBrowser(AmqpSession session, JmsConsumerInfo info) {
        super(session, info);
    }

    /**
     * Starts the QueueBrowser by activating drain mode with the initial credits.
     */
    @Override
    public void start(AsyncResult<Void> request) {
        this.endpoint.flow(info.getPrefetchSize());
        request.onSuccess();
    }

    /**
     * QueueBrowser will attempt to initiate a pull whenever there are no pending Messages.
     *
     * We need to initiate a drain to see if there are any messages and if the remote sender
     * indicates it is drained then we can send end of browse.  We only do this when there
     * are no pending incoming deliveries and all delivered messages have become settled
     * in order to give the remote a chance to dispatch more messages once all deliveries
     * have been settled.
     *
     * @param timeout
     *        ignored in this context.
     */
    @Override
    public void pull(long timeout) {
        if (!endpoint.getDrain() && endpoint.current() == null && endpoint.getUnsettled() == 0) {
            LOG.trace("QueueBrowser {} will try to drain remote.", getConsumerId());
            this.endpoint.drain(info.getPrefetchSize());
        }
    }

    @Override
    public void processUpdates() {
        if (endpoint.getDrain() && endpoint.current() != null) {
            LOG.trace("{} incoming delivery, cancel drain.", getConsumerId());
            endpoint.setDrain(false);
        }
        super.processUpdates();

        if (endpoint.getDrain() && endpoint.getCredit() == endpoint.getRemoteCredit()) {
            JmsInboundMessageDispatch browseDone = new JmsInboundMessageDispatch();
            browseDone.setConsumerId(getConsumerId());
            deliver(browseDone);
        } else {
            endpoint.setDrain(false);
        }
    }

    @Override
    protected void configureSource(Source source) {
        if (info.isBrowser()) {
            source.setDistributionMode(COPY);
        }

        super.configureSource(source);
    }

    @Override
    public boolean isBrowser() {
        return true;
    }
}
