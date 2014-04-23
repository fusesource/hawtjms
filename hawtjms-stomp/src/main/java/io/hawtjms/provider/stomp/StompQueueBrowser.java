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
import io.hawtjms.jms.meta.JmsConsumerInfo;

import javax.jms.JMSException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * QueueBrowser implementation for those STOMP servers that support
 * doing a Queue browse.  The browser will check incoming messages for
 * the end of browse indicator using the configured STOMP server adapter.
 */
public class StompQueueBrowser extends StompConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(StompQueueBrowser.class);

    /**
     * Creates a new STOMP Queue Browser.
     *
     * @param session
     *        the parent of this Queue Browser.
     * @param consumerInfo
     *        the consumer information that identifies this browser instance.
     */
    public StompQueueBrowser(StompSession session, JmsConsumerInfo consumerInfo) {
        super(session, consumerInfo);
    }

    @Override
    public void processMessage(StompFrame message) throws JMSException {
        if (adapter.isEndOfBrowse(message)) {
            LOG.debug("Received end of browse frame: {}", getConsumerId());
            JmsInboundMessageDispatch browseDone = new JmsInboundMessageDispatch();
            browseDone.setConsumerId(getConsumerId());
            browseDone.setProviderHint(message);
            connection.getProvider().getProviderListener().onMessage(browseDone);
        } else {
            super.processMessage(message);
        }
    }

    @Override
    public boolean isBrowser() {
        return true;
    }
}
