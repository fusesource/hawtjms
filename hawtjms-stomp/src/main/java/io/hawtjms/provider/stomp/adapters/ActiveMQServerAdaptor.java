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
package io.hawtjms.provider.stomp.adapters;

import static io.hawtjms.provider.stomp.StompConstants.BROWSER;
import static io.hawtjms.provider.stomp.StompConstants.TRUE;
import io.hawtjms.jms.meta.JmsConsumerInfo;
import io.hawtjms.provider.stomp.StompConnection;
import io.hawtjms.provider.stomp.StompFrame;

import javax.jms.JMSException;

/**
 * Server Adapter instance used to interact with an ActiveMQ Broker.
 */
public class ActiveMQServerAdaptor extends GenericStompServerAdaptor {

    private static final String SUBSCRIPTION_NAME = "activemq.subscriptionName";
    private static final String NO_LOCAL = "activemq.noLocal";
    private static final String PREFETCH = "activemq.prefetchSize";

    /**
     * Creates a new ActiveMQ Server Adapter instance.
     *
     * @param connection
     *        the connection instance that owns this ServerAdapter.
     * @param version
     *        the version of ActiveMQ that we've connected to.
     */
    public ActiveMQServerAdaptor(StompConnection connection, String version) {
        super(connection, version);
    }

    @Override
    public String getServerName() {
        return "ActiveMQ";
    }

    @Override
    public String getQueuePrefix() {
        return "/queue/";
    }

    @Override
    public String getTopicPrefix() {
        return "/topic/";
    }

    @Override
    public String getTempQueuePrefix() {
        return "/temp-queue/";
    }

    @Override
    public String getTempTopicPrefix() {
        return "/temp-topic/";
    }

    @Override
    public void addSubscribeHeaders(StompFrame frame, JmsConsumerInfo consumerInfo) throws JMSException {
        String version = getServerVersion();

        if (consumerInfo.isBrowser()) {
            if (version == null || version.isEmpty()) {
                throw new JMSException("ActiveMQ does not support browsing over STOMP");
            }

            // 5.6 and above reports it's version and support Queue browser.
            frame.setProperty(BROWSER, "true");
        }
        if (consumerInfo.isNoLocal()) {
            frame.setProperty(NO_LOCAL, TRUE);
        }
        if (consumerInfo.isDurable()) {
            frame.setProperty(SUBSCRIPTION_NAME, consumerInfo.getSubscriptionName());
        }

        frame.setProperty(PREFETCH, String.valueOf(consumerInfo.getPrefetchSize()));
    }

    @Override
    public boolean isEndOfBrowse(StompFrame message) {
        String browser = message.getProperty(BROWSER);
        if (browser != null && !browser.isEmpty()) {
            return browser.equalsIgnoreCase("end");
        } else {
            return false;
        }
    }
}
