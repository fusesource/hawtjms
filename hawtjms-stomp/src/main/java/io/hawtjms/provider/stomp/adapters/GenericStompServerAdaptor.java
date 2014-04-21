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

import static io.hawtjms.provider.stomp.StompConstants.ID;
import static io.hawtjms.provider.stomp.StompConstants.UNSUBSCRIBE;
import io.hawtjms.jms.JmsDestination;
import io.hawtjms.jms.JmsQueue;
import io.hawtjms.jms.JmsTopic;
import io.hawtjms.jms.meta.JmsConsumerInfo;
import io.hawtjms.provider.stomp.StompConnection;
import io.hawtjms.provider.stomp.StompFrame;

import javax.jms.JMSException;

/**
 * Base adapter class for supporting STOMP servers that offer extended features
 * beyond the defined STOMP specification.
 */
public class GenericStompServerAdaptor implements StompServerAdapter {

    private final String version;
    private final StompConnection connection;

    public GenericStompServerAdaptor(StompConnection connection) {
        this.version = "Unknown";
        this.connection = connection;
    }

    public GenericStompServerAdaptor(StompConnection connection, String version) {
        this.version = version;
        this.connection = connection;
    }

    @Override
    public StompConnection getStompConnection() {
        return this.connection;
    }

    @Override
    public String getQueuePrefix() {
        return connection.getQueuePrefix();
    }

    @Override
    public String getTopicPrefix() {
        return connection.getTopicPrefix();
    }

    @Override
    public String getTempQueuePrefix() {
        return connection.getTempQueuePrefix();
    }

    @Override
    public String getTempTopicPrefix() {
        return connection.getTempTopicPrefix();
    }

    @Override
    public String toStompDestination(JmsDestination destination) {
        String result = null;

        if (destination.isTopic()) {
            if (destination.isTemporary()) {
                result = getTempTopicPrefix() + destination.getName();
            } else {
                result = getTopicPrefix() + destination.getName();
            }
        } else {
            if (destination.isTemporary()) {
                result = getTempQueuePrefix() + destination.getName();
            } else {
                result = getQueuePrefix() + destination.getName();
            }
        }

        return result;
    }

    @Override
    public JmsDestination toJmsDestination(String destinationName) throws JMSException {
        JmsDestination result = null;

        if (destinationName == null) {
            return null;
        }

        if (destinationName.startsWith(getTopicPrefix())) {
            result = new JmsTopic(destinationName.substring(getTopicPrefix().length()));
        } else if (destinationName.startsWith(getQueuePrefix())) {
            result = new JmsQueue(destinationName.substring(getQueuePrefix().length()));
        } else if (destinationName.startsWith(getTempTopicPrefix())) {
            result = new JmsQueue(destinationName.substring(getTempTopicPrefix().length()));
        } else if (destinationName.startsWith(getTempQueuePrefix())) {
            result = new JmsQueue(destinationName.substring(getTempQueuePrefix().length()));
        } else {
            throw new JMSException("Cannot transform unknown destination type: " + destinationName);
        }

        return result;
    }

    @Override
    public StompFrame createCreditFrame(StompFrame messageFrame) {
        return null;
    }

    @Override
    public void addSubscribeHeaders(StompFrame frame, JmsConsumerInfo consumerInfo) throws JMSException {
        if (consumerInfo.isBrowser()) {
            throw new JMSException("Server does not support browsing over STOMP");
        }
        if (consumerInfo.isNoLocal()) {
            throw new JMSException("Server does not support 'no local' semantics over STOMP");
        }
        if (consumerInfo.isDurable()) {
            throw new JMSException("Server does not durable subscriptions over STOMP");
        }
    }

    @Override
    public boolean isEndOfBrowse(StompFrame message) {
        return false;
    }

    @Override
    public StompFrame createUnsubscribeFrame(JmsConsumerInfo consumerInfo) throws JMSException {
        if (consumerInfo.isDurable()) {
            throw new JMSException("Server does not support un-subscribing durable subscriptions over STOMP");
        }
        StompFrame frame = new StompFrame();
        frame.setCommand(UNSUBSCRIBE);
        frame.getProperties().put(ID, consumerInfo.getConsumerId().toString());
        return frame;
    }

    @Override
    public String getServerVersion() {
        return this.version;
    }

    @Override
    public String getServerName() {
        return "Generic";
    }
}
