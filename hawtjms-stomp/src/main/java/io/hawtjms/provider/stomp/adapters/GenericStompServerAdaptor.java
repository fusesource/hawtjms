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

import static io.hawtjms.provider.stomp.StompConstants.CONTENT_LENGTH;
import static io.hawtjms.provider.stomp.StompConstants.CONTENT_TYPE;
import static io.hawtjms.provider.stomp.StompConstants.ID;
import static io.hawtjms.provider.stomp.StompConstants.TRANSFORMATION;
import static io.hawtjms.provider.stomp.StompConstants.UNSUBSCRIBE;
import io.hawtjms.jms.JmsDestination;
import io.hawtjms.jms.JmsQueue;
import io.hawtjms.jms.JmsTopic;
import io.hawtjms.jms.message.JmsMessage;
import io.hawtjms.jms.meta.JmsConsumerInfo;
import io.hawtjms.provider.stomp.StompConnection;
import io.hawtjms.provider.stomp.StompFrame;
import io.hawtjms.provider.stomp.message.StompJmsBytesMessage;
import io.hawtjms.provider.stomp.message.StompJmsMessageFacade;
import io.hawtjms.provider.stomp.message.StompJmsMessageFactory;
import io.hawtjms.provider.stomp.message.StompJmsTextMessage;

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
    public JmsMessage convertToJmsMessage(StompFrame frame) throws JMSException {
        StompJmsMessageFactory messageFactory = connection.getMessageFactory();
        String transformation = frame.getProperty(TRANSFORMATION);
        if (transformation != null) {
            switch (StompJmsMessageFactory.JmsMsgType.valueOf(transformation)) {
                case BYTES:
                    return messageFactory.wrapBytesMessage(frame);
                case TEXT:
                    return messageFactory.wrapTextMessage(frame);
                case TEXT_NULL:
                    return messageFactory.wrapTextMessage(frame);
                case MAP:
                    return messageFactory.wrapMapMessage(frame);
                case OBJECT:
                    return messageFactory.wrapObjectMessage(frame);
                case STREAM:
                    return messageFactory.wrapStreamMessage(frame);
                case MESSAGE:
                    return messageFactory.wrapMessage(frame);
                default:
            }
        }

        transformation = frame.getProperty(CONTENT_TYPE);
        if (transformation != null ) {
            if( transformation.startsWith("text") ||
                transformation.endsWith("json") ||
                transformation.endsWith("xml")) {
                return messageFactory.wrapTextMessage(frame);
            }
        }

        // Standard STOMP contract is that if there is no content-length then the
        // message is a text message.
        transformation = frame.getProperty(CONTENT_LENGTH);
        if (transformation == null) {
            return messageFactory.wrapTextMessage(frame);
        }

        return messageFactory.wrapBytesMessage(frame);
    }

    @Override
    public String getServerVersion() {
        return this.version;
    }

    @Override
    public String getServerName() {
        return "Generic";
    }

    /**
     * Creates a new JmsMessage that wraps the incoming MESSAGE frame.
     */
    public JmsMessage wrapMessage(StompFrame message) {
        return new JmsMessage(new StompJmsMessageFacade(message, connection));
    }

    /**
     * Creates a new JmsTextMessage that wraps the incoming MESSAGE frame.
     */
    public StompJmsTextMessage wrapTextMessage(StompFrame message) {
        return new StompJmsTextMessage(new StompJmsMessageFacade(message, connection));
    }

    /**
     * Creates a new JmsBytesMessage that wraps the incoming MESSAGE frame.
     */
    public StompJmsBytesMessage wrapBytesMessage(StompFrame message) {
        return new StompJmsBytesMessage(new StompJmsMessageFacade(message, connection));
    }

}
