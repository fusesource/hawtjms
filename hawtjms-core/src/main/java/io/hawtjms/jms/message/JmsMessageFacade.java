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
package io.hawtjms.jms.message;

import io.hawtjms.jms.JmsDestination;
import io.hawtjms.jms.meta.JmsMessageId;
import io.hawtjms.jms.meta.JmsTransactionId;

import java.io.IOException;
import java.util.Map;

import javax.jms.JMSException;

/**
 * The Message Proxy interface defines the required mapping between a Provider's
 * own Message type and the JMS Message types.  A Provider can implement the Proxy
 * interface and offer direct access to its message types without the need to
 * copy to / from a more generic JMS message instance.
 */
public interface JmsMessageFacade {

    /**
     * Returns the Message properties contained within this Message instance.
     *
     * @return a Map containing the properties of this Message.
     *
     * @throws IOException if an error occurs while accessing the Message properties.
     */
    public Map<String, Object> getProperties() throws IOException;

    /**
     * Called when a message is sent to allow a Message instance to move the
     * contents from a logical data structure to a binary form for transmission.
     */
    void storeContent() throws JMSException;

    /**
     * Clears the contents of this Message.
     */
    void clearBody();

    /**
     * Clears any Message properties that exist for this Message instance.
     */
    void clearProperties();

    /**
     * Create a new instance and perform a deep copy of this object's
     * contents.
     */
    JmsMessageFacade copy();

    /**
     * @return the transactionId
     */
    JmsTransactionId getTransactionId();

    /**
     * @param transactionId
     *        the transactionId to set
     */
    void setTransactionId(JmsTransactionId transactionId);

    JmsMessageId getMessageId();

    void setMessageId(JmsMessageId messageId);

    long getTimestamp();

    void setTimestamp(long timestamp);

    String getCorrelationId();

    void setCorrelationId(String correlationId);

    boolean isPersistent();

    void setPersistent(boolean value);

    int getRedeliveryCounter();

    void setRedeliveryCounter(int redeliveryCount);

    String getType();

    void setType(String type);

    byte getPriority();

    void setPriority(byte priority);

    long getExpiration();

    void setExpiration(long expiration);

    JmsDestination getDestination() throws JMSException;

    void setDestination(JmsDestination destination);

    JmsDestination getReplyTo() throws JMSException;

    void setReplyTo(JmsDestination replyTo);

    String getUserId();

    void setUserId(String userId);

    String getGroupId();

    void setGroupId(String groupId);

    int getGroupSequence();

    void setGroupSequence(int groupSequence);

}
