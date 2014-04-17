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

import java.io.Serializable;

/**
 * Interface that a Provider should implement to provide a Provider
 * Specific JmsMessage implementation that optimizes the exchange of
 * message properties and payload between the JMS Message API and the
 * underlying Provider Message implementations.
 */
public interface JmsMessageFactory {

    /**
     * Creates an instance of a basic JmsMessage object.  The provider may
     * either create the Message with the default generic internal message
     * implementation or create a Provider specific instance that optimizes
     * the access and marshaling of the message.
     *
     * @return a newly created and initialized JmsMessage instance.
     *
     * @throws UnsupportedOperationException if the provider can't send empty Message types.
     */
    JmsMessage createMessage() throws UnsupportedOperationException;

    /**
     * Creates an instance of a basic JmsTextMessage object.  The provider may
     * either create the Message with the default generic internal message
     * implementation or create a Provider specific instance that optimizes
     * the access and marshaling of the message.
     *
     * @param payload
     *        The value to initially assign to the Message body, or null if empty to start.
     *
     * @returns a newly created and initialized JmsTextMessage instance.
     *
     * @throws UnsupportedOperationException if the provider can't send TextMessage types.
     */
    JmsTextMessage createTextMessage(String payload) throws UnsupportedOperationException;

    /**
     * Creates an instance of a basic JmsTextMessage object.  The provider may
     * either create the Message with the default generic internal message
     * implementation or create a Provider specific instance that optimizes
     * the access and marshaling of the message.
     *
     * @returns a newly created and initialized JmsTextMessage instance.
     *
     * @throws UnsupportedOperationException if the provider can't send TextMessage types.
     */
    JmsTextMessage createTextMessage() throws UnsupportedOperationException;

    /**
     * Creates an instance of a basic JmsBytesMessage object.  The provider may
     * either create the Message with the default generic internal message
     * implementation or create a Provider specific instance that optimizes
     * the access and marshaling of the message.
     *
     * @returns a newly created and initialized JmsTextMessage instance.
     *
     * @throws UnsupportedOperationException if the provider can't send BytesMessage types.
     */
    JmsBytesMessage createBytesMessage() throws UnsupportedOperationException;

    /**
     * Creates an instance of a basic JmsMapMessage object.  The provider may
     * either create the Message with the default generic internal message
     * implementation or create a Provider specific instance that optimizes
     * the access and marshaling of the message.
     *
     * @returns a newly created and initialized JmsTextMessage instance.
     *
     * @throws UnsupportedOperationException if the provider can't send MapMessage types.
     */
    JmsMapMessage createMapMessage() throws UnsupportedOperationException;

    /**
     * Creates an instance of a basic JmsStreamMessage object.  The provider may
     * either create the Message with the default generic internal message
     * implementation or create a Provider specific instance that optimizes
     * the access and marshaling of the message.
     *
     * @returns a newly created and initialized JmsTextMessage instance.
     *
     * @throws UnsupportedOperationException if the provider can't send StreamMessage types.
     */
    JmsStreamMessage createStreamMessage() throws UnsupportedOperationException;

    /**
     * Creates an instance of a basic JmsObjectMessage object.  The provider may
     * either create the Message with the default generic internal message
     * implementation or create a Provider specific instance that optimizes
     * the access and marshaling of the message.
     *
     * @param payload
     *        The value to initially assign to the Message body, or null if empty to start.
     *
     * @returns a newly created and initialized JmsTextMessage instance.
     *
     * @throws UnsupportedOperationException if the provider can't send ObjectMessage types.
     */
    JmsObjectMessage createObjectMessage(Serializable payload) throws UnsupportedOperationException;

    /**
     * Creates an instance of a basic JmsObjectMessage object.  The provider may
     * either create the Message with the default generic internal message
     * implementation or create a Provider specific instance that optimizes
     * the access and marshaling of the message.
     *
     * @returns a newly created and initialized JmsTextMessage instance.
     *
     * @throws UnsupportedOperationException if the provider can't send ObjectMessage types.
     */
    JmsObjectMessage createObjectMessage() throws UnsupportedOperationException;

}
