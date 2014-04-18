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
package io.hawtjms.provider.stomp.message;

import static io.hawtjms.provider.stomp.StompConstants.MESSAGE;
import io.hawtjms.jms.message.JmsBytesMessage;
import io.hawtjms.jms.message.JmsMapMessage;
import io.hawtjms.jms.message.JmsMessage;
import io.hawtjms.jms.message.JmsMessageFactory;
import io.hawtjms.jms.message.JmsObjectMessage;
import io.hawtjms.jms.message.JmsStreamMessage;
import io.hawtjms.jms.message.JmsTextMessage;
import io.hawtjms.provider.stomp.StompConnection;
import io.hawtjms.provider.stomp.StompFrame;

import java.io.Serializable;

import javax.jms.MessageNotWriteableException;

/**
 * STOMP based Message Factory.
 */
public class StompJmsMessageFactory implements JmsMessageFactory {

    private StompConnection connection;

    public StompJmsMessageFactory() {
    }

    public StompJmsMessageFactory(StompConnection connection) {
        this.connection = connection;
    }

    public StompConnection getStompConnection() {
        return this.connection;
    }

    public void setStompConnection(StompConnection connection) {
        this.connection = connection;
    }

    @Override
    public JmsMessage createMessage() throws UnsupportedOperationException {
        return new JmsMessage(new StompJmsMessageFacade(new StompFrame(MESSAGE), connection));
    }

    @Override
    public JmsTextMessage createTextMessage(String payload) throws UnsupportedOperationException {
        StompJmsTextMessage message = new StompJmsTextMessage(new StompJmsMessageFacade(new StompFrame(MESSAGE), connection));
        if (payload != null) {
            try {
                message.setText(payload);
            } catch (MessageNotWriteableException e) {
            }
        }
        return message;
    }

    @Override
    public JmsTextMessage createTextMessage() throws UnsupportedOperationException {
        return createTextMessage(null);
    }

    @Override
    public JmsBytesMessage createBytesMessage() throws UnsupportedOperationException {
        return new StompJmsBytesMessage(new StompJmsMessageFacade(new StompFrame(MESSAGE), connection));
    }

    @Override
    public JmsMapMessage createMapMessage() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("STOMP Provider does not currently support MapMessage");
    }

    @Override
    public JmsStreamMessage createStreamMessage() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("STOMP Provider does not currently support StreamMessage");
    }

    @Override
    public JmsObjectMessage createObjectMessage(Serializable payload) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("STOMP Provider does not currently support ObjectMessage");
    }

    @Override
    public JmsObjectMessage createObjectMessage() throws UnsupportedOperationException {
        return createObjectMessage(null);
    }
}
