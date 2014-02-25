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
package org.fusesource.amqpjms.provider.amqp;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.qpid.proton.jms.JMSVendor;
import org.fusesource.amqpjms.jms.JmsDestination;
import org.fusesource.amqpjms.jms.JmsQueue;
import org.fusesource.amqpjms.jms.JmsTemporaryQueue;
import org.fusesource.amqpjms.jms.JmsTemporaryTopic;
import org.fusesource.amqpjms.jms.JmsTopic;
import org.fusesource.amqpjms.jms.message.JmsBytesMessage;
import org.fusesource.amqpjms.jms.message.JmsMapMessage;
import org.fusesource.amqpjms.jms.message.JmsMessage;
import org.fusesource.amqpjms.jms.message.JmsObjectMessage;
import org.fusesource.amqpjms.jms.message.JmsStreamMessage;
import org.fusesource.amqpjms.jms.message.JmsTextMessage;

public class AmqpJMSVendor extends JMSVendor {

    final public static AmqpJMSVendor INSTANCE = new AmqpJMSVendor();

    private AmqpJMSVendor() {
    }

    @Override
    public BytesMessage createBytesMessage() {
        return new JmsBytesMessage();
    }

    @Override
    public StreamMessage createStreamMessage() {
        return new JmsStreamMessage();
    }

    @Override
    public Message createMessage() {
        return new JmsMessage();
    }

    @Override
    public TextMessage createTextMessage() {
        return new JmsTextMessage();
    }

    @Override
    public ObjectMessage createObjectMessage() {
        return new JmsObjectMessage();
    }

    @Override
    public MapMessage createMapMessage() {
        return new JmsMapMessage();
    }

    @Override
    public Destination createDestination(String name) {
        return super.createDestination(name, Destination.class);
    }

    @Override
    public <T extends Destination> T createDestination(String name, Class<T> kind) {
        if (kind == Queue.class) {
            return kind.cast(new JmsQueue(name));
        }
        if (kind == Topic.class) {
            return kind.cast(new JmsTopic(name));
        }
        if (kind == TemporaryQueue.class) {
            return kind.cast(new JmsTemporaryQueue(name));
        }
        if (kind == TemporaryTopic.class) {
            return kind.cast(new JmsTemporaryTopic(name));
        }

        return kind.cast(new JmsQueue(name));
    }

    @Override
    public void setJMSXUserID(Message msg, String value) {
        ((JmsMessage) msg).setUserId(value);
    }

    @Override
    public void setJMSXGroupID(Message msg, String value) {
        ((JmsMessage) msg).setGroupId(value);
    }

    @Override
    public void setJMSXGroupSequence(Message msg, int value) {
        ((JmsMessage) msg).setGroupSequence(value);
    }

    @Override
    public void setJMSXDeliveryCount(Message msg, long value) {
        ((JmsMessage) msg).setRedeliveryCounter((int) value);
    }

    @Override
    public String toAddress(Destination dest) {
        return ((JmsDestination) dest).getName();
    }
}
