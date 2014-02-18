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
package org.fusesource.amqpjms.jms;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;

import org.fusesource.amqpjms.jms.message.JmsMessageTransformation;
import org.fusesource.amqpjms.jms.meta.JmsProducerId;
import org.fusesource.amqpjms.jms.meta.JmsProducerInfo;

/**
 * Implementation of a Jms MessageProducer
 */
public class JmsMessageProducer implements MessageProducer {

    protected final JmsSession session;
    protected JmsProducerInfo producerMeta;
    protected final boolean flexibleDestination;
    protected int deliveryMode = DeliveryMode.PERSISTENT;
    protected int priority = Message.DEFAULT_PRIORITY;
    protected long timeToLive = Message.DEFAULT_TIME_TO_LIVE;
    protected boolean closed;
    protected boolean disableMessageId;
    protected boolean disableTimestamp;

    protected JmsMessageProducer(JmsProducerId producerId, JmsSession session, JmsDestination destination) {
        this.session = session;
        this.flexibleDestination = destination == null;
        this.producerMeta = new JmsProducerInfo(producerId);
        this.producerMeta.setDestination(destination);
    }

    /**
     * Close the producer
     *
     * @see javax.jms.MessageProducer#close()
     */
    @Override
    public void close() {
        this.closed = true;
        this.session.remove(this);
    }

    /**
     * @return the delivery mode
     * @throws JMSException
     * @see javax.jms.MessageProducer#getDeliveryMode()
     */
    @Override
    public int getDeliveryMode() throws JMSException {
        checkClosed();
        return this.deliveryMode;
    }

    /**
     * @return the destination
     * @throws JMSException
     * @see javax.jms.MessageProducer#getDestination()
     */
    @Override
    public Destination getDestination() throws JMSException {
        checkClosed();
        return this.producerMeta.getDestination();
    }

    /**
     * @return true if disableIds is set
     * @throws JMSException
     * @see javax.jms.MessageProducer#getDisableMessageID()
     */
    @Override
    public boolean getDisableMessageID() throws JMSException {
        checkClosed();
        return this.disableMessageId;
    }

    /**
     * @return true if disable timestamp is set
     * @throws JMSException
     * @see javax.jms.MessageProducer#getDisableMessageTimestamp()
     */
    @Override
    public boolean getDisableMessageTimestamp() throws JMSException {
        checkClosed();
        return this.disableTimestamp;
    }

    /**
     * @return the priority
     * @throws JMSException
     * @see javax.jms.MessageProducer#getPriority()
     */
    @Override
    public int getPriority() throws JMSException {
        checkClosed();
        return this.priority;
    }

    /**
     * @return timeToLive
     * @throws JMSException
     * @see javax.jms.MessageProducer#getTimeToLive()
     */
    @Override
    public long getTimeToLive() throws JMSException {
        checkClosed();
        return this.timeToLive;
    }

    /**
     * @param message
     * @throws JMSException
     * @see javax.jms.MessageProducer#send(javax.jms.Message)
     */
    @Override
    public void send(Message message) throws JMSException {
        send(producerMeta.getDestination(), message, this.deliveryMode, this.priority, this.timeToLive);
    }

    /**
     * @param destination
     * @param message
     * @throws JMSException
     * @see javax.jms.MessageProducer#send(javax.jms.Destination,
     *      javax.jms.Message)
     */
    @Override
    public void send(Destination destination, Message message) throws JMSException {
        send(destination, message, this.deliveryMode, this.priority, this.timeToLive);
    }

    /**
     * @param message
     * @param deliveryMode
     * @param priority
     * @param timeToLive
     * @throws JMSException
     * @see javax.jms.MessageProducer#send(javax.jms.Message, int, int, long)
     */
    @Override
    public void send(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        send(producerMeta.getDestination(), message, deliveryMode, priority, timeToLive);
    }

    /**
     * @param destination
     * @param message
     * @param deliveryMode
     * @param priority
     * @param timeToLive
     * @throws JMSException
     * @see javax.jms.MessageProducer#send(javax.jms.Destination,
     *      javax.jms.Message, int, int, long)
     */
    @Override
    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        if (destination == null) {
            throw new InvalidDestinationException("Don't understand null destinations");
        }
        if (!this.flexibleDestination && !destination.equals(producerMeta.getDestination())) {
            throw new UnsupportedOperationException("This producer can only send messages to: " + producerMeta.getDestination().getName());
        }
        this.session.send(destination, message, deliveryMode, priority, timeToLive, disableMessageId);
    }

    /**
     * @param deliveryMode
     * @throws JMSException
     * @see javax.jms.MessageProducer#setDeliveryMode(int)
     */
    @Override
    public void setDeliveryMode(int deliveryMode) throws JMSException {
        checkClosed();
        this.deliveryMode = deliveryMode;
    }

    /**
     * @param value
     * @throws JMSException
     * @see javax.jms.MessageProducer#setDisableMessageID(boolean)
     */
    @Override
    public void setDisableMessageID(boolean value) throws JMSException {
        checkClosed();
        this.disableMessageId = value;
    }

    /**
     * @param value
     * @throws JMSException
     * @see javax.jms.MessageProducer#setDisableMessageTimestamp(boolean)
     */
    @Override
    public void setDisableMessageTimestamp(boolean value) throws JMSException {
        checkClosed();
        this.disableTimestamp = value;
    }

    /**
     * @param defaultPriority
     * @throws JMSException
     * @see javax.jms.MessageProducer#setPriority(int)
     */
    @Override
    public void setPriority(int defaultPriority) throws JMSException {
        checkClosed();
        this.priority = defaultPriority;
    }

    /**
     * @param timeToLive
     * @throws JMSException
     * @see javax.jms.MessageProducer#setTimeToLive(long)
     */
    @Override
    public void setTimeToLive(long timeToLive) throws JMSException {
        checkClosed();
        this.timeToLive = timeToLive;
    }

    /**
     * @param destination
     *        the destination to set
     * @throws JMSException
     * @throws InvalidDestinationException
     */
    public void setDestination(Destination destination) throws JMSException {
        if (destination == null) {
            throw new InvalidDestinationException("Don't understand null destinations");
        }
        if (!this.flexibleDestination && !destination.equals(producerMeta.getDestination())) {
            throw new UnsupportedOperationException("This producer can only send messages to: " + producerMeta.getDestination().getName());
        }
        producerMeta.setDestination(JmsMessageTransformation.transformDestination(session.getConnection(), destination));
    }

    protected void checkClosed() throws IllegalStateException {
        if (this.closed) {
            throw new IllegalStateException("The MessageProducer is closed");
        }
    }
}
