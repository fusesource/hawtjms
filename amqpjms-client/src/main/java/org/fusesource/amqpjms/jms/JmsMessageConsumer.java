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

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.fusesource.amqpjms.jms.exceptions.JmsExceptionSupport;
import org.fusesource.amqpjms.jms.message.JmsInboundMessageDispatch;
import org.fusesource.amqpjms.jms.message.JmsMessage;
import org.fusesource.amqpjms.jms.meta.JmsConsumerId;
import org.fusesource.amqpjms.jms.meta.JmsConsumerInfo;
import org.fusesource.amqpjms.jms.util.MessageQueue;

/**
 * implementation of a JMS Message Consumer
 */
public class JmsMessageConsumer implements MessageConsumer, JmsMessageListener, JmsMessageDispatcher {

    protected final JmsSession session;
    protected final JmsConnection connection;
    protected JmsConsumerInfo consumerInfo;
    protected final int acknowledgementMode;
    protected final AtomicBoolean closed = new AtomicBoolean();
    protected boolean started;
    protected MessageListener messageListener;
    protected final MessageQueue messageQueue;
    protected final Lock lock = new ReentrantLock();
    protected final AtomicBoolean suspendedConnection = new AtomicBoolean();

    protected JmsMessageConsumer(JmsConsumerId consumerId, JmsSession session, JmsDestination destination, String selector) throws JMSException {
        this.session = session;
        this.connection = session.getConnection();
        this.acknowledgementMode = session.acknowledgementMode();

        if (acknowledgementMode == Session.SESSION_TRANSACTED) {
            throw new UnsupportedOperationException();
            //this.messageQueue = new TxMessageQueue(session.consumerMessageBufferSize);
        } else {
            this.messageQueue = new MessageQueue(session.getConsumerMessageBufferSize());
        }

        this.consumerInfo = new JmsConsumerInfo(consumerId);
        this.consumerInfo.setSelector(selector);
        this.consumerInfo.setDestination(destination);

        // We are ready for dispatching
        this.session.add(this);

        try {
            this.consumerInfo = session.getConnection().createResource(consumerInfo);
        } catch (JMSException ex) {
            this.session.remove(this);
            throw ex;
        }
    }

    public void init() throws JMSException {
        session.add(this);
    }

    public boolean isDurableSubscription() {
        return false;
    }

    public boolean isBrowser() {
        return false;
    }

    /**
     * @throws JMSException
     * @see javax.jms.MessageConsumer#close()
     */
    @Override
    public void close() throws JMSException {
        if (closed.compareAndSet(false, true)) {
            this.session.remove(this);
            this.connection.destroyResource(consumerInfo);
        }
    }

    @Override
    public MessageListener getMessageListener() throws JMSException {
        checkClosed();
        return this.messageListener;
    }

    /**
     * @return the Message Selector
     * @throws JMSException
     * @see javax.jms.MessageConsumer#getMessageSelector()
     */
    @Override
    public String getMessageSelector() throws JMSException {
        checkClosed();
        return this.consumerInfo.getSelector();
    }

    /**
     * @return a Message or null if closed during the operation
     * @throws JMSException
     * @see javax.jms.MessageConsumer#receive()
     */
    @Override
    public Message receive() throws JMSException {
        checkClosed();
        checkMessageListener();

        try {
            return copy(ack(this.messageQueue.dequeue(-1)));
        } catch (Exception e) {
            throw JmsExceptionSupport.create(e);
        }
    }

    /**
     * @param timeout
     * @return a MEssage or null
     * @throws JMSException
     * @see javax.jms.MessageConsumer#receive(long)
     */
    @Override
    public Message receive(long timeout) throws JMSException {
        checkClosed();
        checkMessageListener();

        try {
            return copy(ack(this.messageQueue.dequeue(timeout)));
        } catch (InterruptedException e) {
            throw JmsExceptionSupport.create(e);
        }
    }

    /**
     * @return a Message or null
     * @throws JMSException
     * @see javax.jms.MessageConsumer#receiveNoWait()
     */
    @Override
    public Message receiveNoWait() throws JMSException {
        checkClosed();
        checkMessageListener();

        Message result = copy(ack(this.messageQueue.dequeueNoWait()));
        return result;
    }

    /**
     * @param listener
     * @throws JMSException
     * @see javax.jms.MessageConsumer#setMessageListener(javax.jms.MessageListener)
     */
    @Override
    public void setMessageListener(MessageListener listener) throws JMSException {
        checkClosed();
        this.messageListener = listener;
        drainMessageQueueToListener();
    }

    protected void checkClosed() throws IllegalStateException {
        if (this.closed.get()) {
            throw new IllegalStateException("The MessageProducer is closed");
        }
    }

    JmsMessage copy(final JmsInboundMessageDispatch envelope) throws JMSException {
        if (envelope == null) {
            return null;
        }
        return envelope.getMessage().copy();
    }

    JmsInboundMessageDispatch ack(final JmsInboundMessageDispatch envelope) {
        if (envelope != null) {
            JmsMessage message = envelope.getMessage();
            if (message.getAcknowledgeCallback() != null) {
                // Message has been received by the app.. expand the credit
                // window so that we receive more messages.
//                StompFrame frame = session.channel.serverAdaptor.createCreditFrame(this, message.getFrame());
//                if (frame != null) {
//                    try {
//                        session.channel.sendFrame(frame);
//                    } catch (IOException ignore) {
//                    }
//                }
                // don't actually ack yet.. client code does it.
                return envelope;
            }
            doAck(message);
        }
        return envelope;
    }

    private void doAck(final JmsMessage message) {
//            try {
//                StompChannel channel = session.channel;
//                if (channel == null) {
//                    throw new JMSException("Consumer closed");
//                }
//
//                final Promise<StompFrame> ack = new Promise<StompFrame>();
//                switch (session.acknowledgementMode) {
//                    case Session.CLIENT_ACKNOWLEDGE:
//                        channel.ackMessage(id, message.getMessageID(), null, ack);
//                        break;
//                    case Session.AUTO_ACKNOWLEDGE:
//                        channel.ackMessage(id, message.getMessageID(), null, ack);
//                        break;
//                    case Session.DUPS_OK_ACKNOWLEDGE:
//                        channel.ackMessage(id, message.getMessageID(), null, null);
//                        ack.onSuccess(null);
//                        break;
//                    case Session.SESSION_TRANSACTED:
//                        channel.ackMessage(id, message.getMessageID(), session.currentTransactionId, null);
//                        ack.onSuccess(null);
//                        break;
//                    case JmsSession.SERVER_AUTO_ACKNOWLEDGE:
//                        throw new IllegalStateException("This should never get called.");
//                }
//                ack.await();
//
//            } catch (JMSException e) {
//                session.connection.onException(e);
//                throw new RuntimeException(e);
//            } catch (Exception e) {
//                session.connection.onException(new JMSException("Exception occurred sending ACK for message id : " + message.getMessageID()));
//                throw new RuntimeException("Exception occurred sending ACK for message id : " + message.getMessageID(), e);
//            }
    }

    /**
     * @param message
     */
    @Override
    public void onMessage(final JmsInboundMessageDispatch envelope) {
        lock.lock();
        try {
            if (acknowledgementMode == Session.CLIENT_ACKNOWLEDGE) {
                envelope.getMessage().setAcknowledgeCallback(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        if (session.isClosed()) {
                            throw new javax.jms.IllegalStateException("Session closed.");
                        }
                        doAck(envelope.getMessage());
                        return null;
                    }
                });
            }
            this.messageQueue.enqueue(envelope);
        } finally {
            lock.unlock();
        }

        if (this.messageListener != null && this.started) {
            session.getExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    JmsInboundMessageDispatch envelope;
                    while (session.isStarted() && (envelope = messageQueue.dequeueNoWait()) != null) {
                        try {
                            messageListener.onMessage(copy(ack(envelope)));
                        } catch (Exception e) {
                            session.getConnection().onException(e);
                        }
                    }
                }
            });
        }
    }

    /**
     * @return the id
     */
    public JmsConsumerId getConsumerId() {
        // TODO should we check closed?
        return this.consumerInfo.getConsumerId();
    }

    /**
     * @return the Destination
     */
    public JmsDestination getDestination() {
        // TODO should we check closed?
        return this.consumerInfo.getDestination();
    }

    public void start() {
        lock.lock();
        try {
            this.started = true;
            this.messageQueue.start();
            drainMessageQueueToListener();
        } finally {
            lock.unlock();
        }
    }

    public void stop() {
        lock.lock();
        try {
            this.started = false;
            this.messageQueue.stop();
        } finally {
            lock.unlock();
        }
    }

    void rollback() {
        // ((TxMessageQueue) this.messageQueue).rollback();
        throw new UnsupportedOperationException();
    }

    void commit() {
        // ((TxMessageQueue) this.messageQueue).commit();
        throw new UnsupportedOperationException();
    }

    void drainMessageQueueToListener() {
        MessageListener listener = this.messageListener;
        if (listener != null) {
            if (!this.messageQueue.isEmpty()) {
                List<JmsInboundMessageDispatch> drain = this.messageQueue.removeAll();
                for (JmsInboundMessageDispatch envelope : drain) {
                    try {
                        listener.onMessage(copy(ack(envelope)));
                    } catch (Exception e) {
                        session.getConnection().onException(e);
                    }
                }
                drain.clear();
            }
        }
    }

    protected void checkMessageListener() throws JMSException {
        session.checkMessageListener();
    }

    boolean hasMessageListener() {
        return this.messageListener != null;
    }

    boolean isUsingDestination(JmsDestination destination) {
        return this.consumerInfo.getDestination().equals(destination);
    }

    protected int getMessageQueueSize() {
        return this.messageQueue.size();
    }

    public boolean getNoLocal() throws IllegalStateException {
        return false;
    }
}
