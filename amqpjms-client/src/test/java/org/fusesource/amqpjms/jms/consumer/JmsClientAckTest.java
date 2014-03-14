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
package org.fusesource.amqpjms.jms.consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.fusesource.amqpjms.util.AmqpTestSupport;
import org.fusesource.amqpjms.util.Wait;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that Session CLIENT_ACKNOWLEDGE works as expected.
 */
public class JmsClientAckTest extends AmqpTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(JmsClientAckTest.class);

    @Test
    public void testAckedMessageAreConsumed() throws Exception {
        Connection connection = createAmqpConnection();
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Hello"));

        final QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(1, proxy.getQueueSize());

        // Consume the message...
        MessageConsumer consumer = session.createConsumer(queue);
        Message msg = consumer.receive(1000);
        assertNotNull(msg);
        msg.acknowledge();

        assertTrue("Queued message not consumed.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return proxy.getQueueSize() == 0;
            }
        }));

        connection.close();
    }

    @Test
    public void testLastMessageAcked() throws Exception {
        Connection connection = createAmqpConnection();
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Hello"));
        producer.send(session.createTextMessage("Hello2"));
        producer.send(session.createTextMessage("Hello3"));

        final QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(3, proxy.getQueueSize());

        // Consume the message...
        MessageConsumer consumer = session.createConsumer(queue);
        Message msg = consumer.receive(1000);
        assertNotNull(msg);
        msg = consumer.receive(1000);
        assertNotNull(msg);
        msg = consumer.receive(1000);
        assertNotNull(msg);
        msg.acknowledge();

        assertTrue("Queued message not consumed.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return proxy.getQueueSize() == 0;
            }
        }));

        connection.close();
    }

    @Test
    public void testUnAckedMessageAreNotConsumedOnSessionClose() throws Exception {
        Connection connection = createAmqpConnection();
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Hello"));

        final QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(1, proxy.getQueueSize());

        // Consume the message...but don't ack it.
        MessageConsumer consumer = session.createConsumer(queue);
        Message msg = consumer.receive(1000);
        assertNotNull(msg);
        session.close();

        assertEquals(1, proxy.getQueueSize());

        // Consume the message...and this time we ack it.
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        consumer = session.createConsumer(queue);
        msg = consumer.receive(2000);
        assertNotNull(msg);
        msg.acknowledge();

        assertTrue("Queued message not consumed.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return proxy.getQueueSize() == 0;
            }
        }));

        connection.close();
    }

    @Test
    public void testAckedMessageAreConsumedByAsync() throws Exception {
        Connection connection = createAmqpConnection();
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Hello"));

        final QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(1, proxy.getQueueSize());

        // Consume the message...
        MessageConsumer consumer = session.createConsumer(queue);
        consumer.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
                try {
                    message.acknowledge();
                } catch (JMSException e) {
                    LOG.warn("Unexpected exception on acknowledge: {}", e.getMessage());
                }
            }
        });

        assertTrue("Queued message not consumed.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return proxy.getQueueSize() == 0;
            }
        }));

        connection.close();
    }

    @Test
    public void testUnAckedAsyncMessageAreNotConsumedOnSessionClose() throws Exception {
        Connection connection = createAmqpConnection();
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Hello"));

        final QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(1, proxy.getQueueSize());

        // Consume the message...
        MessageConsumer consumer = session.createConsumer(queue);
        consumer.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
                // Don't ack the message.
            }
        });

        session.close();
        assertEquals(1, proxy.getQueueSize());

        // Now we consume and ack the Message.
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        consumer = session.createConsumer(queue);
        Message msg = consumer.receive(2000);
        assertNotNull(msg);
        msg.acknowledge();

        assertTrue("Queued message not consumed.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return proxy.getQueueSize() == 0;
            }
        }));

        connection.close();
    }

    @Test(timeout=90000)
    public void testAckMarksAllConsumerMessageAsConsumed() throws Exception {
        Connection connection = createAmqpConnection();
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());

        final int MSG_COUNT = 30;
        final AtomicReference<Message> lastMessage = new AtomicReference<Message>();
        final CountDownLatch done = new CountDownLatch(MSG_COUNT);

        MessageListener myListener = new MessageListener() {

            @Override
            public void onMessage(Message message) {
                lastMessage.set(message);
                done.countDown();
            }
        };

        MessageConsumer consumer1 = session.createConsumer(queue);
        consumer1.setMessageListener(myListener);
        MessageConsumer consumer2 = session.createConsumer(queue);
        consumer2.setMessageListener(myListener);
        MessageConsumer consumer3 = session.createConsumer(queue);
        consumer3.setMessageListener(myListener);

        MessageProducer producer = session.createProducer(queue);
        for (int i = 0; i < MSG_COUNT; ++i) {
            producer.send(session.createTextMessage("Hello: " + i));
        }

        final QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(MSG_COUNT, proxy.getQueueSize());

        assertTrue("Failed to consume all messages.", done.await(20, TimeUnit.SECONDS));
        assertNotNull(lastMessage.get());
        assertEquals(MSG_COUNT, proxy.getInFlightCount());

        lastMessage.get().acknowledge();

        assertTrue("Queued message not consumed.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return proxy.getQueueSize() == 0;
            }
        }));

        connection.close();
    }
}
