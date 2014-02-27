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
package org.fusesource.amqpjms;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.fusesource.amqpjms.jms.JmsConnection;
import org.fusesource.amqpjms.jms.JmsConnectionFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Test basic Session functionality.
 */
public class JmsSessionTest extends AmqpTestSupport {

    @Rule public TestName name = new TestName();

    @Test(timeout = 60000)
    public void testCreateSession() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getBrokerAmqpConnectionURI());
        JmsConnection connection = (JmsConnection) factory.createConnection();
        assertNotNull(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);

        session.close();
        connection.close();
    }

    @Test(timeout=30000)
    public void testSessionCreateProducer() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getBrokerAmqpConnectionURI());
        JmsConnection connection = (JmsConnection) factory.createConnection();
        assertNotNull(connection);
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);

        Queue queue = session.createQueue("test.queue");
        MessageProducer producer = session.createProducer(queue);

        producer.close();
        session.close();
        connection.close();
    }

    @Test(timeout=30000)
    public void testSessionCreateConsumer() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getBrokerAmqpConnectionURI());
        JmsConnection connection = (JmsConnection) factory.createConnection();
        assertNotNull(connection);
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);

        Queue queue = session.createQueue("test.queue");
        MessageConsumer consumer = session.createConsumer(queue);

        consumer.close();
        session.close();
        connection.close();
    }

    @Test(timeout=30000)
    public void testSessionDoubleCloseWithoutException() throws Exception {
        Connection connection = createAmqpConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.close();
        session.close();
        connection.close();
    }

    @Test(timeout=30000)
    public void testSessionCloseWontThrowWhenNotConnected() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        Connection connection = createAmqpConnection();
        connection.setExceptionListener(new ExceptionListener() {

            @Override
            public void onException(JMSException exception) {
                latch.countDown();
            }
        });
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        stopBroker();
        assertTrue(latch.await(10, TimeUnit.SECONDS));

        session.close();
        connection.close();
    }

    @Test(timeout=30000)
    public void testCreateConsumerThrowsWhenNotConnected() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        Connection connection = createAmqpConnection();
        connection.setExceptionListener(new ExceptionListener() {

            @Override
            public void onException(JMSException exception) {
                latch.countDown();
            }
        });
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(name.getMethodName());

        stopBroker();
        assertTrue(latch.await(10, TimeUnit.SECONDS));

        try {
            session.createConsumer(destination);
            fail("Should have thrown once connection failed.");
        } catch (JMSException ex) {
            // Expected
        }

        connection.close();
    }

    @Test(timeout=30000)
    public void testCreateProducerThrowsWhenNotConnected() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        Connection connection = createAmqpConnection();
        connection.setExceptionListener(new ExceptionListener() {

            @Override
            public void onException(JMSException exception) {
                latch.countDown();
            }
        });
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(name.getMethodName());

        stopBroker();
        assertTrue(latch.await(10, TimeUnit.SECONDS));

        try {
            session.createProducer(destination);
            fail("Should have thrown once connection failed.");
        } catch (JMSException ex) {
            // Expected
        }

        connection.close();
    }

    @Test(timeout=30000)
    public void testCreateTextMessageThrowsWhenNotConnected() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        Connection connection = createAmqpConnection();
        connection.setExceptionListener(new ExceptionListener() {

            @Override
            public void onException(JMSException exception) {
                latch.countDown();
            }
        });
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        stopBroker();
        assertTrue(latch.await(10, TimeUnit.SECONDS));

        try {
            session.createTextMessage();
            fail("Should have thrown once connection failed.");
        } catch (JMSException ex) {
            // Expected
        }

        connection.close();
    }
}
