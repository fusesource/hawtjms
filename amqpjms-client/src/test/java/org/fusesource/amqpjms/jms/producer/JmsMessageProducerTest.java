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
package org.fusesource.amqpjms.jms.producer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.fusesource.amqpjms.util.AmqpTestSupport;
import org.junit.Test;

/**
 *
 */
public class JmsMessageProducerTest extends AmqpTestSupport {

    @Test(timeout = 60000)
    public void testCreateMessageProducer() throws Exception {
        Connection connection = createAmqpConnection();
        assertNotNull(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(name.toString());
        session.createProducer(queue);

        QueueViewMBean proxy = getProxyToQueue(name.toString());
        assertEquals(0, proxy.getQueueSize());
        connection.close();
    }

    @Test
    public void testSendWorksWhenConnectionNotStarted() throws Exception {
        Connection connection = createAmqpConnection();
        assertNotNull(connection);

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(name.toString());
        MessageProducer producer = session.createProducer(queue);

        QueueViewMBean proxy = getProxyToQueue(name.toString());
        assertEquals(0, proxy.getQueueSize());

        Message message = session.createMessage();
        producer.send(message);

        assertEquals(1, proxy.getQueueSize());
        connection.close();
    }

    @Test
    public void testSendWorksAfterConnectionStopped() throws Exception {
        Connection connection = createAmqpConnection();
        assertNotNull(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(name.toString());
        MessageProducer producer = session.createProducer(queue);

        QueueViewMBean proxy = getProxyToQueue(name.toString());
        assertEquals(0, proxy.getQueueSize());
        connection.stop();

        Message message = session.createMessage();
        producer.send(message);

        assertEquals(1, proxy.getQueueSize());
        connection.close();
    }

    @Test
    public void testPersistentSendsAreMarkedPersistent() throws Exception {
        Connection connection = createAmqpConnection();
        assertNotNull(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(name.getMethodName());
        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(0, proxy.getQueueSize());

        Message message = session.createMessage();
        producer.send(message);

        assertEquals(1, proxy.getQueueSize());

        MessageConsumer consumer = session.createConsumer(queue);
        message = consumer.receive(5000);
        assertNotNull(message);
        assertTrue(message.getJMSDeliveryMode() == DeliveryMode.PERSISTENT);

        connection.close();
    }
}
