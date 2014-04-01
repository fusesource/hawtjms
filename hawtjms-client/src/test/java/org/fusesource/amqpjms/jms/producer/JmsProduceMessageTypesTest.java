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

import javax.jms.BytesMessage;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.fusesource.amqpjms.jms.JmsConnection;
import org.fusesource.amqpjms.jms.JmsConnectionFactory;
import org.fusesource.amqpjms.util.AmqpTestSupport;
import org.junit.Test;

/**
 * Test basic MessageProducer functionality.
 */
public class JmsProduceMessageTypesTest extends AmqpTestSupport {

    @Test(timeout = 60000)
    public void testSendJMSMessage() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getBrokerAmqpConnectionURI());
        JmsConnection connection = (JmsConnection) factory.createConnection();
        assertNotNull(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue("test.queue");
        MessageProducer producer = session.createProducer(queue);
        Message message = session.createMessage();
        producer.send(message);
        QueueViewMBean proxy = getProxyToQueue("test.queue");
        assertEquals(1, proxy.getQueueSize());

        connection.close();
    }

    @Test(timeout = 60000)
    public void testSendJMSBytesMessage() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getBrokerAmqpConnectionURI());
        JmsConnection connection = (JmsConnection) factory.createConnection();
        assertNotNull(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue("test.queue");
        MessageProducer producer = session.createProducer(queue);
        BytesMessage message = session.createBytesMessage();
        message.writeUTF("TEST");
        producer.send(message);
        QueueViewMBean proxy = getProxyToQueue("test.queue");
        assertEquals(1, proxy.getQueueSize());

        connection.close();
    }

    @Test(timeout = 60000)
    public void testSendJMSMapMessage() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getBrokerAmqpConnectionURI());
        JmsConnection connection = (JmsConnection) factory.createConnection();
        assertNotNull(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue("test.queue");
        MessageProducer producer = session.createProducer(queue);
        MapMessage message = session.createMapMessage();
        message.setBoolean("Boolean", false);
        message.setString("STRING", "TEST");
        producer.send(message);
        QueueViewMBean proxy = getProxyToQueue("test.queue");
        assertEquals(1, proxy.getQueueSize());

        connection.close();
    }

    @Test(timeout = 60000)
    public void testSendJMSStreamMessage() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getBrokerAmqpConnectionURI());
        JmsConnection connection = (JmsConnection) factory.createConnection();
        assertNotNull(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue("test.queue");
        MessageProducer producer = session.createProducer(queue);
        StreamMessage message = session.createStreamMessage();
        message.writeString("TEST");
        producer.send(message);
        QueueViewMBean proxy = getProxyToQueue("test.queue");
        assertEquals(1, proxy.getQueueSize());

        connection.close();
    }

    @Test(timeout = 60000)
    public void testSendJMSTextMessage() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getBrokerAmqpConnectionURI());
        JmsConnection connection = (JmsConnection) factory.createConnection();
        assertNotNull(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue("test.queue");
        MessageProducer producer = session.createProducer(queue);
        TextMessage message = session.createTextMessage("TEST");
        producer.send(message);
        QueueViewMBean proxy = getProxyToQueue("test.queue");
        assertEquals(1, proxy.getQueueSize());

        connection.close();
    }

    @Test(timeout = 60000)
    public void testSendJMSObjectMessage() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getBrokerAmqpConnectionURI());
        JmsConnection connection = (JmsConnection) factory.createConnection();
        assertNotNull(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue("test.queue");
        MessageProducer producer = session.createProducer(queue);
        ObjectMessage message = session.createObjectMessage("TEST");
        producer.send(message);
        QueueViewMBean proxy = getProxyToQueue("test.queue");
        assertEquals(1, proxy.getQueueSize());

        connection.close();
    }
}
