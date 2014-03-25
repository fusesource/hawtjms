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
package org.fusesource.amqpjms.jms.transactions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.fusesource.amqpjms.util.AmqpTestSupport;
import org.junit.After;
import org.junit.Test;

/**
 * Test consumer behavior for Transacted Session Consumers.
 */
public class JmsTransactedConsumerTest extends AmqpTestSupport {

    private Connection connection;

    @Override
    @After
    public void tearDown() throws Exception {
        connection.close();
        super.tearDown();
    }

    @Test
    public void testCreateConsumerFromTxSession() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        assertNotNull(session);
        assertTrue(session.getTransacted());

        Queue queue = session.createQueue(name.getMethodName());
        MessageConsumer consumer = session.createConsumer(queue);
        assertNotNull(consumer);
    }

    @Test
    public void testConsumedInTxAreAcked() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        sendToAmqQueue(1);

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = session.createQueue(name.getMethodName());
        MessageConsumer consumer = session.createConsumer(queue);

        Message message = consumer.receive(5000);
        assertNotNull(message);

        QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(1, proxy.getQueueSize());

        session.commit();

        assertEquals(0, proxy.getQueueSize());
    }
}
