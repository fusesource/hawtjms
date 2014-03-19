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
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.fusesource.amqpjms.jms.JmsConnection;
import org.fusesource.amqpjms.jms.JmsConnectionFactory;
import org.fusesource.amqpjms.util.AmqpTestSupport;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test for messages produced inside a local transaction.
 */
public class JmsTransactedProducerTest extends AmqpTestSupport {

    @Test(timeout = 60000)
    public void testCreateTxSessionAndProducer() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getBrokerAmqpConnectionURI());
        JmsConnection connection = (JmsConnection) factory.createConnection();
        assertNotNull(connection);
        connection.start();

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        assertNotNull(session);
        assertTrue(session.getTransacted());

        Queue queue = session.createQueue(name.getMethodName());
        MessageProducer producer = session.createProducer(queue);
        assertNotNull(producer);

        connection.close();
    }

    @Test(timeout = 60000)
    public void testTXProducerCommitsAreQueued() throws Exception {
        final int MSG_COUNT = 10;
        Connection connection = createAmqpConnection();
        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = session.createQueue(name.getMethodName());
        MessageProducer producer = session.createProducer(queue);

        for (int i = 0; i < MSG_COUNT; ++i) {
            producer.send(session.createTextMessage());
        }

        QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        session.commit();
        assertEquals(MSG_COUNT, proxy.getQueueSize());
        connection.close();
    }

    @Ignore
    @Test(timeout = 60000)
    public void testTXProducerRollbacksNotQueued() throws Exception {
        final int MSG_COUNT = 10;
        Connection connection = createAmqpConnection();
        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = session.createQueue(name.getMethodName());
        MessageProducer producer = session.createProducer(queue);

        for (int i = 0; i < MSG_COUNT; ++i) {
            producer.send(session.createTextMessage());
        }

        QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        session.rollback();
        assertEquals(0, proxy.getQueueSize());
        connection.close();
    }

}
