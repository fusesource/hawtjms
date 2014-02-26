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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.junit.Test;

/**
 *
 */
public class JmsMessageConsumerTest extends AmqpTestSupport {

    @Test(timeout = 60000)
    public void testCreateMessageConsumer() throws Exception {
        Connection connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue("test.queue");
        session.createConsumer(queue);

        QueueViewMBean proxy = getProxyToQueue("test.queue");
        assertEquals(0, proxy.getQueueSize());
        connection.close();
    }

    @Test(timeout = 60000)
    public void testSyncConsumeFromQueue() throws Exception {
        Connection connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue("test.queue");
        MessageConsumer consumer = session.createConsumer(queue);

        sendToAmqQueue(1);

        QueueViewMBean proxy = getProxyToQueue("test.queue");
        assertEquals(1, proxy.getQueueSize());

        assertNotNull("Failed to receive any message.", consumer.receive(2000));

        assertEquals("Queued message not consumed.", 0, proxy.getQueueSize());
        connection.close();
    }

    private void sendToAmqQueue(int count) throws Exception {
        Connection activemqConnection = createActiveMQConnection();
        Session amqSession = activemqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue amqTestQueue = amqSession.createQueue("test.queue");
        sendMessages(activemqConnection, amqTestQueue, 1);
    }
}
