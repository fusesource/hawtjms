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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.fusesource.amqpjms.util.AmqpTestSupport;
import org.fusesource.amqpjms.util.Wait;
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

        final QueueViewMBean proxy = getProxyToQueue("test.queue");
        assertEquals(1, proxy.getQueueSize());

        assertNotNull("Failed to receive any message.", consumer.receive(2000));

        assertTrue("Queued message not consumed.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return proxy.getQueueSize() == 0;
            }
        }));
        connection.close();
    }

    /**
     * Test to check if consumer thread wakes up inside a receive(timeout) after
     * a message is dispatched to the consumer
     *
     * @throws Exception
     */
    @Test
    public void testConsumerReceiveBeforeMessageDispatched() throws Exception {
        final Connection connection = createAmqpConnection();
        connection.start();

        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue queue = session.createQueue("test");

        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    TimeUnit.SECONDS.sleep(10);
                    MessageProducer producer = session.createProducer(queue);
                    producer.send(session.createTextMessage("Hello"));
                } catch (Exception e) {
                    LOG.warn("Caught during message send: {}", e.getMessage());
                }
            }
        };
        t.start();
        MessageConsumer consumer = session.createConsumer(queue);
        Message msg = consumer.receive(60000);
        assertNotNull(msg);
        connection.close();
    }

    private void sendToAmqQueue(int count) throws Exception {
        Connection activemqConnection = createActiveMQConnection();
        Session amqSession = activemqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue amqTestQueue = amqSession.createQueue("test.queue");
        sendMessages(activemqConnection, amqTestQueue, 1);
    }
}
