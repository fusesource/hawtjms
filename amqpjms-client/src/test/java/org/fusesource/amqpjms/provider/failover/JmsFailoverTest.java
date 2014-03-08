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
package org.fusesource.amqpjms.provider.failover;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.fusesource.amqpjms.jms.JmsConnectionFactory;
import org.fusesource.amqpjms.util.AmqpTestSupport;
import org.fusesource.amqpjms.util.Wait;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Basic tests for the FailoverProvider implementation
 */
public class JmsFailoverTest extends AmqpTestSupport {

    @Test(timeout=60000)
    public void testFailoverConnects() throws Exception {
        URI brokerURI = new URI("failover:" + getBrokerAmqpConnectionURI());
        Connection connection = createAmqpConnection(brokerURI);
        connection.start();
        connection.close();
    }

    @Test(timeout=60000, expected=JMSException.class)
    public void testStartupReconnectAttempts() throws Exception {
        URI brokerURI = new URI("failover://(amqp://localhost:61616)" +
                                "?maxReconnectDelay=1000&startupMaxReconnectAttempts=5");
        JmsConnectionFactory factory = new JmsConnectionFactory(brokerURI);
        Connection connection = factory.createConnection();
        connection.start();
    }

    @Test(timeout=60000)
    public void testStartFailureWithAsyncExceptionListener() throws Exception {
        URI brokerURI = new URI("failover://("+ getBrokerAmqpConnectionURI() +")" +
                                "?maxReconnectDelay=1000&maxReconnectAttempts=5");

        final CountDownLatch failed = new CountDownLatch(1);
        JmsConnectionFactory factory = new JmsConnectionFactory(brokerURI);
        factory.setExceptionListener(new ExceptionListener() {

            @Override
            public void onException(JMSException exception) {
                LOG.info("Connection got exception: {}", exception.getMessage());
                failed.countDown();
            }
        });
        Connection connection = factory.createConnection();
        connection.start();

        stopBroker();

        assertTrue("No async exception", failed.await(15, TimeUnit.SECONDS));
    }

    @Ignore
    @SuppressWarnings("unused")
    @Test(timeout=200000)
    public void testBasicStateRestoration() throws Exception {
        URI brokerURI = new URI("failover://("+ getBrokerAmqpConnectionURI() +")?maxReconnectDelay=1000");

        Connection connection = createAmqpConnection(brokerURI);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.toString());
        MessageProducer producer = session.createProducer(queue);
        MessageConsumer consumer = session.createConsumer(queue);

        assertEquals(1, brokerService.getAdminView().getQueueSubscribers().length);
        assertEquals(1, brokerService.getAdminView().getQueueProducers().length);

        stopBroker();
        TimeUnit.SECONDS.sleep(2);
        startBroker();

        assertTrue("Should have a new connection.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return brokerService.getAdminView().getCurrentConnectionsCount() == 1;
            }
        }));

        assertEquals(1, brokerService.getAdminView().getQueueSubscribers().length);
        assertEquals(1, brokerService.getAdminView().getQueueProducers().length);

        connection.close();
    }
}
