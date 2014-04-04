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
package io.hawtjms.tests.discovery;

import static org.junit.Assert.assertTrue;
import io.hawtjms.jms.JmsConnection;
import io.hawtjms.jms.JmsConnectionFactory;
import io.hawtjms.jms.JmsConnectionListener;
import io.hawtjms.jms.message.JmsInboundMessageDispatch;
import io.hawtjms.test.util.AmqpTestSupport;
import io.hawtjms.test.util.Wait;

import javax.jms.Connection;

import org.junit.Test;

/**
 * Test that a Broker using AMQP can be discovered and JMS operations can be performed.
 */
public class JmsAmqpDiscoveryTest extends AmqpTestSupport implements JmsConnectionListener {

    @Test
    public void testRunningBrokerIsDiscovered() throws Exception {
        connection = createConnection();
        final JmsConnection jmsConnection = (JmsConnection) connection;
        connection.start();

        assertTrue("connection never connected.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return jmsConnection.isConnected();
            }
        }));
    }

    @Override
    protected boolean isAmqpDiscovery() {
        return true;
    }

    protected Connection createConnection() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory("discovery:multicast://default");
        return factory.createConnection();
    }

    @Override
    public void onConnectionFailure(Throwable error) {
    }

    @Override
    public void onConnectionInterrupted() {
    }

    @Override
    public void onConnectionRestored() {
    }

    @Override
    public void onMessage(JmsInboundMessageDispatch envelope) {
    }
}
