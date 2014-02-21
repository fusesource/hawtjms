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

import javax.jms.JMSException;

import org.fusesource.amqpjms.jms.JmsConnection;
import org.fusesource.amqpjms.jms.JmsConnectionFactory;
import org.junit.Test;

/**
 * Test for basic JmsConnection functionality and error handling.
 */
public class JmsConnectionTest extends AmqpTestSupport {

    @Test
    public void testCreateConnection() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getBrokerAmqpConnectionURI());
        JmsConnection connection = (JmsConnection) factory.createConnection();
        assertNotNull(connection);
    }

    @Test
    public void testCreateConnectionAndStart() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getBrokerAmqpConnectionURI());
        JmsConnection connection = (JmsConnection) factory.createConnection();
        assertNotNull(connection);
        connection.start();
    }

    @Test(expected = JMSException.class)
    public void testCreateWithDuplicateClientIdFails() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getBrokerAmqpConnectionURI());
        JmsConnection connection1 = (JmsConnection) factory.createConnection();
        connection1.setClientID("Test");
        assertNotNull(connection1);
        connection1.start();
        JmsConnection connection2 = (JmsConnection) factory.createConnection();
        connection2.setClientID("Test");
        connection2.start();
    }
}
