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
import static org.junit.Assert.assertNull;

import java.net.URI;
import java.net.URISyntaxException;

import javax.jms.Connection;
import javax.jms.JMSException;

import org.fusesource.amqpjms.util.AmqpTestSupport;
import org.junit.Test;

public class JmsConnectionFactoryTest extends AmqpTestSupport {

    private final String username = "USER";
    private final String password = "PASSWORD";

    protected String getGoodProviderAddress() {
        return "amqp://127.0.0.1:" + amqpPort;
    }

    protected URI getGoodProviderAddressURI() throws URISyntaxException {
        return new URI(getGoodProviderAddress());
    }

    protected String getBadProviderAddress() {
        return "bad://127.0.0.1:" + amqpPort;
    }

    protected URI getBadProviderAddressURI() throws URISyntaxException {
        return new URI(getBadProviderAddress());
    }

    @Test
    public void testConnectionFactoryCreate() {
        JmsConnectionFactory factory = new JmsConnectionFactory();
        assertNull(factory.getUsername());
        assertNull(factory.getPassword());
    }

    @Test
    public void testConnectionFactoryCreateUsernameAndPassword() {
        JmsConnectionFactory factory = new JmsConnectionFactory(username, password);
        assertNotNull(factory.getUsername());
        assertNotNull(factory.getPassword());
        assertEquals(username, factory.getUsername());
        assertEquals(password, factory.getPassword());
    }

    @Test(expected = JMSException.class)
    public void testCreateConnectionBadProviderURI() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getBadProviderAddressURI());
        factory.createConnection();
    }

    @Test(expected = JMSException.class)
    public void testCreateConnectionBadProviderString() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getBadProviderAddress());
        factory.createConnection();
    }

    @Test
    public void testCreateConnectionGoodProviderURI() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getGoodProviderAddressURI());
        Connection connection = factory.createConnection();
        assertNotNull(connection);
        connection.close();
    }

    @Test
    public void testCreateConnectionGoodProviderString() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getGoodProviderAddress());
        Connection connection = factory.createConnection();
        assertNotNull(connection);
        connection.close();
    }
}
