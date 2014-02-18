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
import static org.junit.Assert.assertNull;

import java.net.URI;
import java.net.URISyntaxException;

import javax.jms.JMSException;

import org.fusesource.amqpjms.jms.JmsConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class JmsConnectionFactoryTest {

    private final String username = "USER";
    private final String password = "PASSWORD";

    private final String badProvider = "unknown://127.0.0.1:61616";
    private final URI badProviderURI;

    private final String goodProvider = "amqp://127.0.0.1:61616";
    private final URI goodProviderURI;

    public JmsConnectionFactoryTest() throws URISyntaxException {
        badProviderURI = new URI(badProvider);
        goodProviderURI = new URI(goodProvider);
    }

    @Before
    public void before() {
    }

    @After
    public void after() {
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
    public void testCreateConnectionBadProviderURI() throws JMSException {
        JmsConnectionFactory factory = new JmsConnectionFactory(badProviderURI);
        factory.createConnection();
    }

    @Test(expected = JMSException.class)
    public void testCreateConnectionBadProviderString() throws JMSException {
        JmsConnectionFactory factory = new JmsConnectionFactory(badProvider);
        factory.createConnection();
    }

    @Ignore
    @Test
    public void testCreateConnectionGoodProviderURI() throws JMSException {
        JmsConnectionFactory factory = new JmsConnectionFactory(goodProviderURI);
        assertNotNull(factory.createConnection());
    }

    @Ignore
    @Test
    public void testCreateConnectionGoodProviderString() throws JMSException {
        JmsConnectionFactory factory = new JmsConnectionFactory(goodProvider);
        assertNotNull(factory.createConnection());
    }
}
