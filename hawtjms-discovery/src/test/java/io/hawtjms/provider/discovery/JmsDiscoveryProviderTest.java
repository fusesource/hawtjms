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
package io.hawtjms.provider.discovery;

import static org.junit.Assert.assertNotNull;
import io.hawtjms.provider.DefaultBlockingProvider;
import io.hawtjms.provider.DefaultProviderListener;
import io.hawtjms.provider.discovery.DiscoveryProviderFactory;

import java.io.IOException;
import java.net.URI;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test basic discovery of remote brokers
 */
public class JmsDiscoveryProviderTest {

    protected static final Logger LOG = LoggerFactory.getLogger(JmsDiscoveryProviderTest.class);

    @Rule public TestName name = new TestName();

    private BrokerService broker;

    @Before
    public void setup() throws Exception {
        broker = createBroker();
        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
            broker = null;
        }
    }

    @Test(timeout=30000)
    public void testCreateDiscvoeryProvider() throws Exception {
        URI discoveryUri = new URI("discovery:multicast://default");
        DefaultBlockingProvider blocking = (DefaultBlockingProvider)
            DiscoveryProviderFactory.createBlocking(discoveryUri);
        assertNotNull(blocking);

        DefaultProviderListener listener = new DefaultProviderListener();
        blocking.setProviderListener(listener);
        blocking.start();
        blocking.close();
    }

    @Test(timeout=30000, expected=IllegalStateException.class)
    public void testStartFailsWithNoListener() throws Exception {
        URI discoveryUri = new URI("discovery:multicast://default");
        DefaultBlockingProvider blocking = (DefaultBlockingProvider)
            DiscoveryProviderFactory.createBlocking(discoveryUri);
        assertNotNull(blocking);
        blocking.start();
        blocking.close();
    }

    @Test(timeout=30000, expected=IOException.class)
    public void testCreateFailsWithUnknownAgent() throws Exception {
        URI discoveryUri = new URI("discovery:unknown://default");
        DefaultBlockingProvider blocking = (DefaultBlockingProvider)
            DiscoveryProviderFactory.createBlocking(discoveryUri);
        blocking.close();
    }

    protected BrokerService createBroker() throws Exception {

        BrokerService brokerService = new BrokerService();
        brokerService.setBrokerName("localhost");
        brokerService.setPersistent(false);
        brokerService.setAdvisorySupport(false);
        brokerService.setUseJmx(false);

        TransportConnector connector = brokerService.addConnector("amqp://0.0.0.0:0");
        connector.setName("amqp");
        connector.setDiscoveryUri(new URI("multicast://default"));

        return brokerService;
    }
}
