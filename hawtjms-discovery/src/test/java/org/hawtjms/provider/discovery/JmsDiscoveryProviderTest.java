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
package org.hawtjms.provider.discovery;

import static org.junit.Assert.assertNotNull;
import io.hawtjms.provider.DefaultBlockingProvider;
import io.hawtjms.provider.DefaultProviderListener;
import io.hawtjms.provider.discovery.DiscoveryProviderFactory;

import java.io.IOException;
import java.net.URI;

import org.hawtjms.util.AmqpTestSupport;
import org.junit.Test;

/**
 * Test basic discovery of remote brokers
 */
public class JmsDiscoveryProviderTest extends AmqpTestSupport {

    @Override
    protected boolean isAmqpDiscovery() {
        return true;
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
}
