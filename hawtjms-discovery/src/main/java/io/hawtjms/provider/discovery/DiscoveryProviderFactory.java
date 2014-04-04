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

import io.hawtjms.provider.AsyncProvider;
import io.hawtjms.provider.BlockingProvider;
import io.hawtjms.provider.DefaultBlockingProvider;
import io.hawtjms.provider.ProviderFactory;
import io.hawtjms.provider.failover.FailoverProvider;
import io.hawtjms.util.PropertyUtil;
import io.hawtjms.util.URISupport;
import io.hawtjms.util.URISupport.CompositeData;

import java.net.URI;
import java.util.Map;

/**
 * Factory for creating the Discovery Provider
 */
public class DiscoveryProviderFactory extends ProviderFactory {

    private static final String DISCOVERED_OPTION_PREFIX = "discovered.";

    @Override
    public BlockingProvider createProvider(URI remoteURI) throws Exception {

        CompositeData composite = URISupport.parseComposite(remoteURI);
        Map<String, String> options = composite.getParameters();

        // Failover will apply the nested options to each URI while attempting to connect.
        Map<String, String> nested = PropertyUtil.filterProperties(options, DISCOVERED_OPTION_PREFIX);
        FailoverProvider failover = new FailoverProvider(nested);
        PropertyUtil.setProperties(failover, options);

        // TODO - Revisit URI options setting and enhance the ProperyUtils to provide a
        //        means of setting some properties on a object and obtaining the leftovers
        //        so we can pass those along to the next until we consume them all or we
        //        have leftovers which implies a bad URI.

        DiscoveryProvider discovery = new DiscoveryProvider(remoteURI, failover);
        PropertyUtil.setProperties(discovery, options);

        DiscoveryAgent agent = DiscoveryAgentFactory.createAgent(composite.getComponents()[0]);
        discovery.setDiscoveryAgent(agent);

        return new DefaultBlockingProvider(discovery);
    }

    @Override
    public AsyncProvider createAsyncProvider(URI remoteURI) throws Exception {
        throw new UnsupportedOperationException("Async create not supported.");
    }

    @Override
    public String getName() {
        return "Discovery";
    }
}
