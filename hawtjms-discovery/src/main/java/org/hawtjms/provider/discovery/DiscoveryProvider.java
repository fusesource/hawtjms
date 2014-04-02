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

import io.hawtjms.provider.AsyncProviderWrapper;
import io.hawtjms.provider.failover.FailoverProvider;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An AsyncProvider instance that wraps the FailoverProvider and listens for
 * events about discovered remote peers using a configured DiscoveryAgent
 * instance.
 */
public class DiscoveryProvider extends AsyncProviderWrapper<FailoverProvider> {

    private final URI discoveryUri;

    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryProviderFactory.class);

    private DiscoveryAgent discoveryAgent;
    private final ConcurrentHashMap<String, URI> serviceURIs = new ConcurrentHashMap<String, URI>();

    /**
     * Creates a new instance of the DiscoveryProvider.
     *
     * The Provider is created and initialized with the original URI used to create it,
     * and an instance of a FailoverProcider which it will use to initiate and maintain
     * connections to the discovered peers.
     *
     * @param discoveryUri
     * @param next
     */
    public DiscoveryProvider(URI discoveryUri, FailoverProvider next) {
        super(next);
        this.discoveryUri = discoveryUri;
    }

    @Override
    public void start() throws IOException, IllegalStateException {
        if (this.discoveryAgent == null) {
            throw new IllegalStateException("No DiscoveryAgent configured.");
        }

        discoveryAgent.setDiscoveryListener(null);
        discoveryAgent.start();

        next.start();
    }

    /**
     * @return the original URI used to configure this DiscoveryProvider.
     */
    public URI getDiscoveryURI() {
        return this.discoveryUri;
    }
}
