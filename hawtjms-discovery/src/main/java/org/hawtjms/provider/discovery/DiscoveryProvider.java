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
import io.hawtjms.util.URISupport;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An AsyncProvider instance that wraps the FailoverProvider and listens for
 * events about discovered remote peers using a configured DiscoveryAgent
 * instance.
 */
public class DiscoveryProvider extends AsyncProviderWrapper<FailoverProvider> implements DiscoveryListener {

    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryProviderFactory.class);
    private static final String DISCOVERED_OPTION_PREFIX = "discovered.";

    private final URI discoveryUri;
    private DiscoveryAgent discoveryAgent;
    private final ConcurrentHashMap<String, URI> serviceURIs = new ConcurrentHashMap<String, URI>();
    private Map<String, String> discoveredOptions = Collections.emptyMap();

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

    @Override
    public void close() {
        discoveryAgent.close();
        next.close();
    }

    //------------------- Property Accessors ---------------------------------//

    /**
     * @return the original URI used to configure this DiscoveryProvider.
     */
    public URI getDiscoveryURI() {
        return this.discoveryUri;
    }

    /**
     * @return the configured DiscoveryAgent instance used by this DiscoveryProvider.
     */
    public DiscoveryAgent getDiscoveryAgent() {
        return this.discoveryAgent;
    }

    /**
     * @return the Map of options that will be added to the URI of discovered peers.
     */
    public Map<String, String> getDiscoveredOptions() {
        return this.discoveredOptions;
    }

    /**
     * Sets the Map of key / value options that are added and URI properties to the discovered
     * URI of each remote peer.
     *
     * @param discoveredOptions
     *        a Map<String, String> that is added as key / value to a discovered remote peer URI.
     */
    public void setDiscoveredOptions(Map<String, String> discoveredOptions) {
        this.discoveredOptions = discoveredOptions;
    }

    //------------------- Discovery Event Handlers ---------------------------//

    @Override
    public void onServiceAdd(DiscoveryEvent event) {
        String url = event.getPeerUri();
        if (url != null) {
            try {
                URI uri = new URI(url);
                LOG.info("Adding new peer connection URL: {}", uri);
                uri = URISupport.applyParameters(uri, discoveredOptions, DISCOVERED_OPTION_PREFIX);
                serviceURIs.put(event.getPeerName(), uri);
                next.add(uri);
            } catch (URISyntaxException e) {
                LOG.warn("Could not add remote URI: {} due to bad URI syntax: {}", url, e.getMessage());
            }
        }
    }

    @Override
    public void onServiceRemove(DiscoveryEvent event) {
        URI uri = serviceURIs.get(event.getPeerName());
        if (uri != null) {
            next.remove(uri);
        }
    }

    //------------------- Connection State Handlers --------------------------//

    @Override
    public void onConnectionInterrupted() {
        this.discoveryAgent.resume();
        super.onConnectionInterrupted();
    }

    @Override
    public void onConnectionRestored() {
        this.discoveryAgent.suspend();
        super.onConnectionRestored();
    }
}
