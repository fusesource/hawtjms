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

import io.hawtjms.util.FactoryFinder;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Finder of DiscoveryAgent factory classes.
 */
public final class DiscoveryAgentFactoryFinder {

    private static final FactoryFinder AGENT_FACTORY_FINDER =
        new FactoryFinder("META-INF/services/io/hawtjms/discovery/agents");
    private static final ConcurrentHashMap<String, DiscoveryAgentFactory> AGENT_FACTORIES =
        new ConcurrentHashMap<String, DiscoveryAgentFactory>();

    private DiscoveryAgentFactoryFinder() {
    }

    /**
     * Finds and returns a DiscoveryAgentFactory for the given URI.  The factory will be returned
     * in a configured state using the URI to set all options.
     *
     * @param location
     *      The URI that indicates the type and configuration of the DiscoveryAgentFactory.
     *
     * @return the configured DiscoveryAgentFactory.
     *
     * @throws Exception if an error occurs while locating and loading the Factory.
     */
    public static DiscoveryAgentFactory locate(URI providerURI) throws Exception {
        DiscoveryAgentFactory factory = findAgentFactory(providerURI);
        return factory;
    }

    /**
     * Allow registration of a DiscoveryAgent factory without wiring via META-INF classes
     *
     * @param scheme
     *        The URI scheme value that names the target DiscoveryAgent instance.
     * @param factory
     *        The ProviderFactory to register in this finder.
     */
    public static void registerAgentFactory(String scheme, DiscoveryAgentFactory factory) {
        AGENT_FACTORIES.put(scheme, factory);
    }

    /**
     * Searches for a DiscoveryAgentFactory by using the scheme from the given URI.
     *
     * The search first checks the local cache of discovery agent factories before moving on
     * to search in the classpath.
     *
     * @param location
     *        The URI whose scheme will be used to locate a DiscoveryAgentFactory.
     *
     * @return a Discovery Agent factory instance matching the URI's scheme.
     *
     * @throws IOException if an error occurs while locating the factory.
     */
    public static DiscoveryAgentFactory findAgentFactory(URI location) throws IOException {
        String scheme = location.getScheme();
        if (scheme == null) {
            throw new IOException("No Discovery Agent scheme specified: [" + location + "]");
        }

        DiscoveryAgentFactory factory = AGENT_FACTORIES.get(scheme);
        if (factory == null) {
            try {
                factory = (DiscoveryAgentFactory) AGENT_FACTORY_FINDER.newInstance(scheme);
                AGENT_FACTORIES.put(scheme, factory);
            } catch (Throwable e) {
                throw new IOException("Discovery Agent scheme NOT recognized: [" + scheme + "]", e);
            }
        }

        return factory;
    }
}
