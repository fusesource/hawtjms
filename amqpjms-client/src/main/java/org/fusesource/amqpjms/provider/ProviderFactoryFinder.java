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
package org.fusesource.amqpjms.provider;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;

import org.fusesource.amqpjms.util.FactoryFinder;

public abstract class ProviderFactoryFinder {

    private static final FactoryFinder PROVIDER_FACTORY_FINDER =
        new FactoryFinder("META-INF/services/org/fusesource/amqpjms/providers/");
    private static final ConcurrentHashMap<String, ProviderFactory> PROVIDER_FACTORYS =
        new ConcurrentHashMap<String, ProviderFactory>();

    /**
     * Finds and returns a ProviderFactory for the given URI.  The factory will be returned
     * in a configured state using the URI to set all options.
     *
     * @param location
     *      The URI that indicates the type and configuration of the ProviderFactory.
     *
     * @return the configured ProviderFactory.
     *
     * @throws Exception if an error occurs while locating and loading the Factory.
     */
    public static ProviderFactory locate(URI providerURI) throws Exception {
        ProviderFactory factory = findProviderFactory(providerURI);
        return factory;
    }

    /**
     * Allow registration of a Provider factory without wiring via META-INF classes
     *
     * @param scheme
     *        The URI scheme value that names the target Provider instance.
     * @param factory
     *        The ProviderFactory to register in this finder.
     */
    public static void registerProviderFactory(String scheme, ProviderFactory factory) {
        PROVIDER_FACTORYS.put(scheme, factory);
    }

    /**
     * Searches for a ProviderFactory by using the scheme from the given URI.
     *
     * The search first checks the local cache of provider factories before moving on
     * to search in the classpath.
     *
     * @param location
     *        The URI whose scheme will be used to locate a ProviderFactory.
     *
     * @return a provider factory instance matching the URI's scheme.
     *
     * @throws IOException if an error occurs while locating the factory.
     */
    public static ProviderFactory findProviderFactory(URI location) throws IOException {
        String scheme = location.getScheme();
        if (scheme == null) {
            throw new IOException("No Provider scheme specified: [" + location + "]");
        }

        ProviderFactory factory = PROVIDER_FACTORYS.get(scheme);
        if (factory == null) {
            try {
                factory = (ProviderFactory) PROVIDER_FACTORY_FINDER.newInstance(scheme);
                PROVIDER_FACTORYS.put(scheme, factory);
            } catch (Throwable e) {
                throw new IOException("Provider scheme NOT recognized: [" + scheme + "]", e);
            }
        }

        return factory;
    }
}
