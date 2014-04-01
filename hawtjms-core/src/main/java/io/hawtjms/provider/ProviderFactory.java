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
package io.hawtjms.provider;

import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface that all JMS Providers must implement.
 */
public abstract class ProviderFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ProviderFactory.class);

    /**
     * Creates an instance of the given BlockingProvider and configures it using the
     * properties set on the given remote broker URI.
     *
     * @param remoteURI
     *        The URI used to connect to a remote Broker.
     *
     * @return a new BlockingProvider instance.
     *
     * @throws Exception if an error occurs while creating the Provider instance.
     */
    public abstract BlockingProvider createProvider(URI remoteURI) throws Exception;

    /**
     * Creates an instance of the given AsyncProvider and configures it using the
     * properties set on the given remote broker URI.
     *
     * @param remoteURI
     *        The URI used to connect to a remote Broker.
     *
     * @return a new AsyncProvider instance.
     *
     * @throws Exception if an error occurs while creating the Provider instance.
     */
    public abstract AsyncProvider createAsyncProvider(URI remoteURI) throws Exception;

    /**
     * @return the name of this JMS Provider, e.g. STOMP, AMQP, MQTT...etc
     */
    public abstract String getName();

    /**
     * Static create method that performs the ProviderFactory search and handles the
     * configuration and setup.
     *
     * @param remoteURI
     *        the URI of the remote peer.
     *
     * @return a new BlockingProvider instance that is ready for use.
     *
     * @throws Exception if an error occurs while creating the BlockingProvider instance.
     */
    public static BlockingProvider createBlocking(URI remoteURI) throws Exception {
        BlockingProvider result = null;

        try {
            ProviderFactory factory = ProviderFactoryFinder.findProviderFactory(remoteURI);
            result = factory.createProvider(remoteURI);
            result.connect();
        } catch (Exception ex) {
            LOG.error("Failed to create BlockingProvider instance for: {}", remoteURI.getScheme());
            LOG.trace("Error: ", ex);
            throw ex;
        }

        return result;
    }

    /**
     * Static create method that performs the ProviderFactory search and handles the
     * configuration and setup.
     *
     * @param remoteURI
     *        the URI of the remote peer.
     * @param sslContext
     *        the configured SSL Context if the target Provider is SSL capable.
     *
     * @return a new AsyncProvider instance that is ready for use.
     *
     * @throws Exception if an error occurs while creating the BlockingProvider instance.
     */
    public static AsyncProvider createAsync(URI remoteURI) throws Exception {
        AsyncProvider result = null;

        try {
            ProviderFactory factory = ProviderFactoryFinder.findProviderFactory(remoteURI);
            result = factory.createAsyncProvider(remoteURI);
            result.connect();
        } catch (Exception ex) {
            LOG.error("Failed to create BlockingProvider instance for: {}", remoteURI.getScheme());
            LOG.trace("Error: ", ex);
            throw ex;
        }

        return result;
    }
}
