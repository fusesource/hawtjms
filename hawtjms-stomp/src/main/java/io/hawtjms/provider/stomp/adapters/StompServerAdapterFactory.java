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
package io.hawtjms.provider.stomp.adapters;

import io.hawtjms.provider.stomp.StompConnection;
import io.hawtjms.util.FactoryFinder;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory class for STOMP Server Adapters.
 */
public abstract class StompServerAdapterFactory {

    private static final Logger LOG = LoggerFactory.getLogger(StompServerAdapterFactory.class);

    private static final FactoryFinder<StompServerAdapterFactory> ADAPTER_FACTORY_FINDER =
        new FactoryFinder<StompServerAdapterFactory>(StompServerAdapterFactory.class,
            "META-INF/services/io/hawtjms/providers/stomp-server-adapters/");

    /**
     * Creates a new instance of a STOMP server adapter based on the server String that
     * is returned in the STOMP CONNECTED frame.
     *
     * @param connection
     *        the StompConnection instance that will own this adapter.
     * @param server
     *        the server String returned from the CONNECTED frame.
     * @param version
     *        the version string extracted from the CONNECTED frame.
     *
     * @return a STOMP server adapter based on the server string.
     *
     * @throws Exception if an error occurs while parsing the server string and creating an adapter.
     */
    public abstract StompServerAdapter createAdapter(StompConnection connection, String server, String version) throws Exception;

    /**
     * @return the name of this STOMP server adapter factory, e.g. ActiveMQ, Apollo...etc
     */
    public abstract String getName();

    /**
     * Static create method that performs the StompServerAdapterFactory search and handles the
     * configuration and setup.  If a specific server adapter cannot be found this method will
     * return a generic server adapter instead of throwing an exception.
     *
     * @param connection
     *        the StompConnection instance that will own this adapter.
     * @param server
     *        the server String returned from the CONNECTED frame.
     *
     * @return a new StompServerAdapter instance that is ready for use.
     */
    public static StompServerAdapter create(StompConnection connection, String server) {
        StompServerAdapter result = null;

        String[] parts = server.split("/");
        if (parts.length == 0) {
            result = new GenericStompServerAdaptor(connection);
        } else {
            String serverName = parts[0];
            String version = "";
            if (parts.length > 1) {
                version = parts[1];
            }

            try {
                StompServerAdapterFactory factory = findProviderFactory(serverName);
                result = factory.createAdapter(connection, serverName, version);
            } catch (Exception ex) {
                LOG.error("Failed to create StompServerAdapter instance for: {}", serverName);
                LOG.trace("Error: ", ex);
                result = new GenericStompServerAdaptor(connection);
            }
        }
        return result;
    }

    /**
     * Searches for a StompServerAdapterFactory by using the scheme from the given server name.
     *
     * The search first checks the local cache of provider factories before moving on
     * to search in the classpath.
     *
     * @param server
     *        the server String returned from the CONNECTED frame.
     *
     * @return a server adapter factory instance matching the given server name.
     *
     * @throws IOException if an error occurs while locating the factory.
     */
    public static StompServerAdapterFactory findProviderFactory(String server) throws IOException {
        if (server == null) {
            throw new IOException("No Server name specified: [" + server + "]");
        }

        server = server.toLowerCase();
        StompServerAdapterFactory factory = null;
        try {
            factory = ADAPTER_FACTORY_FINDER.newInstance(server);
        } catch (Throwable e) {
            throw new IOException("STOMP server NOT recognized: [" + server + "]", e);
        }

        return factory;
    }
}
