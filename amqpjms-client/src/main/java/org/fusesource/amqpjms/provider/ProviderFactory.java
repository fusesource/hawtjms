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

import java.net.URI;

/**
 * Interface that all JMS Providers must implement.
 */
public interface ProviderFactory {

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
    BlockingProvider createProvider(URI remoteURI) throws Exception;

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
    AsyncProvider createAsyncProvider(URI remoteURI) throws Exception;

    /**
     * @return the name of this JMS Provider, e.g. STOMP, AMQP, MQTT...etc
     */
    String getName();

}
