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

import java.io.IOException;

/**
 * Interface for all agents used to detect instances of remote peers on the network.
 */
public interface DiscoveryAgent {

    /**
     * Sets the discovery listener
     *
     * @param listener
     *        the listener to notify on discovery events, or null to clear.
     */
    void setDiscoveryListener(DiscoveryListener listener);

    /**
     * Starts the agent after which new remote peers can start to be found.
     *
     * @throws IOException if an IO error occurs while starting the agent.
     * @throws IllegalStateException if the agent is not properly configured.
     */
    void start() throws IOException, IllegalStateException;

    /**
     * Stops the agent after which no new remote peers will be found.
     *
     * @throws IOException if an error occurs while stopping the agent resources.
     */
    void stop() throws IOException;

}
