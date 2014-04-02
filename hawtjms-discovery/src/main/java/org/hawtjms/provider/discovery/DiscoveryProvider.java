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

import io.hawtjms.provider.AsyncProvider;
import io.hawtjms.provider.AsyncProviderWrapper;

import java.net.URI;

/**
 * An AsyncProvider instance that wraps the FailoverProvider and listens for
 * events about discovered remote peers using a configured DiscoveryAgent
 * instance.
 */
public class DiscoveryProvider extends AsyncProviderWrapper {

    private final URI discoveryUri;

    public DiscoveryProvider(URI discoveryUri, AsyncProvider next) {
        super(next);
        this.discoveryUri = discoveryUri;
    }

    /**
     * @return the original URI used to configure this DiscoveryProvider.
     */
    public URI getDiscoveryURI() {
        return this.discoveryUri;
    }
}
