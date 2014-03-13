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
package org.fusesource.amqpjms.provider.failover;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;

/**
 * Manages the list of available failover URIs that are used to connect
 * and recover a connection.
 */
public class FailoverUriPool {

    private final LinkedList<URI> uris;
    private final Map<String, String> nestedOptions;
    private boolean randomize;

    public FailoverUriPool() {
        this.uris = new LinkedList<URI>();
        this.nestedOptions = Collections.emptyMap();
    }

    public FailoverUriPool(URI[] uris, Map<String, String> nestedOptions) {
        this.uris = new LinkedList<URI>(Arrays.asList(uris));
        if (nestedOptions != null) {
            this.nestedOptions = nestedOptions;
        } else {
            this.nestedOptions = Collections.emptyMap();
        }
    }

    /**
     * Returns the next URI in the pool of URIs.  The URI will be shifted to the
     * end of the list and not be attempted again until the full list has been
     * returned once.
     *
     * @return the next URI that should be used for a connection attempt.
     */
    public URI getNext() {
        URI next = uris.removeFirst();
        uris.addLast(next);

        // TODO add nested query options

        return next;
    }

    /**
     * Reports that the Failover Provider connected to the last URI returned from
     * this pool.  If the Pool is set to randomize this will result in the Pool of
     * URIs being shuffled in preparation for the next connect cycle.
     */
    public void connected() {
        if (isRandomize()) {
            Collections.shuffle(uris);
        }
    }

    public boolean isRandomize() {
        return randomize;
    }

    public void setRandomize(boolean randomize) {
        this.randomize = randomize;
        if (randomize) {
            Collections.shuffle(uris);
        }
    }

    public Map<String, String> getNestedOptions() {
        return nestedOptions;
    }
}
