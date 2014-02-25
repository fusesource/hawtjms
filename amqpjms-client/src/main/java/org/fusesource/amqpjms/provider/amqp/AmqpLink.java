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
package org.fusesource.amqpjms.provider.amqp;

import org.apache.qpid.proton.engine.Link;

/**
 * Interface for the AmqpLink derivatives.
 */
public interface AmqpLink extends AmqpResource {

    /**
     * @return the Proton Link instance this object wraps.
     */
    Link getProtonLink();

    /**
     * @return the Proton Terminus that defines this Link's remote state (Source or Target).
     */
    Object getRemoteTerminus();

    /**
     * Instructs the Link to process any state changes on the remote end in order
     * to close out any pending work.
     */
    void processUpdates();

}
