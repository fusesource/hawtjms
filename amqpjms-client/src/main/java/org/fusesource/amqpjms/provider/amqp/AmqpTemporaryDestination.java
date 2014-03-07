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

import org.apache.qpid.proton.engine.Sender;
import org.fusesource.amqpjms.jms.JmsDestination;

/**
 * Manages a Temporary Destination linked to a given Connection.
 */
public class AmqpTemporaryDestination extends AbstractAmqpResource<JmsDestination, Sender> {

    private final AmqpConnection connection;

    /**
     * @param info
     */
    public AmqpTemporaryDestination(AmqpConnection connection, JmsDestination info) {
        super(info);
        this.connection = connection;
    }

    @Override
    public void processUpdates() {
        // TODO Auto-generated method stub
    }

    @Override
    protected void doOpen() {
        this.connection.addToPendingOpen(this);
    }

    @Override
    protected void doClose() {
        this.connection.addToPendingClose(this);
    }

    public AmqpConnection getConnection() {
        return this.connection;
    }

    public JmsDestination getJmsDestination() {
        return this.info;
    }
}