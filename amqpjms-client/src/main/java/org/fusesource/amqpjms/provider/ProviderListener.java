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

import org.fusesource.amqpjms.jms.message.JmsInboundMessageDispatch;

/**
 * Events interface used to update the listener with changes in provider state.
 */
public interface ProviderListener {

    /**
     * Called when a new Message has arrived for a registered consumer.
     *
     * @param envelope
     *        The dispatch object containing the message and delivery information.
     */
    void onMessage(JmsInboundMessageDispatch envelope);

    /**
     * Called from a fault tolerant Provider instance to signal that the underlying
     * connection to the Broker has been lost.  The Provider will attempt to reconnect
     * following this event unless closed.
     */
    void onConnectionInterrupted();

    /**
     * Called to indicate that a connection to the Broker has been reestablished and
     * that notified listener should start to recover it's state.  The provider will
     * not transition to the recovered state until the listener notifies the provider
     * that recovery is complete.
     *
     * @param provider
     *        The new Provider instance that will become active after the state
     *        has been recovered.
     */
    void onConnectionRecovery(BlockingProvider provider);

    /**
     * Called to signal that all recovery operations are now complete and the Provider
     * is again in a normal connected state.
     */
    void onConnectionRestored();

    /**
     * Called to indicate that the underlying connection to the Broker has been lost and
     * the Provider will not perform any reconnect.  Following this call the provider is
     * in a failed state and further calls to it will throw an Exception.
     *
     * @param ex
     *        The exception that indicates the cause of this Provider failure.
     */
    void onConnectionFailure(IOException ex);

}
