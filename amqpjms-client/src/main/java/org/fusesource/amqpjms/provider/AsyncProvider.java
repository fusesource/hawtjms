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

import org.fusesource.amqpjms.jms.message.JmsInboundMessageDispatch;
import org.fusesource.amqpjms.jms.message.JmsOutboundMessageDispatch;
import org.fusesource.amqpjms.jms.meta.JmsResource;
import org.fusesource.amqpjms.jms.meta.JmsSessionInfo;
import org.fusesource.amqpjms.jms.meta.JmsTransactionId;
import org.fusesource.amqpjms.provider.ProviderConstants.ACK_TYPE;

/**
 * Defines the interface that an Implementation of a Specific wire level protocol
 * provider must implement.  This Provider interface requires that the implementation
 * methods all operate in an asynchronous manner.
 */
public interface AsyncProvider {

    /**
     * Performs the initial low level connection for this provider such as TCP or
     * SSL connection to a remote Broker.  If this operation fails then the Provider
     * is considered to be unusable and no further operations should be attempted
     * using this Provider.
     *
     * @throws IOException if the remote resource can not be contacted.
     */
    void connect() throws IOException;

    /**
     * Closes this Provider terminating all connections and canceling any pending
     * operations.  The Provider is considered unusable after this call.  This call
     * is a blocking call and will not return until the Provider has closed or an
     * error occurs.
     */
    void close();

    /**
     * Returns the URI used to configure this Provider and specify the remote address of the
     * Broker it connects to.
     *
     * @return the URI used to configure this Provider.
     */
    URI getRemoteURI();

    /**
     * Create the Provider version of the given JmsResource.
     *
     * For each JMS Resource type the Provider implementation must create it's own internal
     * representation and upon successful creation provide the caller with a response.  The
     * response is either a possible updated version of the requested JmsResource instance
     * with any necessary configuration changes, or an error value indicating what happened.
     *
     * @param resource
     *        The JmsResouce instance that indicates what is being created.
     * @param request
     *        The request object that should be signaled when this operation completes.
     *
     * @throws IOException if an error occurs or the Provider is already closed.
     */
    void create(JmsResource resource, ProviderRequest<JmsResource> request) throws IOException;

    /**
     * Instruct the Provider to dispose of a given JmsResource.
     *
     * The provider is given a JmsResource which it should use to remove any associated
     * resources and inform the remote Broker instance of the removal of this resource.
     *
     * @param resource
     *        The JmsResouce that identifies a previously created JmsResource.
     * @param request
     *        The request object that should be signaled when this operation completes.
     *
     * @throws IOException if an error occurs or the Provider is already closed.
     */
    void destroy(JmsResource resource, ProviderRequest<Void> request) throws IOException;

    /**
     * Sends the JmsMessage contained in the outbound dispatch envelope.
     *
     * @param envelope
     *        the message envelope containing the JmsMessage to send.
     * @param request
     *        The request object that should be signaled when this operation completes.
     *
     * @throws IOException if an error occurs or the Provider is already closed.
     */
    void send(JmsOutboundMessageDispatch envelope, ProviderRequest<Void> request) throws IOException;

    /**
     * Called to acknowledge all messages that have been delivered in a given session.
     *
     * This method is typically used by a Session that is configured for client acknowledge
     * mode.  The acknowledgment should only be applied to Messages that have been marked
     * as delivered and not those still awaiting dispatch.
     *
     * @param session
     *        the Session whose delivered messages should be acknowledged.
     * @param request
     *        The request object that should be signaled when this operation completes.
     *
     * @throws IOException if an error occurs or the Provider is already closed.
     */
    void acknowledge(JmsSessionInfo session, ProviderRequest<Void> request) throws IOException;

    /**
     * Called to acknowledge a JmsMessage has been delivered, consumed, re-delivered...etc.
     *
     * The provider should perform an acknowledgment for the message based on the configured
     * mode of the consumer that it was dispatched to and the capabilities of the protocol.
     *
     * @param envelope
     *        The message dispatch envelope containing the Message delivery information.
     * @param ackType
     *        The type of acknowledgment being done.
     * @param request
     *        The request object that should be signaled when this operation completes.
     *
     * @throws IOException if an error occurs or the Provider is already closed.
     */
    void acknowledge(JmsInboundMessageDispatch envelope, ACK_TYPE ackType, ProviderRequest<Void> request) throws IOException;

    /**
     * Called to commit an open transaction.
     *
     * @param txId
     *        the transaction id that should be committed.
     * @param request
     *        The request object that should be signaled when this operation completes.
     *
     * @throws IOException if an error occurs or the Provider is already closed.
     */
    void commit(JmsTransactionId txId, ProviderRequest<Void> request) throws IOException;

    /**
     * Called to roll back an open transaction.
     *
     * @param txId
     *        the transaction id that should be rolled back.
     * @param request
     *        The request object that should be signaled when this operation completes.
     *
     * @throws IOException if an error occurs or the Provider is already closed.
     */
    void rollback(JmsTransactionId txId, ProviderRequest<Void> request) throws IOException;

    /**
     * Sets the listener of events from this Provider instance.
     *
     * @param listener
     *        The listener instance that will receive all event callbacks.
     */
    void setProviderListener(ProviderListener listener);

    /**
     * Gets the currently set ProdiverListener instance.
     *
     * @return the currently set ProviderListener instance.
     */
    ProviderListener getProviderListener();

}
