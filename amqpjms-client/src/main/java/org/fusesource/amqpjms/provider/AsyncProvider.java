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
import org.fusesource.amqpjms.jms.meta.JmsConsumerId;
import org.fusesource.amqpjms.jms.meta.JmsResource;
import org.fusesource.amqpjms.jms.meta.JmsSessionId;
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
     * representation and upon successful creation provide the caller with a response.
     *
     * @param resource
     *        The JmsResouce instance that indicates what is being created.
     * @param request
     *        The request object that should be signaled when this operation completes.
     *
     * @throws IOException if an error occurs or the Provider is already closed.
     */
    void create(JmsResource resource, AsyncResult<Void> request) throws IOException;

    /**
     * Starts the Provider version of the given JmsResource.
     *
     * For some JMS Resources it is necessary or advantageous to have a started state that
     * must be triggered prior to it's normal use.
     *
     * An example of this would be a MessageConsumer which should not receive any incoming
     * messages until the JMS layer is in a state to handle them.  One such time would be
     * after connection recovery.  A JMS consumer should normally recover with it's prefetch
     * value set to zero, or an AMQP link credit of zero and only open up the credit window
     * once all Connection resources are restored.
     *
     * @param resource
     *        The JmsResouce instance that indicates what is being started.
     * @param request
     *        The request object that should be signaled when this operation completes.
     *
     * @throws IOException if an error occurs or the Provider is already closed.
     */
    void start(JmsResource resource, AsyncResult<Void> request) throws IOException;

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
    void destroy(JmsResource resourceId, AsyncResult<Void> request) throws IOException;

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
    void send(JmsOutboundMessageDispatch envelope, AsyncResult<Void> request) throws IOException;

    /**
     * Called to acknowledge all messages that have been delivered in a given session.
     *
     * This method is typically used by a Session that is configured for client acknowledge
     * mode.  The acknowledgment should only be applied to Messages that have been marked
     * as delivered and not those still awaiting dispatch.
     *
     * @param sessionId
     *        the ID of the Session whose delivered messages should be acknowledged.
     * @param request
     *        The request object that should be signaled when this operation completes.
     *
     * @throws IOException if an error occurs or the Provider is already closed.
     */
    void acknowledge(JmsSessionId sessionId, AsyncResult<Void> request) throws IOException;

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
    void acknowledge(JmsInboundMessageDispatch envelope, ACK_TYPE ackType, AsyncResult<Void> request) throws IOException;

    /**
     * Called to commit an open transaction.
     *
     * @param sessionId
     *        the Id of the JmsSession that is committing the current transaction.
     * @param request
     *        The request object that should be signaled when this operation completes.
     *
     * @throws IOException if an error occurs or the Provider is already closed.
     */
    void commit(JmsSessionId sessionId, AsyncResult<Void> request) throws IOException;

    /**
     * Called to roll back an open transaction.
     *
     * @param sessionId
     *        the Id of the JmsSession that is rolling back the current transaction.
     * @param request
     *        The request object that should be signaled when this operation completes.
     *
     * @throws IOException if an error occurs or the Provider is already closed.
     */
    void rollback(JmsSessionId sessionId, AsyncResult<Void> request) throws IOException;

    /**
     * Called to recover all unacknowledged messages for a Session in client Ack mode.
     *
     * @param sessionId
     *        the Id of the JmsSession that is recovering unacknowledged messages..
     * @param request
     *        The request object that should be signaled when this operation completes.
     *
     * @throws IOException if an error occurs or the Provider is already closed.
     */
    void recover(JmsSessionId sessionId, AsyncResult<Void> request) throws IOException;

    /**
     * Remove a durable topic subscription by name.
     *
     * @param subscription
     *        the name of the durable subscription that is to be removed.
     * @param request
     *        The request object that should be signaled when this operation completes.
     *
     * @throws IOException if an error occurs or the Provider is already closed.
     */
    void unsubscribe(String subscription, AsyncResult<Void> request) throws IOException;

    /**
     * Request a remote peer send a Message to this client.  A message pull request is
     * usually only needed in the case where the client sets a zero prefetch limit on the
     * consumer.  If the consumer has a set prefetch that's greater than zero this method
     * should just return without performing and action.
     *
     * Some protocols will not be able to honor the timeout value given, in this case it
     * should still initiate the message pull if possible even if this leaves the pull
     * window open indefinitely.
     *
     * @param timeout
     *        the amount of time to tell the remote peer to keep this pull request valid.
     * @param request
     *        The request object that should be signaled when this operation completes.
     *
     * @throws IOException if an error occurs or the Provider is already closed.
     */
    void pull(JmsConsumerId consumerId, long timeout, AsyncResult<Void> request) throws IOException;

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
