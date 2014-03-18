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

import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.transaction.Coordinator;
import org.apache.qpid.proton.amqp.transaction.TxnCapability;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Sender;
import org.fusesource.amqpjms.jms.meta.JmsSessionInfo;
import org.fusesource.amqpjms.jms.meta.JmsTransactionId;
import org.fusesource.amqpjms.provider.AsyncResult;

/**
 * Handles the operations surrounding AMQP transaction control.
 *
 * The Transaction will carry a JmsTransactionId while the Transaction is open, once a
 * transaction has been committed or rolled back the Transaction Id is cleared.
 */
public class AmqpTransactionContext extends AbstractAmqpResource<JmsSessionInfo, Sender> implements AmqpLink {

    private final AmqpSession session;
    private JmsTransactionId current;

    /**
     * Creates a new AmqpTransaction instance.
     *
     * @param session
     *        The session that owns this transaction
     * @param info
     *        The JmsTransactionInfo that defines this Transaction.
     */
    public AmqpTransactionContext(AmqpSession session) {
        super(session.getJmsResource());
        this.session = session;
    }

    @Override
    public void processUpdates() {
        // TODO Auto-generated method stub
    }

    @Override
    protected void doOpen() {
        Coordinator coordinator = new Coordinator();
        coordinator.setCapabilities(TxnCapability.LOCAL_TXN, TxnCapability.MULTI_TXNS_PER_SSN);
        Source source = new Source();

        String coordinatorName = info.getSessionId().toString();
        endpoint = session.getProtonSession().sender(coordinatorName);
        endpoint.setSource(source);
        endpoint.setTarget(coordinator);
        endpoint.setSenderSettleMode(SenderSettleMode.UNSETTLED);
        endpoint.setReceiverSettleMode(ReceiverSettleMode.FIRST);

        this.session.addPedingLinkOpen(this);
    }

    @Override
    protected void doClose() {
        this.session.addPedingLinkClose(this);
    }

    public void begin(JmsTransactionId txId, AsyncResult<Void> request) {
        request.onSuccess();
    }

    public void commit(JmsTransactionId txId, AsyncResult<Void> request) {
    }

    public void rollback(JmsTransactionId txId, AsyncResult<Void> request) {
    }

    public AmqpSession getSession() {
        return this.session;
    }

    public JmsTransactionId getTransactionId() {
        return this.current;
    }

    @Override
    public Link getProtonLink() {
        return this.endpoint;
    }

    @Override
    public String toString() {
        return this.session.getSessionId() + ": txContext";
    }
}
