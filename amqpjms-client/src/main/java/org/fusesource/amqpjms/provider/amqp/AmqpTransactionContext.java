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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.BufferOverflowException;

import javax.jms.IllegalStateException;
import javax.jms.TransactionRolledBackException;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.transaction.Coordinator;
import org.apache.qpid.proton.amqp.transaction.Declare;
import org.apache.qpid.proton.amqp.transaction.Declared;
import org.apache.qpid.proton.amqp.transaction.Discharge;
import org.apache.qpid.proton.amqp.transaction.TxnCapability;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.message.Message;
import org.fusesource.amqpjms.jms.meta.JmsSessionInfo;
import org.fusesource.amqpjms.jms.meta.JmsTransactionId;
import org.fusesource.amqpjms.provider.AsyncResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the operations surrounding AMQP transaction control.
 *
 * The Transaction will carry a JmsTransactionId while the Transaction is open, once a
 * transaction has been committed or rolled back the Transaction Id is cleared.
 */
public class AmqpTransactionContext extends AbstractAmqpResource<JmsSessionInfo, Sender> implements AmqpLink {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpTransactionContext.class);

    private final AmqpSession session;
    private JmsTransactionId current;
    private long nextTagId;

    private Delivery pendingDelivery;
    private AsyncResult<Void> pendingRequest;

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
        if (pendingDelivery != null && pendingDelivery.remotelySettled()) {
            DeliveryState state = pendingDelivery.getRemoteState();
            if (state instanceof Declared) {
                Declared declared = (Declared) state;
                current.setProviderHint(declared.getTxnId());
                pendingDelivery.settle();
                LOG.info("New TX started: {}", current.getProviderHint());
                AsyncResult<Void> request = this.pendingRequest;
                this.pendingRequest = null;
                this.pendingDelivery = null;
                request.onSuccess();
            } else if (state instanceof Rejected) {
                LOG.info("Last TX request failed: {}", current.getProviderHint());
                pendingDelivery.settle();
                Rejected rejected = (Rejected) state;
                TransactionRolledBackException ex =
                    new TransactionRolledBackException(rejected.getError().getDescription());
                AsyncResult<Void> request = this.pendingRequest;
                this.current = null;
                this.pendingRequest = null;
                this.pendingDelivery = null;
                request.onFailure(ex);
            } else {
                LOG.info("Last TX request succeeded: {}", current.getProviderHint());
                pendingDelivery.settle();
                AsyncResult<Void> request = this.pendingRequest;
                this.current = null;
                this.pendingRequest = null;
                this.pendingDelivery = null;
                request.onSuccess();
            }
        }

        // TODO check for and handle endpoint detached state.
        //endpoint.getRemoteState().equals(EndpointState.CLOSED);
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

    public void begin(JmsTransactionId txId, AsyncResult<Void> request) throws Exception {
        if (current != null) {
            throw new IOException("Begin called while a TX is still Active.");
        }

        Message message = session.getMessageFactory().createMessage();
        Declare declare = new Declare();
        message.setBody(new AmqpValue(declare));

        pendingDelivery = endpoint.delivery(getNextTagId());
        pendingRequest = request;
        current = txId;

        sendTxCommand(message);
    }

    public void commit(AsyncResult<Void> request) throws Exception {
        if (current == null) {
            throw new IllegalStateException("Rollback called with no active Transaction.");
        }

        Message message = session.getMessageFactory().createMessage();
        Discharge discharge = new Discharge();
        discharge.setFail(false);
        discharge.setTxnId((Binary) current.getProviderHint());
        message.setBody(new AmqpValue(discharge));

        pendingDelivery = endpoint.delivery(getNextTagId());
        pendingRequest = request;

        sendTxCommand(message);
    }

    public void rollback(AsyncResult<Void> request) throws Exception {
        if (current == null) {
            throw new IllegalStateException("Rollback called with no active Transaction.");
        }

        Message message = session.getMessageFactory().createMessage();
        Discharge discharge = new Discharge();
        discharge.setFail(true);
        discharge.setTxnId((Binary) current.getProviderHint());
        message.setBody(new AmqpValue(discharge));

        pendingDelivery = endpoint.delivery(getNextTagId());
        pendingRequest = request;

        sendTxCommand(message);
    }

    private void sendTxCommand(Message message) throws IOException {
        int encodedSize = 0;
        byte[] buffer = new byte[4 * 1024];
        while (true) {
            try {
                encodedSize = message.encode(buffer, 0, buffer.length);
                break;
            } catch (BufferOverflowException e) {
                buffer = new byte[buffer.length * 2];
            }
        }

        this.endpoint.send(buffer, 0, encodedSize);
        this.endpoint.advance();
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

    private byte[] getNextTagId() throws UnsupportedEncodingException {
        return Long.toHexString(nextTagId++).getBytes("UTF-8");
    }
}
