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
import org.fusesource.amqpjms.jms.meta.JmsSessionInfo;
import org.fusesource.amqpjms.jms.meta.JmsTransactionId;

/**
 * Handles the operations surrounding AMQP transaction control.
 *
 * The Transaction will carry a JmsTransactionId while the Transaction is open, once a
 * transaction has been committed or rolled back the Transaction Id is cleared.
 */
public class AmqpTransaction extends AbstractAmqpResource<JmsSessionInfo, Sender> {

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
    public AmqpTransaction(AmqpSession session) {
        super(session.getJmsResource());
        this.session = session;
    }

    public void createTransaction(JmsTransactionId txId) {

    }

    public void commit() {

    }

    public void rollback() {

    }

    public AmqpSession getSession() {
        return this.session;
    }

    public JmsTransactionId getTransactionId() {
        return this.current;
    }

    @Override
    public void processUpdates() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void doOpen() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void doClose() {
        // TODO Auto-generated method stub

    }
}
