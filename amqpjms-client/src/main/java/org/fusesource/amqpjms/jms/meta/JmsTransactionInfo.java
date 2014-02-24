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
package org.fusesource.amqpjms.jms.meta;

public final class JmsTransactionInfo implements JmsResource, Comparable<JmsTransactionInfo> {

    protected final byte type;
    protected final JmsConnectionId connectionId;
    protected final JmsTransactionId transactionId;

    public JmsTransactionInfo(JmsConnectionId connectionId, JmsTransactionId transactionId, byte type) {
        this.connectionId = connectionId;
        this.transactionId = transactionId;
        this.type = type;
    }

    public JmsTransactionInfo copy() {
        return new JmsTransactionInfo(connectionId, transactionId, type);
    }

    public JmsConnectionId getConnectionId() {
        return connectionId;
    }

    public JmsTransactionId getTransactionId() {
        return transactionId;
    }

    public byte getType() {
        return type;
    }

    @Override
    public int hashCode() {
        return (transactionId == null) ? 0 : transactionId.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        JmsTransactionInfo other = (JmsTransactionInfo) obj;

        if (transactionId == null && other.transactionId != null) {
            return false;
        } else if (!transactionId.equals(other.transactionId)) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(JmsTransactionInfo other) {
        return this.transactionId.compareTo(other.transactionId);
    }

    @Override
    public void visit(JmsResourceVistor visitor) throws Exception {
        // TODO Auto-generated method stub
    }
}
