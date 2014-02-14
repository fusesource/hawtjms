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

import org.fusesource.amqpjms.jms.JmsDestination;
import org.fusesource.amqpjms.jms.util.ToStringSupport;

public class JmsConsumerMeta implements Comparable<JmsConsumerMeta> {

    protected final JmsConsumerId consumerId;
    protected JmsDestination destination;
    protected int prefetchSize;
    protected boolean browser;
    protected String selector;
    protected String clientId;
    protected String subscriptionName;
    protected boolean noLocal;

    // Can be used to track the last consumed message.
    private transient long lastDeliveredSequenceId;

    public JmsConsumerMeta(JmsConsumerId consumerId) {
        this.consumerId = consumerId;
    }

    public JmsConsumerMeta(JmsSessionMeta sessionInfo, long consumerId) {
        this.consumerId = new JmsConsumerId(sessionInfo.getSessionId(), consumerId);
    }

    public JmsConsumerMeta copy() {
        JmsConsumerMeta info = new JmsConsumerMeta(consumerId);
        copy(info);
        return info;
    }

    public void copy(JmsConsumerMeta info) {
        info.destination = destination;
        info.prefetchSize = prefetchSize;
        info.browser = browser;
        info.selector = selector;
        info.clientId = clientId;
        info.subscriptionName = subscriptionName;
        info.noLocal = noLocal;
    }

    public boolean isDurable() {
        return subscriptionName != null;
    }

    public JmsConsumerId getConsumerId() {
        return consumerId;
    }

    public boolean isBrowser() {
        return browser;
    }

    public void setBrowser(boolean browser) {
        this.browser = browser;
    }

    public JmsDestination getDestination() {
        return destination;
    }

    public void setDestination(JmsDestination destination) {
        this.destination = destination;
    }

    public int getPrefetchSize() {
        return prefetchSize;
    }

    public void setPrefetchSize(int prefetchSize) {
        this.prefetchSize = prefetchSize;
    }

    public String getSelector() {
        return selector;
    }

    public void setSelector(String selector) {
        this.selector = selector;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getSubscriptionName() {
        return subscriptionName;
    }

    public void setSubscriptionName(String durableSubscriptionId) {
        this.subscriptionName = durableSubscriptionId;
    }

    public boolean isNoLocal() {
        return noLocal;
    }

    public void setNoLocal(boolean noLocal) {
        this.noLocal = noLocal;
    }

    public void setLastDeliveredSequenceId(long lastDeliveredSequenceId) {
        this.lastDeliveredSequenceId  = lastDeliveredSequenceId;
    }

    public long getLastDeliveredSequenceId() {
        return lastDeliveredSequenceId;
    }

    @Override
    public String toString() {
        return ToStringSupport.toString(this);
    }

    @Override
    public int hashCode() {
        return (consumerId == null) ? 0 : consumerId.hashCode();
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

        JmsConsumerMeta other = (JmsConsumerMeta) obj;

        if (consumerId == null && other.consumerId != null) {
            return false;
        } else if (!consumerId.equals(other.consumerId)) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(JmsConsumerMeta other) {
        return this.consumerId.compareTo(other.consumerId);
    }
}
