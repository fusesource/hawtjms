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
package io.hawtjms.provider.stomp;

import io.hawtjms.jms.meta.JmsConsumerId;
import io.hawtjms.jms.meta.JmsConsumerInfo;
import io.hawtjms.jms.meta.JmsSessionId;

/**
 * STOMP Consumer class used to manage the subscription process for
 * STOMP and handle the various ACK modes and mappings to JMS.
 */
public class StompConsumer {

    private final JmsConsumerInfo consumerInfo;
    private final StompSession session;

    /**
     * Create a new STOMP Consumer that maps a STOMP subscription to a JMS Framework
     * MessageConsumer.
     *
     * @param session
     *        the session that acts as this consumer's parent.
     * @param consumerInfo
     *        the information object that defines this consumer instance.
     */
    public StompConsumer(StompSession session, JmsConsumerInfo consumerInfo) {
        this.consumerInfo = consumerInfo;
        this.session = session;

        this.consumerInfo.getConsumerId().setProviderHint(this);
    }

    public JmsConsumerId getConsumerId() {
        return this.consumerInfo.getConsumerId();
    }

    public JmsSessionId getSessionId() {
        return this.consumerInfo.getParentId();
    }

    public StompSession getSession() {
        return this.session;
    }
}
