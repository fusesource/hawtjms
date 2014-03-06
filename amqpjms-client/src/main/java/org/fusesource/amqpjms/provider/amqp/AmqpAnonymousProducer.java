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
import org.fusesource.amqpjms.jms.meta.JmsProducerInfo;

/**
 * Handles the case of anonymous JMS MessageProducers.
 *
 * In order to simulate the anonymous producer we must create a sender for each message
 * send attempt and close it following a successful send.
 */
public class AmqpAnonymousProducer extends AbstractAmqpResource<JmsProducerInfo, Sender>{

    private final AmqpSession session;

    /**
     * @param info
     */
    public AmqpAnonymousProducer(AmqpSession session, JmsProducerInfo info) {
        super(info);
        this.session = session;
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

    public AmqpSession getSession() {
        return session;
    }
}
