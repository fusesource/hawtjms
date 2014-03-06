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

import org.apache.qpid.proton.engine.Sender;
import org.fusesource.amqpjms.jms.message.JmsOutboundMessageDispatch;
import org.fusesource.amqpjms.jms.meta.JmsProducerId;
import org.fusesource.amqpjms.jms.meta.JmsProducerInfo;
import org.fusesource.amqpjms.provider.AsyncResult;

/**
 * Base class for Producer instances.
 */
public abstract class AmqpProducer extends AbstractAmqpResource<JmsProducerInfo, Sender> implements AmqpLink {

    protected final AmqpSession session;

    public AmqpProducer(AmqpSession session, JmsProducerInfo info) {
        super(info);
        this.session = session;

        // Add a shortcut back to this Producer for quicker lookup.
        this.info.getProducerId().setProviderHint(this);
    }

    public abstract void send(JmsOutboundMessageDispatch envelope, AsyncResult<Void> request) throws IOException;

    /**
     * @return the JmsProducerId that was assigned to this AmqpProducer.
     */
    public JmsProducerId getProducerId() {
        return this.info.getProducerId();
    }
}
