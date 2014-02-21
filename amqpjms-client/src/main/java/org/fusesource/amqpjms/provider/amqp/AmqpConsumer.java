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

import java.util.UUID;

import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Receiver;
import org.fusesource.amqpjms.jms.meta.JmsConsumerInfo;
import org.fusesource.amqpjms.jms.meta.JmsResource;
import org.fusesource.amqpjms.provider.ProviderRequest;

/**
 * AMQP Consumer object that is used to manage JMS MessageConsumer semantics.
 */
public class AmqpConsumer {

    private final AmqpSession session;
    private final JmsConsumerInfo info;
    private Receiver protonReceiver;

    private ProviderRequest<JmsResource> openRequest;
    private ProviderRequest<Void> closeRequest;

    public AmqpConsumer(AmqpSession session, JmsConsumerInfo info) {
        this.session = session;
        this.info = info;
    }

    public void open(ProviderRequest<JmsResource> request) {

        String subscription = info.getDestination().getName() + "->" + UUID.randomUUID().toString();

        Source source = new Source();
        source.setAddress(subscription);
        Target target = new Target();

        protonReceiver = session.getProtonSession().receiver(subscription);
        protonReceiver.setSource(source);
        protonReceiver.setTarget(target);
        protonReceiver.setContext(this);
        protonReceiver.setSenderSettleMode(SenderSettleMode.UNSETTLED);
        protonReceiver.setReceiverSettleMode(ReceiverSettleMode.FIRST);

        this.openRequest = request;
    }

    public boolean isOpen() {
        return this.protonReceiver.getRemoteState() == EndpointState.ACTIVE;
    }

    public void opened() {
        if (openRequest != null) {
            openRequest.onSuccess(info);
            openRequest = null;
        }
    }

    public void close(ProviderRequest<Void> request) {
        this.protonReceiver.close();
        this.closeRequest = request;
    }

    public boolean isClosed() {
        return this.protonReceiver.getRemoteState() == EndpointState.CLOSED;
    }

    public void closed() {
        if (closeRequest != null) {
            closeRequest.onSuccess(null);
            closeRequest = null;
        }
    }

    public AmqpSession getSession() {
        return this.session;
    }

    public Receiver getProtonReceiver() {
        return this.protonReceiver;
    }
}
