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

import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Session;
import org.fusesource.amqpjms.jms.meta.JmsResource;
import org.fusesource.amqpjms.jms.meta.JmsSessionId;
import org.fusesource.amqpjms.jms.meta.JmsSessionInfo;
import org.fusesource.amqpjms.provider.ProviderResponse;

public class AmqpSession {

    private final AmqpConnection connection;
    private final JmsSessionInfo info;
    private Session protonSession;

    private ProviderResponse<JmsResource> openRequest;
    private ProviderResponse<Void> closeRequest;

    public AmqpSession(AmqpConnection connection, JmsSessionInfo info) {
        this.connection = connection;
        this.info = info;
    }

    public void open(ProviderResponse<JmsResource> request) {
        this.protonSession = connection.getProtonConnection().session();
        this.protonSession.setContext(this);
        this.protonSession.open();
        this.openRequest = request;
    }

    public boolean isOpen() {
        return this.protonSession.getRemoteState() == EndpointState.ACTIVE;
    }

    public void opened() {
        if (openRequest != null) {
            openRequest.onSuccess(info);
            openRequest = null;
        }
    }

    public void close(ProviderResponse<Void> request) {
        this.protonSession.close();
        this.closeRequest = request;
    }

    public boolean isClosed() {
        return this.protonSession.getRemoteState() == EndpointState.CLOSED;
    }

    public void closed() {
        if (closeRequest != null) {
            closeRequest.onSuccess(null);
            closeRequest = null;
        }
    }

    public AmqpConnection getConnection() {
        return this.connection;
    }

    public JmsSessionId getSessionId() {
        return this.info.getSessionId();
    }

    public Session getProtonSession() {
        return this.protonSession;
    }
}
