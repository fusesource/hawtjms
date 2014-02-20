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
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Sasl;
import org.apache.qpid.proton.engine.Session;
import org.fusesource.amqpjms.jms.meta.JmsConnectionInfo;
import org.fusesource.amqpjms.jms.meta.JmsResource;
import org.fusesource.amqpjms.jms.meta.JmsSessionId;
import org.fusesource.amqpjms.jms.meta.JmsSessionInfo;
import org.fusesource.amqpjms.provider.ProviderResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpConnection {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpConnection.class);

    private final Connection protonConnection;
    private final JmsConnectionInfo info;
    private final URI remoteURI;
    private final Sasl sasl;
    private final Map<JmsSessionId, AmqpSession> sessions = new HashMap<JmsSessionId, AmqpSession>();
    private final Map<JmsSessionId, Session> pendingSessions = new HashMap<JmsSessionId, Session>();

    private ProviderResponse<JmsResource> pendingConnect;

    public AmqpConnection(URI remoteURI, Connection protonConnection, Sasl sasl, JmsConnectionInfo info) {
        this.protonConnection = protonConnection;
        this.sasl = sasl;
        this.info = info;
        this.remoteURI = remoteURI;

        this.protonConnection.setContainer(info.getClientId());
        this.protonConnection.setContext(this);
        this.protonConnection.setHostname(remoteURI.getHost());
        this.protonConnection.open();

        LOG.info("After open() call, state is {}", protonConnection.getLocalState());

        // TODO check info to see if we can meet all the requested options.
    }

    public void open(ProviderResponse<JmsResource> pendingConnect) {
        this.pendingConnect = pendingConnect;
    }

    public JmsConnectionInfo getConnectionInfo() {
        return this.info;
    }

    public void close() {
        this.protonConnection.close();
    }

    public Connection getProtonConnection() {
        return this.protonConnection;
    }

    public URI getRemoteURI() {
        return this.remoteURI;
    }

    public Session createSession(JmsSessionInfo sessionInfo) {
        Session session = this.protonConnection.session();
        return session;
    }

    public void processUpdates() {

        LOG.info("Connection local {} and remote {} states",
                 protonConnection.getLocalState(), protonConnection.getRemoteState());

        // We are waiting for the Broker to answer our Connection open request.
        if (protonConnection.getLocalState() != EndpointState.CLOSED &&
            protonConnection.getRemoteState() != EndpointState.ACTIVE) {

            LOG.info("Connection opened on Broker:");
            pendingConnect.onSuccess(this.info);
        }

        // We are opened and something on the remote end has closed us, singal an error.
        if (protonConnection.getLocalState() != EndpointState.ACTIVE &&
            protonConnection.getRemoteState() == EndpointState.CLOSED) {
            LOG.info("Connection remotely closed on Broker:");
            if (pendingConnect != null) {
                pendingConnect.onFailure(new IOException("Connection closed by remote Broker."));
            } else {
                // TODO - Fire an exception to the JMS layer indicating something went wrong.
            }
        }
    }
}
