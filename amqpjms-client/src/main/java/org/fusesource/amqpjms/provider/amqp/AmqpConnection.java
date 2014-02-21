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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Sasl;
import org.apache.qpid.proton.engine.Session;
import org.fusesource.amqpjms.jms.meta.JmsConnectionInfo;
import org.fusesource.amqpjms.jms.meta.JmsResource;
import org.fusesource.amqpjms.jms.meta.JmsSessionId;
import org.fusesource.amqpjms.jms.meta.JmsSessionInfo;
import org.fusesource.amqpjms.provider.ProviderRequest;
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
    private final AmqpProvider provider;

    private ProviderRequest<JmsResource> pendingConnect;

    private final Map<JmsSessionId, AmqpSession> pendingOpenSessions = new HashMap<JmsSessionId, AmqpSession>();
    private final Map<JmsSessionId, AmqpSession> pendingCloseSessions = new HashMap<JmsSessionId, AmqpSession>();

    public AmqpConnection(AmqpProvider provider, Connection protonConnection, Sasl sasl, JmsConnectionInfo info) {
        this.provider = provider;
        this.protonConnection = protonConnection;
        this.sasl = sasl;
        this.info = info;
        this.remoteURI = provider.getRemoteURI();

        this.protonConnection.setContainer(info.getClientId());
        this.protonConnection.setContext(this);
        this.protonConnection.setHostname(remoteURI.getHost());
        this.protonConnection.open();
        // TODO check info to see if we can meet all the requested options.
    }

    public void open(ProviderRequest<JmsResource> pendingConnect) {
        this.pendingConnect = pendingConnect;
    }

    public void close() {
        this.protonConnection.close();
    }

    public void createSession(JmsSessionInfo sessionInfo, ProviderRequest<JmsResource> request) {
        JmsSessionId sessionId = sessionInfo.getSessionId();
        AmqpSession pendingSession = new AmqpSession(this, sessionInfo);
        pendingSession.open(request);
        pendingOpenSessions.put(sessionId, pendingSession);
    }

    public void closeSession(JmsSessionInfo sessionInfo, ProviderRequest<Void> request) {
        JmsSessionId sessionId = sessionInfo.getSessionId();
        AmqpSession session = sessions.remove(sessionInfo.getSessionId());
        if (session != null) {
            session.close(request);
            pendingCloseSessions.put(sessionId, session);
        }
    }

    public void processUpdates() {

        LOG.info("Connection local {} and remote {} states",
                 protonConnection.getLocalState(), protonConnection.getRemoteState());

        // We are waiting for the Broker to answer our Connection open request.
        if (protonConnection.getLocalState() == EndpointState.ACTIVE &&
            protonConnection.getRemoteState() == EndpointState.ACTIVE) {

            LOG.info("Connection opened on Broker:");
            pendingConnect.onSuccess(this.info);
        }

        // We are opened and something on the remote end has closed us, signal an error.
        // TODO - need to figure out exactly what the failure states are.
        if (protonConnection.getLocalState() == EndpointState.ACTIVE &&
            protonConnection.getRemoteState() != EndpointState.ACTIVE) {

            if (protonConnection.getRemoteCondition().getCondition() != null) {
                LOG.info("Error condition detected on Connection open {}.",
                         protonConnection.getRemoteCondition().getCondition());

                String message = getRemoteErrorMessage();
                if (message == null) {
                    message = "Remote perr closed connection unexpectedly.";
                }

                if (pendingConnect != null) {
                    pendingConnect.onFailure(new IOException(message));
                } else {
                    provider.fireProviderException(new IOException(message));
                }
            }
        }

        processPendingSessions();

        for (AmqpSession session : this.sessions.values()) {
            session.processUpdates();
        }
    }

    private void processPendingSessions() {

        if (pendingOpenSessions.isEmpty() && pendingCloseSessions.isEmpty()) {
            return;
        }

        ArrayList<JmsSessionId> toRemove = new ArrayList<JmsSessionId>();
        for (Entry<JmsSessionId, AmqpSession> entry : pendingOpenSessions.entrySet()) {
            if (entry.getValue().isOpen()) {
                toRemove.add(entry.getKey());
                sessions.put(entry.getKey(), entry.getValue());
                entry.getValue().opened();
            }
        }

        for (JmsSessionId id : toRemove) {
            LOG.info("Session {} is now open", id);
            pendingOpenSessions.remove(id);
        }

        toRemove.clear();

        for (Entry<JmsSessionId, AmqpSession> entry : pendingCloseSessions.entrySet()) {
            if (entry.getValue().isClosed()) {
                toRemove.add(entry.getKey());
                entry.getValue().closed();
            }
        }

        for (JmsSessionId id : toRemove) {
            LOG.info("Session {} is now closed", id);
            pendingCloseSessions.remove(id);
        }
    }

    private String getRemoteErrorMessage() {
        if (protonConnection.getRemoteCondition() != null) {
            ErrorCondition error = protonConnection.getRemoteCondition();
            if (error.getDescription() != null && !error.getDescription().isEmpty()) {
                return error.getDescription();
            }
        }

        return null;
    }

    public JmsConnectionInfo getConnectionInfo() {
        return this.info;
    }

    public Connection getProtonConnection() {
        return this.protonConnection;
    }

    public URI getRemoteURI() {
        return this.remoteURI;
    }

    public String getUsername() {
        return this.info.getUsername();
    }

    public String getPassword() {
        return this.info.getPassword();
    }

    /**
     * Retrieve the indicated Session instance from the list of active sessions.
     *
     * @param sessionId
     *        The JmsSessionId that's associated with the target session.
     *
     * @return the AmqpSession associated with the given id.
     */
    public AmqpSession getSession(JmsSessionId sessionId) {
        // TODO - Hide a session reference in the sessionId hint field.

        return this.sessions.get(sessionId);
    }
}

