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

import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.JMSSecurityException;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Sasl;
import org.fusesource.amqpjms.jms.JmsDestination;
import org.fusesource.amqpjms.jms.meta.JmsConnectionInfo;
import org.fusesource.amqpjms.jms.meta.JmsResource;
import org.fusesource.amqpjms.jms.meta.JmsSessionId;
import org.fusesource.amqpjms.jms.meta.JmsSessionInfo;
import org.fusesource.amqpjms.provider.AsyncResult;
import org.fusesource.amqpjms.util.IOExceptionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpConnection extends AbstractAmqpResource<JmsConnectionInfo, Connection> {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpConnection.class);

    private final URI remoteURI;
    private final Map<JmsSessionId, AmqpSession> sessions = new HashMap<JmsSessionId, AmqpSession>();
    private final Map<JmsDestination, AmqpTemporaryDestination> tempDests = new HashMap<JmsDestination, AmqpTemporaryDestination>();
    private final AmqpProvider provider;
    private boolean connected;
    private AmqpSaslAuthenticator authenticator;
    private final AmqpSession connectionSession;

    private final List<AmqpResource> pendingOpen = new LinkedList<AmqpResource>();
    private final List<AmqpResource> pendingClose = new LinkedList<AmqpResource>();

    private String queuePrefix;
    private String topicPrefix;
    private String tempQueuePrefix;
    private String tempTopicPrefix;

    public AmqpConnection(AmqpProvider provider, Connection protonConnection, Sasl sasl, JmsConnectionInfo info) {
        super(info, protonConnection);

        this.provider = provider;
        this.remoteURI = provider.getRemoteURI();

        if (sasl != null) {
            this.authenticator = new AmqpSaslAuthenticator(sasl, info);
        }

        this.info.getConnectionId().setProviderHint(this);

        this.queuePrefix = info.getQueuePrefix();
        this.topicPrefix = info.getTopicPrefix();
        this.tempQueuePrefix = info.getTempQueuePrefix();
        this.tempTopicPrefix = info.getTempTopicPrefix();

        // TODO check info to see if we can meet all the requested options.

        // Create a Session for this connection that is used for Temporary Destinations
        // and perhaps later on management and advisory monitoring.
        JmsSessionInfo sessionInfo = new JmsSessionInfo(this.info, -1);
        this.connectionSession = new AmqpSession(this, sessionInfo);
    }

    @Override
    protected void doOpen() {
        this.endpoint.setContainer(info.getClientId());
        this.endpoint.setHostname(remoteURI.getHost());
    }

    @Override
    protected void doClose() {
    }

    public AmqpSession createSession(JmsSessionInfo sessionInfo) {
        AmqpSession session = new AmqpSession(this, sessionInfo);
        return session;
    }

    public AmqpTemporaryDestination createTemporaryDestination(JmsDestination destination) {
        AmqpTemporaryDestination temporary = new AmqpTemporaryDestination(connectionSession, destination);
        return temporary;
    }

    @Override
    public void processUpdates() {

        processSaslHandshake();

        if (!connected && isOpen()) {
            connectionSession.open(new AsyncResult<JmsResource>() {

                @Override
                public void onSuccess(JmsResource result) {
                    LOG.debug("AMQP Connection Session opened: {}", result);
                    opened();
                    connected = true;
                }

                @Override
                public void onFailure(Throwable result) {
                    LOG.debug("AMQP Connection Session failed to open.");
                    failed(IOExceptionSupport.create(result));
                }
            });
        }

        // We are opened and something on the remote end has closed us, signal an error.
        // TODO - need to figure out exactly what the failure states are.
        if (endpoint.getLocalState() == EndpointState.ACTIVE &&
            endpoint.getRemoteState() != EndpointState.ACTIVE) {

            if (endpoint.getRemoteCondition().getCondition() != null) {
                LOG.info("Error condition detected on Connection open {}.", endpoint.getRemoteCondition().getCondition());

                String message = getRemoteErrorMessage();
                if (message == null) {
                    message = "Remote perr closed connection unexpectedly.";
                }

                Throwable thrown = null;
                Symbol error = endpoint.getRemoteCondition().getCondition();
                if (error.equals(AmqpError.UNAUTHORIZED_ACCESS)) {
                    thrown = new JMSSecurityException(message);
                } else {
                    thrown = new JMSException(message);
                }

                if (openRequest != null) {
                    openRequest.onFailure(thrown);
                } else {
                    provider.fireProviderException(thrown);
                }
            }
        }

        processPendingResources();

        for (AmqpSession session : this.sessions.values()) {
            session.processUpdates();
        }

        for (AmqpTemporaryDestination tempDest : this.tempDests.values()) {
            tempDest.processUpdates();
        }

        // Transition cleanly to closed state.
        if (connected && endpoint.getRemoteState() == EndpointState.CLOSED) {
            closed();
        }
    }

    private void processSaslHandshake() {

        if (connected || authenticator == null) {
            return;
        }

        try {
            if (authenticator.authenticate()) {
                authenticator = null;
            }
        } catch (JMSSecurityException ex) {
            failed(ex);
        }
    }

    private void processPendingResources() {

        if (pendingOpen.isEmpty() && pendingClose.isEmpty()) {
            return;
        }

        Iterator<AmqpResource> iterator = pendingOpen.iterator();
        while (iterator.hasNext()) {
            AmqpResource resource = iterator.next();
            if (resource.isOpen()) {
                if (resource instanceof AmqpSession) {
                    AmqpSession session = (AmqpSession) resource;
                    sessions.put(session.getSessionId(), session);
                    LOG.info("Session {} is now open", session.getSessionId());
                } else if (resource instanceof AmqpTemporaryDestination) {
                    AmqpTemporaryDestination destination = (AmqpTemporaryDestination) resource;
                    tempDests.put(destination.getJmsDestination(), destination);
                    LOG.info("Temporary Destination {} is now open", destination);
                }

                iterator.remove();
                resource.opened();
            }
        }

        iterator = pendingClose.iterator();
        while (iterator.hasNext()) {
            AmqpResource resource = iterator.next();
            if (resource.isClosed()) {
                if (resource instanceof AmqpSession) {
                    AmqpSession session = (AmqpSession) resource;
                    sessions.remove(session.getSessionId());
                    LOG.info("Session {} is now closed", session.getSessionId());
                } else if (resource instanceof AmqpTemporaryDestination) {
                    AmqpTemporaryDestination destination = (AmqpTemporaryDestination) resource;
                    tempDests.remove(destination.getJmsDestination());
                    LOG.info("Temporary Destination {} is now closed", destination);
                }

                iterator.remove();
                resource.closed();
            }
        }
    }

    private String getRemoteErrorMessage() {
        if (endpoint.getRemoteCondition() != null) {
            ErrorCondition error = endpoint.getRemoteCondition();
            if (error.getDescription() != null && !error.getDescription().isEmpty()) {
                return error.getDescription();
            }
        }

        return null;
    }

    void addToPendingOpen(AmqpResource session) {
        this.pendingOpen.add(session);
    }

    void addToPendingClose(AmqpResource session) {
        this.pendingClose.add(session);
    }

    public JmsConnectionInfo getConnectionInfo() {
        return this.info;
    }

    public Connection getProtonConnection() {
        return this.endpoint;
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

    public AmqpProvider getProvider() {
        return this.provider;
    }

    public String getQueuePrefix() {
        return queuePrefix;
    }

    public void setQueuePrefix(String queuePrefix) {
        this.queuePrefix = queuePrefix;
    }

    public String getTopicPrefix() {
        return topicPrefix;
    }

    public void setTopicPrefix(String topicPrefix) {
        this.topicPrefix = topicPrefix;
    }

    public String getTempQueuePrefix() {
        return tempQueuePrefix;
    }

    public void setTempQueuePrefix(String tempQueuePrefix) {
        this.tempQueuePrefix = tempQueuePrefix;
    }

    public String getTempTopicPrefix() {
        return tempTopicPrefix;
    }

    public void setTempTopicPrefix(String tempTopicPrefix) {
        this.tempTopicPrefix = tempTopicPrefix;
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
        if (sessionId.getProviderHint() instanceof AmqpSession) {
            return (AmqpSession) sessionId.getProviderHint();
        }
        return this.sessions.get(sessionId);
    }
}

