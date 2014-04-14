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

import static io.hawtjms.provider.stomp.StompConstants.ACCEPT_VERSION;
import static io.hawtjms.provider.stomp.StompConstants.CLIENT_ID;
import static io.hawtjms.provider.stomp.StompConstants.CONNECTED;
import static io.hawtjms.provider.stomp.StompConstants.ERROR;
import static io.hawtjms.provider.stomp.StompConstants.HEARTBEAT;
import static io.hawtjms.provider.stomp.StompConstants.HOST;
import static io.hawtjms.provider.stomp.StompConstants.INVALID_CLIENTID_EXCEPTION;
import static io.hawtjms.provider.stomp.StompConstants.JMS_SECURITY_EXCEPTION;
import static io.hawtjms.provider.stomp.StompConstants.LOGIN;
import static io.hawtjms.provider.stomp.StompConstants.PASSCODE;
import static io.hawtjms.provider.stomp.StompConstants.SECURITY_EXCEPTION;
import static io.hawtjms.provider.stomp.StompConstants.SESSION;
import static io.hawtjms.provider.stomp.StompConstants.STOMP;
import io.hawtjms.jms.meta.JmsConnectionId;
import io.hawtjms.jms.meta.JmsConnectionInfo;
import io.hawtjms.jms.meta.JmsSessionId;
import io.hawtjms.jms.meta.JmsSessionInfo;
import io.hawtjms.provider.AsyncResult;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.jms.InvalidClientIDException;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The STOMP Connection instance, all resources reside under this class.
 */
public class StompConnection {

    private static final Logger LOG = LoggerFactory.getLogger(StompConnection.class);

    /**
     * We currently restrict accepted versions to v1.1 and v1.2 which are nearly identical
     * and can map quite easily to JMS.  We could later add support for v1.0 but it doesn't
     * lend itself to JMS mapping as easily so for now we don't support it.
     */
    private static final String DEFAULT_ACCEPT_VERSIONS = "1.1,1.2";

    private final Map<JmsSessionId, StompSession> sessions = new HashMap<JmsSessionId, StompSession>();
    private final JmsConnectionInfo connectionInfo;

    private AsyncResult<Void> pendingConnect;
    private final boolean connected = false;
    private final StompProvider provider;
    private String remoteSessionId;
    private String version;
    private String remoteServerId;

    /**
     * Create a new instance of the StompConnection.
     *
     * @param connectionInfo
     *        The connection information used to create this StompConnection.
     */
    public StompConnection(StompProvider provider, JmsConnectionInfo connectionInfo) {
        this.connectionInfo = connectionInfo;
        this.provider = provider;
    }

    /**
     * Initiates a Connection by generating a new StompFrame that contains all
     * the properties which define the configured Connection.  The StompConnection
     * is not considered connected until it has received a CONNECTED frame back
     * indicating that it's connection request was accepted.
     *
     * @param request
     *        the async request object that awaits the connection complete step.
     *
     * @returns a new StompFrame containing the configured CONNECT command.
     *
     * @throws IOException if a connection attempt is in-progress or already connected.
     */
    public StompFrame connect(AsyncResult<Void> request) throws IOException {
        if (connected || pendingConnect != null) {
            throw new IOException("The connection is already established");
        }

        this.pendingConnect = request;

        StompFrame connect = new StompFrame(STOMP);
        connect.setProperty(ACCEPT_VERSION, DEFAULT_ACCEPT_VERSIONS);
        if (connectionInfo.getUsername() != null && !connectionInfo.getUsername().isEmpty()) {
            connect.setProperty(LOGIN, connectionInfo.getUsername());
        }
        if (connectionInfo.getPassword() != null && !connectionInfo.getPassword().isEmpty()) {
            connect.setProperty(PASSCODE, connectionInfo.getPassword());
        }
        if (!connectionInfo.isOmitHost()) {
            connect.setProperty(HOST, provider.getRemoteURI().getHost());
        }
        connect.setProperty(CLIENT_ID, connectionInfo.getClientId());
        connect.setProperty(HEARTBEAT, "0,0");  // TODO - Implement Heart Beat support.

        return connect;
    }

    /**
     * Creates a new logical session instance.  STOMP really doesn't support multiple
     * sessions so our session is just a logical container for the JMS resources to
     * reside in.
     *
     * @param sessionInfo
     *        the session information used to create the session instance.
     *
     * @return a new StompSession instance.
     *
     * @throws IOException if a session with the same Id value already exists.
     */
    public StompSession createSession(JmsSessionInfo sessionInfo) throws IOException {
        if (this.sessions.containsKey(sessionInfo.getSessionId())) {
            throw new IOException("A Session with the given ID already exists.");
        }

        StompSession session = new StompSession(sessionInfo);
        sessions.put(sessionInfo.getSessionId(), session);
        return session;
    }

    /**
     * Handle all newly received StompFrames and update Connection resources with the
     * new data.
     *
     * @param frame
     *        a newly received StompFrame.
     *
     * @throws JMSException if a JMS related error occurs.
     * @throws IOException if an error occurs while handling the new StompFrame.
     */
    public void processFrame(StompFrame frame) throws JMSException, IOException {
        if (pendingConnect != null) {
            processConnect(frame);
        }
    }

    private void processConnect(StompFrame frame) throws JMSException, IOException {
        if (pendingConnect != null && frame.getCommand().equals(ERROR)) {
            LOG.info("Connection attempt failed: {}", frame.getErrorMessage());
            pendingConnect.onFailure(exceptionFromErrorFrame(frame));
        }

        if (frame.getCommand().equals(CONNECTED)) {
            if (this.pendingConnect == null) {
                provider.fireProviderException(new IOException("Unexpected CONNECTED frame received."));
            }

            LOG.debug("Received new CONNCETED frame from Broker: {}", frame);

            String sessionId = frame.getProperty(SESSION);
            if (sessionId != null) {
                LOG.debug("Broker assigned this connection an id: {}", sessionId);
                remoteSessionId = sessionId;
            }

            this.pendingConnect.onSuccess();
        } else {
            throw new IOException("Received Unexpected frame during connect: " + frame.getCommand());
        }
    }

    /**
     * @return the unique Connection Id for this STOMP Connection.
     */
    public JmsConnectionId getConnectionId() {
        return this.connectionInfo.getConnectionId();
    }

    /**
     * Search for the StompSession with the given Id in this Connection's open
     * sessions.
     *
     * @param sessionId
     *        the Id of the StompSession to lookup.
     *
     * @return the session with the given Id or null if no such session exists.
     */
    public StompSession getSession(JmsSessionId sessionId) {
        return this.sessions.get(sessionId);
    }

    /**
     * @return true if this StompConnection is fully connected.
     */
    public boolean isConnected() {
        return connected;
    }

    /**
     * @return the remote session Id that the remote peer assigned this connection.
     */
    public String getRemoteSessionId() {
        return this.remoteSessionId;
    }

    /**
     * @return the version string returned by the remote peer.
     */
    public String getVersion() {
        return this.version;
    }

    /**
     * @return the server string returned by the remote peer.
     */
    public String getRemoteServerId() {
        return this.remoteServerId;
    }

    protected void checkConnected() throws IOException {
        if (!connected) {
            throw new IOException("already connected");
        }
    }

    protected JMSException exceptionFromErrorFrame(StompFrame frame) {
        JMSException exception = null;
        String errorDetail = "";
        try {
            errorDetail = frame.getContentAsString();
        } catch (Exception ex) {
        }

        // Lets not search overly large exception stacks, if the exception name
        // isn't in the first bit of the string then it's probably not there at all.
        errorDetail.substring(0, Math.min(100, errorDetail.length()));

        if (errorDetail.contains(INVALID_CLIENTID_EXCEPTION)) {
            exception = new InvalidClientIDException(frame.getErrorMessage());
        } else if (errorDetail.contains(JMS_SECURITY_EXCEPTION)) {
            exception = new JMSSecurityException(frame.getErrorMessage());
        } else if (errorDetail.contains(SECURITY_EXCEPTION)) {
            exception = new JMSSecurityException(frame.getErrorMessage());
        } else {
            exception = new JMSException(frame.getErrorMessage());
        }

        return exception;
    }
}
