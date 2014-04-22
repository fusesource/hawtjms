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
import static io.hawtjms.provider.stomp.StompConstants.INVALID_SELECTOR_EXCEPTION;
import static io.hawtjms.provider.stomp.StompConstants.JMS_SECURITY_EXCEPTION;
import static io.hawtjms.provider.stomp.StompConstants.LOGIN;
import static io.hawtjms.provider.stomp.StompConstants.MESSAGE;
import static io.hawtjms.provider.stomp.StompConstants.PASSCODE;
import static io.hawtjms.provider.stomp.StompConstants.RECEIPT;
import static io.hawtjms.provider.stomp.StompConstants.RECEIPT_ID;
import static io.hawtjms.provider.stomp.StompConstants.RECEIPT_REQUESTED;
import static io.hawtjms.provider.stomp.StompConstants.SECURITY_EXCEPTION;
import static io.hawtjms.provider.stomp.StompConstants.SERVER;
import static io.hawtjms.provider.stomp.StompConstants.SESSION;
import static io.hawtjms.provider.stomp.StompConstants.STOMP;
import static io.hawtjms.provider.stomp.StompConstants.SUBSCRIPTION;
import static io.hawtjms.provider.stomp.StompConstants.VERSION;
import io.hawtjms.jms.meta.JmsConnectionId;
import io.hawtjms.jms.meta.JmsConnectionInfo;
import io.hawtjms.jms.meta.JmsConsumerId;
import io.hawtjms.jms.meta.JmsProducerId;
import io.hawtjms.jms.meta.JmsSessionId;
import io.hawtjms.jms.meta.JmsSessionInfo;
import io.hawtjms.provider.AsyncResult;
import io.hawtjms.provider.stomp.adapters.GenericStompServerAdaptor;
import io.hawtjms.provider.stomp.adapters.StompServerAdapter;
import io.hawtjms.provider.stomp.adapters.StompServerAdapterFactory;
import io.hawtjms.provider.stomp.message.StompJmsMessageFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.jms.InvalidClientIDException;
import javax.jms.InvalidSelectorException;
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

    private final StompJmsMessageFactory messageFactory;
    private final Map<JmsSessionId, StompSession> sessions = new HashMap<JmsSessionId, StompSession>();
    private final Map<String, AsyncResult<Void>> requests = new HashMap<String, AsyncResult<Void>>();
    private final JmsConnectionInfo connectionInfo;
    private final StompProvider provider;

    private StompServerAdapter serverAdapter;
    private AsyncResult<Void> pendingConnect;
    private boolean connected;
    private long requestCounter;
    private String remoteSessionId;
    private String version;
    private String remoteServerId;
    private String queuePrefix;
    private String topicPrefix;
    private String tempQueuePrefix;
    private String tempTopicPrefix;

    /**
     * Create a new instance of the StompConnection.
     *
     * @param connectionInfo
     *        The connection information used to create this StompConnection.
     */
    public StompConnection(StompProvider provider, JmsConnectionInfo connectionInfo) {
        this.connectionInfo = connectionInfo;
        this.provider = provider;
        this.messageFactory = new StompJmsMessageFactory(this);
        this.tempQueuePrefix = connectionInfo.getTempQueuePrefix();
        this.tempTopicPrefix = connectionInfo.getTempTopicPrefix();
        this.queuePrefix = connectionInfo.getQueuePrefix();
        this.topicPrefix = connectionInfo.getTopicPrefix();

        connectionInfo.getConnectionId().setProviderHint(this);
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
     * @throws IOException if a connection attempt is in-progress or already connected.
     */
    public void connect(AsyncResult<Void> request) throws IOException {
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

        provider.send(connect);
    }

    /**
     * Creates a new logical session instance.  STOMP really doesn't support multiple
     * sessions so our session is just a logical container for the JMS resources to
     * reside in.
     *
     * @param sessionInfo
     *        the session information used to create the session instance.
     * @param request
     *        the asynchronous request that is waiting for this action to complete.
     *
     * @throws IOException if a session with the same Id value already exists.
     */
    public void createSession(JmsSessionInfo sessionInfo, AsyncResult<Void> request) throws IOException {
        if (this.sessions.containsKey(sessionInfo.getSessionId())) {
            throw new IOException("A Session with the given ID already exists.");
        }

        StompSession session = new StompSession(this, sessionInfo);
        sessions.put(sessionInfo.getSessionId(), session);
        request.onSuccess();
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
            return;
        }

        if (frame.getCommand().equals(MESSAGE)) {
            processMessage(frame);
        } else if (frame.getCommand().equals(RECEIPT)) {
            processReceipt(frame);
        } else if (frame.getCommand().equals(ERROR)) {
            processError(frame);
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

            String server = frame.getProperty(SERVER);
            if (server != null) {
                serverAdapter = StompServerAdapterFactory.create(this, server);
            } else {
                serverAdapter = new GenericStompServerAdaptor(this);
            }

            version = frame.getProperty(VERSION);
            if (version == null || !DEFAULT_ACCEPT_VERSIONS.contains(version)) {
                throw new IOException("Cannot connect to remote peer, version not supported: " + version);
            }

            LOG.info("Using STOMP server adapter: {}", serverAdapter.getServerName());

            connected = true;
            pendingConnect.onSuccess();
            pendingConnect = null;
        } else {
            throw new IOException("Received Unexpected frame during connect: " + frame.getCommand());
        }
    }

    /**
     * Handles an incoming MESSAGE frame.  The Frame is inspected for it's ID header and that
     * value is turned into a JmsConsumerId which should allow us to locate the consumer that
     * is subscribed for the destination the message was sent to.
     *
     * @param message
     *        the incoming message frame.
     *
     * @throws JMSException if an error occurs while dispatching the incoming message.
     */
    protected void processMessage(StompFrame message) throws JMSException {
        String id = message.getProperty(SUBSCRIPTION);
        if (id == null) {
            provider.fireProviderException(new IOException("Invalid Message frame received, no ID."));
        }

        JmsConsumerId consumerId = new JmsConsumerId(id);
        StompConsumer consumer = getConsumer(consumerId);
        if (consumer != null) {
            consumer.processMessage(message);
        } else {
            LOG.debug("Received message for a consumer that doesn't exist.");
        }
    }

    /**
     * Handle incoming RECEIPT frames from the broker by triggering the onSuccess callback
     * for the associated AsyncResult instance awaiting an answer.
     *
     * @param receiptFrame
     *        the receipt frame to process.
     */
    protected void processReceipt(StompFrame receiptFrame) {

        String receipt = receiptFrame.getProperty(RECEIPT_ID);
        if (receipt == null || receipt.isEmpty()) {
            provider.fireProviderException(new IOException("Invalid Receipt frame received."));
        }

        AsyncResult<Void> request = requests.remove(receipt);
        if (request == null) {
            LOG.warn("received receipt for unknown request: " + receipt);
        }

        request.onSuccess();
    }

    /**
     * Handle STOMP ERROR frames by first checking for a pending request that
     * matches the optional receipt Id in the ERROR frame and if that doesn't
     * happen then fire an exception to the provider listener.
     *
     * @param errorFrame
     *        the ERROR frame to process.
     */
    protected void processError(StompFrame errorFrame) {

        JMSException error = exceptionFromErrorFrame(errorFrame);
        String receipt = errorFrame.getProperty(RECEIPT_ID);

        if (receipt != null && !receipt.isEmpty()) {
            AsyncResult<Void> request = requests.remove(receipt);
            if (request != null) {
                request.onFailure(error);
                return;
            }
        }

        provider.fireProviderException(error);
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
     * Search for the StompProducer with the given Id in this Connection's open
     * resources.
     *
     * @param producerId
     *        the Id of the StompProducer to lookup.
     *
     * @return the producer with the given Id or null if no such producer exists.
     */
    public StompProducer getProducer(JmsProducerId producerId) {
        StompProducer producer = null;
        if (producerId.getProviderHint() instanceof StompProducer) {
            producer = (StompProducer) producerId.getProviderHint();
        } else {
            StompSession session = getSession(producerId.getParentId());
            producer = session.getProducer(producerId);
        }

        return producer;
    }

    /**
     * Search for the StompConsumer with the given Id in this Connection's open
     * resources.
     *
     * @param consumerId
     *        the Id of the StompConsumer to lookup.
     *
     * @return the consumer with the given Id or null if no such consumer exists.
     */
    public StompConsumer getConsumer(JmsConsumerId consumerId) {
        StompConsumer consumer = null;
        if (consumerId.getProviderHint() instanceof StompConsumer) {
            consumer = (StompConsumer) consumerId.getProviderHint();
        } else {
            StompSession session = getSession(consumerId.getParentId());
            consumer = session.getConsumer(consumerId);
        }

        return consumer;
    }

    /**
     * Sends the given STOMP frame without adding any additional properties or requesting
     * a RECEIPT message for the frame.
     *
     * @param frame
     *        the frame to send.
     *
     * @throws IOException if an error occurs while sending the frame.
     */
    public void send(StompFrame frame) throws IOException {
        provider.send(frame);
    }

    /**
     * Sends a StompFrame with supplied receipt Id which will result in the server
     * sending a RECEIPT frame back once the request has been successfully processed,
     * or an ERROR frame with the receipt Id set as a header if the request fails.
     *
     * @param frame
     *        the frame to send as a request.
     * @param request
     *        the AsyncResult to signal once the request operation is completed.
     *
     * @throws IOException if an error occurs while sending the request frame.
     */
    public void request(StompFrame frame, AsyncResult<Void> request) throws IOException {
        String receiptId = String.valueOf(getNextRequestId());
        frame.setProperty(RECEIPT_REQUESTED, receiptId);
        requests.put(receiptId, request);

        provider.send(frame);
    }

    /**
     * AsnycResult class used to handle STOMP RECEIPT frames which complete
     * some STOMP operation.  Other STOMP classes can extend this to implement
     * custom logic on completion of the asynchronous operation they initiated.
     *
     * @param <T>
     */
    protected class ReceiptHandler implements AsyncResult<Void> {

        private final AsyncResult<Void> pending;

        public ReceiptHandler(AsyncResult<Void> pending) {
            this.pending = pending;
        }

        @Override
        public boolean isComplete() {
            return pending.isComplete();
        }

        @Override
        public void onFailure(Throwable result) {
            pending.onFailure(result);
        }

        @Override
        public void onSuccess(Void result) {
            pending.onSuccess(result);
        }

        @Override
        public void onSuccess() {
            onSuccess(null);
        }
    }

    //----------- Property Getters and Setters -------------------------------//

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

    public String getUsername() {
        return connectionInfo.getUsername();
    }

    public String getPassword() {
        return connectionInfo.getPassword();
    }

    public StompProvider getProvider() {
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

    public StompServerAdapter getServerAdapter() {
        return this.serverAdapter;
    }

    //---------- Internal utilities ------------------------------------------//

    protected long getNextRequestId() {
        return requestCounter++;
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
        } else if (errorDetail.contains(INVALID_SELECTOR_EXCEPTION)) {
            exception = new InvalidSelectorException(frame.getErrorMessage());
        } else if (errorDetail.contains(JMS_SECURITY_EXCEPTION)) {
            exception = new JMSSecurityException(frame.getErrorMessage());
        } else if (errorDetail.contains(SECURITY_EXCEPTION)) {
            exception = new JMSSecurityException(frame.getErrorMessage());
        } else {
            exception = new JMSException(frame.getErrorMessage());
        }

        return exception;
    }

    /**
     * @return the STOMP based JmsMessageFactory for this Connection.
     */
    public StompJmsMessageFactory getMessageFactory() {
        return this.messageFactory;
    }
}
