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

import io.hawtjms.jms.message.JmsDefaultMessageFactory;
import io.hawtjms.jms.message.JmsInboundMessageDispatch;
import io.hawtjms.jms.message.JmsOutboundMessageDispatch;
import io.hawtjms.jms.meta.JmsResource;
import io.hawtjms.jms.meta.JmsSessionId;
import io.hawtjms.provider.AbstractAsyncProvider;
import io.hawtjms.provider.AsyncResult;
import io.hawtjms.provider.ProviderConstants.ACK_TYPE;
import io.hawtjms.transports.TcpTransport;
import io.hawtjms.transports.Transport;
import io.hawtjms.transports.TransportListener;

import java.io.IOException;
import java.net.URI;

import javax.jms.JMSException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.buffer.Buffer;

/**
 * Async Provider implementation for the STOMP protocol.
 */
public class StompProvider extends AbstractAsyncProvider implements TransportListener {

    private static final Logger LOG = LoggerFactory.getLogger(StompProvider.class);

    private Transport transport;

    public StompProvider(URI remoteURI) {
        super(remoteURI, new JmsDefaultMessageFactory());
    }

    @Override
    public void connect() throws IOException {
        checkClosed();

        transport = createTransport(getRemoteURI());
        transport.connect();
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
    }

    @Override
    public void create(JmsResource resource, AsyncResult<Void> request) throws IOException, JMSException, UnsupportedOperationException {
        // TODO Auto-generated method stub
    }

    @Override
    public void start(JmsResource resource, AsyncResult<Void> request) throws IOException {
        // TODO Auto-generated method stub
    }

    @Override
    public void destroy(JmsResource resourceId, AsyncResult<Void> request) throws IOException, JMSException, UnsupportedOperationException {
        // TODO Auto-generated method stub
    }

    @Override
    public void send(JmsOutboundMessageDispatch envelope, AsyncResult<Void> request) throws IOException, JMSException {
        // TODO Auto-generated method stub
    }

    @Override
    public void acknowledge(JmsSessionId sessionId, AsyncResult<Void> request) throws IOException {
        // TODO Auto-generated method stub
    }

    @Override
    public void acknowledge(JmsInboundMessageDispatch envelope, ACK_TYPE ackType, AsyncResult<Void> request) throws IOException {
        // TODO Auto-generated method stub
    }

    @Override
    public void commit(JmsSessionId sessionId, AsyncResult<Void> request) throws IOException, JMSException, UnsupportedOperationException {
        // TODO Auto-generated method stub
    }

    @Override
    public void rollback(JmsSessionId sessionId, AsyncResult<Void> request) throws IOException, JMSException, UnsupportedOperationException {
        // TODO Auto-generated method stub
    }

    @Override
    public void recover(JmsSessionId sessionId, AsyncResult<Void> request) throws IOException, UnsupportedOperationException {
        // TODO Auto-generated method stub
    }

    @Override
    public void unsubscribe(String subscription, AsyncResult<Void> request) throws IOException, JMSException, UnsupportedOperationException {
        // TODO Auto-generated method stub
    }

    /**
     * Provides an extension point for subclasses to insert other types of transports such
     * as SSL etc.
     *
     * @param remoteLocation
     *        The remote location where the transport should attempt to connect.
     *
     * @return the newly created transport instance.
     */
    protected Transport createTransport(URI remoteLocation) {
        return new TcpTransport(this, remoteLocation);
    }

    @Override
    public void onData(Buffer incoming) {
        // TODO Auto-generated method stub
    }

    /**
     * Callback method for the Transport to report connection errors.  When called
     * the method will queue a new task to fire the failure error back to the listener.
     *
     * @param error
     *        the error that causes the transport to fail.
     */
    @Override
    public void onTransportError(final Throwable error) {
        if (!closed.get()) {
            serializer.execute(new Runnable() {
                @Override
                public void run() {
                    LOG.info("Transport failed: {}", error.getMessage());
                    if (!closed.get()) {
                        fireProviderException(error);
                    }
                }
            });
        }
    }

    /**
     * Callback method for the Transport to report that the underlying connection
     * has closed.  When called this method will queue a new task that will check for
     * the closed state on this transport and if not closed then an exception is raied
     * to the registered ProviderListener to indicate connection loss.
     */
    @Override
    public void onTransportClosed() {
        if (!closed.get()) {
            serializer.execute(new Runnable() {
                @Override
                public void run() {
                    LOG.info("Transport connection remotely closed:");
                    if (!closed.get()) {
                        fireProviderException(new IOException("Connection remotely closed."));
                    }
                }
            });
        }
    }
}
