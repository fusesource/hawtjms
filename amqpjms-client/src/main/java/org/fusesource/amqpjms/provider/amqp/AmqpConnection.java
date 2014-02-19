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

import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.EngineFactory;
import org.apache.qpid.proton.engine.Transport;
import org.apache.qpid.proton.engine.impl.EngineFactoryImpl;
import org.apache.qpid.proton.engine.impl.ProtocolTracer;
import org.apache.qpid.proton.engine.impl.TransportImpl;
import org.apache.qpid.proton.framing.TransportFrame;
import org.fusesource.amqpjms.jms.meta.JmsConnectionInfo;
import org.fusesource.amqpjms.jms.meta.JmsResource;
import org.fusesource.amqpjms.provider.AsyncResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.buffer.Buffer;

public class AmqpConnection {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpConnection.class);
    private static final Logger TRACE_BYTES = LoggerFactory.getLogger(AmqpConnection.class.getPackage().getName() + ".BYTES");
    private static final Logger TRACE_FRAMES = LoggerFactory.getLogger(AmqpConnection.class.getPackage().getName() + ".FRAMES");

    private final EngineFactory engineFactory = new EngineFactoryImpl();
    private final Transport protonTransport = engineFactory.createTransport();
    private final Connection protonConnection = engineFactory.createConnection();

    private boolean trace;

    public AmqpConnection() {
        this.protonTransport.bind(this.protonConnection);
        updateTracer();
    }

    public void createConnection(JmsConnectionInfo connectionInfo, AsyncResult<JmsResource> result) throws IOException {
        result.onSuccess(connectionInfo);
    }

    public void destroyConnection(JmsConnectionInfo connectionInfo, AsyncResult<Void> result) throws IOException {
        result.onSuccess(null);
    }

    private void updateTracer() {
        if (isTrace()) {
            ((TransportImpl) protonTransport).setProtocolTracer(new ProtocolTracer() {
                @Override
                public void receivedFrame(TransportFrame transportFrame) {
                    TRACE_FRAMES.trace("RECV: {}", transportFrame.getBody());
                }

                @Override
                public void sentFrame(TransportFrame transportFrame) {
                    TRACE_FRAMES.trace("SENT: {}", transportFrame.getBody());
                }
            });
        }
    }

    void onAmqpData(Buffer input) {

    }

    void onTransportError(Throwable error) {
        LOG.info("Transport failed: {}", error.getMessage());
    }

    public void setTrace(boolean trace) {
        this.trace = trace;
    }

    public boolean isTrace() {
        return this.trace;
    }
}
