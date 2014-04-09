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
package io.hawtjms.transports;

import io.hawtjms.util.IOExceptionSupport;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.VertxFactory;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.net.NetClient;
import org.vertx.java.core.net.NetSocket;

/**
 * Vertex based TCP transport for raw data packets.
 */
public class TcpTransport implements Transport {

    private static final Logger LOG = LoggerFactory.getLogger(TcpTransport.class);

    private final Vertx vertx = VertxFactory.newVertx();
    private final NetClient client = vertx.createNetClient();
    private final TransportListener listener;
    private final URI remoteLocation;
    private final AtomicBoolean connected = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<Throwable> connectionError = new AtomicReference<Throwable>();

    private NetSocket socket;

    /**
     * Create a new instance of the transport.
     *
     * @param listener
     *        The TransportListener that will receive data from this Transport instance.
     * @param remoteLocation
     *        The remote location where this transport should connection to.
     */
    public TcpTransport(TransportListener listener, URI remoteLocation) {
        this.listener = listener;
        this.remoteLocation = remoteLocation;
    }

    @Override
    public void connect() throws IOException {
        final CountDownLatch connectLatch = new CountDownLatch(1);

        configureNetClient(client);

        try {
            client.connect(remoteLocation.getPort(), remoteLocation.getHost(), new AsyncResultHandler<NetSocket>() {
                @Override
                public void handle(AsyncResult<NetSocket> asyncResult) {
                    if (asyncResult.succeeded()) {
                        socket = asyncResult.result();
                        LOG.info("We have connected! Socket is {}", socket);

                        connected.set(true);
                        connectLatch.countDown();

                        socket.dataHandler(new Handler<Buffer>() {
                            @Override
                            public void handle(Buffer event) {
                                listener.onData(event);
                            }
                        });

                        socket.closeHandler(new Handler<Void>() {
                            @Override
                            public void handle(Void event) {
                                listener.onTransportClosed();
                            }
                        });

                        socket.exceptionHandler(new Handler<Throwable>() {
                            @Override
                            public void handle(Throwable event) {
                                listener.onTransportError(event);
                            }
                        });

                    } else {
                        connected.set(false);
                        connectionError.set(asyncResult.cause());
                        connectLatch.countDown();
                    }
                }
            });
        } catch (Throwable reason) {
            LOG.info("Failed to connect to target Broker: {}", reason);
            throw IOExceptionSupport.create(reason);
        }

        try {
            connectLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (connectionError.get() != null) {
            throw IOExceptionSupport.create(connectionError.get());
        }
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            if (connected.get()) {
                socket.close();
                connected.set(false);
            }

            vertx.stop();
        }
    }

    @Override
    public void send(ByteBuffer output) throws IOException {
        checkConnected();
        Buffer sendBuffer = new Buffer(output.remaining());
        while (output.hasRemaining()) {
            sendBuffer.appendByte(output.get());
        }
        vertx.eventBus().send(socket.writeHandlerID(), sendBuffer);
    }

    /**
     * Allows a subclass to configure the NetClient beyond what this transport might do.
     *
     * @throws IOException if an error occurs.
     */
    protected void configureNetClient(NetClient client) throws IOException {
        // NO-OP for this Transport.
    }

    private void checkConnected() throws IOException {
        if (!connected.get()) {
            throw new IOException("Cannot send to a non-connected transport.");
        }
    }
}
