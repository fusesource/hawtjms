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

import javax.jms.JMSException;

import org.apache.qpid.proton.engine.Endpoint;
import org.apache.qpid.proton.engine.EndpointState;
import org.fusesource.amqpjms.jms.meta.JmsResource;
import org.fusesource.amqpjms.provider.ProviderRequest;

/**
 * Abstract base for all AmqpResource implementations to extend.
 *
 * This abstract class wraps up the basic state management bits so that the concrete
 * object don't have to reproduce it.  Provides hooks for the subclasses to initialize
 * and shutdown.
 */
public abstract class AbstractAmqpResource<R extends JmsResource, E extends Endpoint> implements AmqpResource {

    protected ProviderRequest<JmsResource> openRequest;
    protected ProviderRequest<Void> closeRequest;

    protected E endpoint;
    protected final R info;

    /**
     * Creates a new AbstractAmqpResource instance with the JmsResource provided, and
     * sets the Endpoint to null.
     *
     * @param info
     *        The JmsResource instance that this AmqpResource is managing.
     */
    public AbstractAmqpResource(R info) {
        this(info, null);
    }

    /**
     * Creates a new AbstractAmqpResource instance with the JmsResource provided, and
     * sets the Endpoint to the given value.
     *
     * @param info
     *        The JmsResource instance that this AmqpResource is managing.
     * @param endpoint
     *        The Proton Endpoint instance that this object maps to.
     */
    public AbstractAmqpResource(R info, E endpoint) {
        this.info = info;
        this.endpoint = endpoint;
    }

    @Override
    public void open(ProviderRequest<JmsResource> request) {
        doOpen();
        this.endpoint.setContext(this);
        this.endpoint.open();
        this.openRequest = request;
    }

    @Override
    public boolean isOpen() {
        return this.endpoint.getRemoteState() == EndpointState.ACTIVE;
    }

    @Override
    public void opened() {
        if (this.openRequest != null) {
            this.openRequest.onSuccess(info);
            this.openRequest = null;
        }
    }

    @Override
    public void close(ProviderRequest<Void> request) {
        doClose();
        this.endpoint.close();
        this.closeRequest = request;
    }

    @Override
    public boolean isClosed() {
        return this.endpoint.getRemoteState() == EndpointState.CLOSED;
    }

    @Override
    public void closed() {
        if (this.closeRequest != null) {
            this.closeRequest.onSuccess(null);
            this.closeRequest = null;
        }
    }

    @Override
    public void failed() {
        // TODO - Figure out a real exception to throw.
        if (openRequest != null) {
            openRequest.onFailure(new JMSException("Failed to create Session"));
            openRequest = null;
        }

        if (closeRequest != null) {
            closeRequest.onFailure(new JMSException("Failed to create Session"));
            closeRequest = null;
        }
    }

    public E getEndpoint() {
        return this.endpoint;
    }

    public R getJmsResource() {
        return this.info;
    }

    protected abstract void doOpen();

    protected abstract void doClose();
}
