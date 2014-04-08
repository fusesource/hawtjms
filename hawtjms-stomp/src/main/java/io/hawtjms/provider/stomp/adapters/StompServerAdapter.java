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
package io.hawtjms.provider.stomp.adapters;

import io.hawtjms.provider.stomp.StompFrame;

import java.util.Map;

import javax.jms.JMSException;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;

import org.fusesource.hawtbuf.AsciiBuffer;

/**
 * The StompServerAdapter defines an interface for an adapter class that can
 * augment the standard STOMP protocol with additional functionality offered by
 * various STOMP server implementations.
 */
public interface StompServerAdapter {

    /**
     * Returns whether the server name given in the STOMP CONNECTED frame match this
     * ServerAdapter instance.
     *
     * @param server
     *        the name of the server that we have connected to.
     *
     * @return true if this server matches.
     */
    public abstract boolean matchesServerAndVersion(String server);

    public abstract boolean isTemporaryQueue(String value) throws JMSException;

    public abstract boolean isTemporaryTopic(String value) throws JMSException;

    public abstract StompFrame createCreditFrame(StompFrame messageFrame);

    public abstract TemporaryQueue createTemporaryQueue() throws JMSException;

    public abstract TemporaryTopic createTemporaryTopic() throws JMSException;

    public abstract void addSubscribeHeaders(Map<AsciiBuffer, AsciiBuffer> headerMap, boolean persistent, boolean browser, boolean noLocal, int prefetch)
        throws JMSException;

    public abstract StompFrame createUnsubscribeFrame(AsciiBuffer consumerId, boolean persistent) throws JMSException;

}