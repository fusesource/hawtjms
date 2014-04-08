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

import static io.hawtjms.provider.stomp.StompConstants.ID;
import static io.hawtjms.provider.stomp.StompConstants.TRUE;

import java.util.Map;

import javax.jms.JMSException;

import org.fusesource.hawtbuf.AsciiBuffer;

/**
 * Server Adapter instance used to interact with an ActiveMQ Broker.
 */
public class ActiveMQServerAdaptor extends GenericStompServerAdaptor {

    private static final AsciiBuffer SUBSCRIPTION_NAME = new AsciiBuffer("activemq.subscriptionName");
    private static final AsciiBuffer NO_LOCAL = new AsciiBuffer("activemq.noLocal");

    @Override
    public boolean matchesServerAndVersion(String server) {
        return server != null && server.startsWith("ActiveMQ/");
    }

    @Override
    public void addSubscribeHeaders(Map<AsciiBuffer, AsciiBuffer> headerMap, boolean persistent, boolean browser, boolean noLocal, int prefetch)
        throws JMSException {
        if (browser) {
            throw new JMSException("ActiveMQ does not support browsing over STOMP");
        }
        if (noLocal) {
            headerMap.put(NO_LOCAL, TRUE);
        }
        if (persistent) {
            headerMap.put(SUBSCRIPTION_NAME, headerMap.get(ID));
        }
    }
}
