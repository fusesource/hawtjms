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
import static io.hawtjms.provider.stomp.StompConstants.UNSUBSCRIBE;
import io.hawtjms.provider.stomp.StompFrame;

import java.util.Map;

import javax.jms.JMSException;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;

/**
 * Base adapter class for supporting STOMP servers that offer extended features
 * beyond the defined STOMP specification.
 */
public class GenericStompServerAdaptor implements StompServerAdapter {

    private final String version;

    public GenericStompServerAdaptor() {
        this.version = "Unknown";
    }

    public GenericStompServerAdaptor(String version) {
        this.version = version;
    }

    @Override
    public boolean isTemporaryQueue(String value) throws JMSException {
        return false;
    }

    @Override
    public boolean isTemporaryTopic(String value) throws JMSException {
        return false;
    }

    @Override
    public StompFrame createCreditFrame(StompFrame messageFrame) {
        return null;
    }

    @Override
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        return null;
    }

    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        return null;
    }

    @Override
    public void addSubscribeHeaders(Map<String, String> headerMap, boolean persistent, boolean browser, boolean noLocal, int prefetch) throws JMSException {
        if (browser) {
            throw new JMSException("Server does not support browsing over STOMP");
        }
        if (noLocal) {
            throw new JMSException("Server does not support 'no local' semantics over STOMP");
        }
        if (persistent) {
            throw new JMSException("Server does not durable subscriptions over STOMP");
        }
    }

    @Override
    public StompFrame createUnsubscribeFrame(String consumerId, boolean persistent) throws JMSException {
        if (persistent) {
            throw new JMSException("Server does not support un-subscribing durable subscriptions over STOMP");
        }
        StompFrame frame = new StompFrame();
        frame.setCommand(UNSUBSCRIBE);
        frame.getProperties().put(ID, consumerId);
        return frame;
    }

    @Override
    public String getServerVersion() {
        return this.version;
    }

    @Override
    public String getServerName() {
        return "Generic";
    }
}
