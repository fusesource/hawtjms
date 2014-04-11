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

import static io.hawtjms.provider.stomp.StompConstants.ACK;
import static io.hawtjms.provider.stomp.StompConstants.BROWSER;
import static io.hawtjms.provider.stomp.StompConstants.CREDIT;
import static io.hawtjms.provider.stomp.StompConstants.ID;
import static io.hawtjms.provider.stomp.StompConstants.PERSISTENT;
import static io.hawtjms.provider.stomp.StompConstants.TRUE;
import static io.hawtjms.provider.stomp.StompConstants.UNSUBSCRIBE;
import io.hawtjms.provider.stomp.StompFrame;

import java.nio.ByteBuffer;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;

/**
 * Apollo Broker server adapter for STOMP.
 */
public class ApolloServerAdaptor extends GenericStompServerAdaptor {

    @Override
    public boolean matchesServerAndVersion(String server) {
        return server != null && server.startsWith("apache-apollo/");
    }

    @Override
    public StompFrame createCreditFrame(StompFrame messageFrame) {
        final ByteBuffer content = messageFrame.getContent();
        String credit = "1";
        if (content != null) {
            credit += "," + content.limit();
        }

        StompFrame frame = new StompFrame();
        frame.setCommand(ACK);
        // frame.headerMap().put(SUBSCRIPTION, consumer.id); TODO
        frame.getProperties().put(CREDIT, credit);
        return frame;
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
        if (noLocal) {
            throw new JMSException("Server does not support 'no local' semantics over STOMP");
        }

        if (persistent) {
            headerMap.put(PERSISTENT, TRUE);
        }

        if (browser) {
            headerMap.put(BROWSER, TRUE);
        }
    }

    @Override
    public StompFrame createUnsubscribeFrame(String consumerId, boolean persistent) throws JMSException {
        StompFrame frame = new StompFrame();
        frame.setCommand(UNSUBSCRIBE);
        frame.getProperties().put(ID, consumerId);
        if (persistent) {
            frame.getProperties().put(PERSISTENT, TRUE);
        }
        return frame;
    }
}
