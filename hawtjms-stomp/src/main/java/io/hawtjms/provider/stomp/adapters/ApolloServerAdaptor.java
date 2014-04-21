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

import static io.hawtjms.provider.stomp.StompConstants.BROWSER;
import static io.hawtjms.provider.stomp.StompConstants.ID;
import static io.hawtjms.provider.stomp.StompConstants.PERSISTENT;
import static io.hawtjms.provider.stomp.StompConstants.TRUE;
import static io.hawtjms.provider.stomp.StompConstants.UNSUBSCRIBE;
import io.hawtjms.jms.meta.JmsConsumerInfo;
import io.hawtjms.provider.stomp.StompConnection;
import io.hawtjms.provider.stomp.StompFrame;

import javax.jms.JMSException;

/**
 * Apollo Broker server adapter for STOMP.
 */
public class ApolloServerAdaptor extends GenericStompServerAdaptor {

    public ApolloServerAdaptor(StompConnection connection, String version) {
        super(connection, version);
    }

    @Override
    public String getServerName() {
        return "Apache-Apollo";
    }

    @Override
    public StompFrame createCreditFrame(StompFrame messageFrame) {
//        final Buffer content = messageFrame.getContent();
//        String credit = "1";
//        if (content != null) {
//            credit += "," + content.length();
//        }
//
//        StompFrame frame = new StompFrame();
//        frame.setCommand(ACK);
//        frame.headerMap().put(SUBSCRIPTION, consumer.id); TODO
//        frame.getProperties().put(CREDIT, credit);
        return null;
    }

    @Override
    public void addSubscribeHeaders(StompFrame frame, JmsConsumerInfo consumerInfo) throws JMSException {
        if (consumerInfo.isNoLocal()) {
            throw new JMSException("Server does not support 'no local' semantics over STOMP");
        }

        if (consumerInfo.isDurable()) {
            frame.setProperty(PERSISTENT, TRUE);
        }

        if (consumerInfo.isBrowser()) {
            frame.setProperty(BROWSER, TRUE);
        }
    }

    @Override
    public StompFrame createUnsubscribeFrame(JmsConsumerInfo consumerInfo) throws JMSException {
        StompFrame frame = new StompFrame();
        frame.setCommand(UNSUBSCRIBE);
        frame.getProperties().put(ID, consumerInfo.getConsumerId().toString());
        if (consumerInfo.isDurable()) {
            frame.getProperties().put(PERSISTENT, TRUE);
        }
        return frame;
    }
}
