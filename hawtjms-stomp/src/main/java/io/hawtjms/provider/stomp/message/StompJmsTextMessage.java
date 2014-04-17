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
package io.hawtjms.provider.stomp.message;

import io.hawtjms.jms.message.JmsMessageFacade;
import io.hawtjms.jms.message.JmsTextMessage;

import org.fusesource.hawtbuf.UTF8Buffer;

/**
 * STOMP JmsTextMessage extension that reads and writes the message String
 * value to the underlying STOMP frame.
 */
public class StompJmsTextMessage extends JmsTextMessage {

    private final StompJmsMessageFacade facade;

    /**
     * @param facade
     */
    public StompJmsTextMessage(JmsMessageFacade facade) {
        super(facade);
        this.facade = (StompJmsMessageFacade) facade;
    }

    @Override
    protected void internalSetText(String text) {
        UTF8Buffer buffer = new UTF8Buffer(text);
        facade.getStompMessage().setContent(buffer);
    }

    @Override
    protected String internalGetText() {
        return facade.getStompMessage().getContentAsString();
    }
}
