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

import io.hawtjms.jms.message.JmsBytesMessage;

import javax.jms.JMSException;

/**
 * STOMP based JmsBytesMessage that gets direct access to the bytes content
 * of the wrapped STOMP Frame.
 */
public class StompJmsBytesMessage extends JmsBytesMessage {

    private final StompJmsMessageFacade facade;

    /**
     * @param facade
     */
    public StompJmsBytesMessage(StompJmsMessageFacade facade) {
        super(facade);
        this.facade = facade;
        this.content = facade.getStompMessage().getContent();
    }

    @Override
    public void onSend() throws JMSException {
        super.onSend();
        facade.getStompMessage().setContent(content);
    }
}
