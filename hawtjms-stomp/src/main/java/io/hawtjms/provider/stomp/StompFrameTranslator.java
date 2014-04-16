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
package io.hawtjms.provider.stomp;

import static io.hawtjms.provider.stomp.StompConstants.MESSAGE;
import io.hawtjms.jms.message.JmsInboundMessageDispatch;
import io.hawtjms.jms.message.JmsOutboundMessageDispatch;

import java.io.IOException;

/**
 * Translate between the JmsMessage types and a StompFrame for transmission and
 * converts StompFrame MESSAGE commands into the appropriate JmsMessage type.
 */
public class StompFrameTranslator {

    /**
     * Converts the given JmsOutboundMessageDispatch into a StompFrame for transmission.
     *
     * @param envelope
     *        the envelope containing the message and all delivery information.
     *
     * @return a StompFrame ready for transmission.
     *
     * @throws IOException if an error occurs in the transformation.
     */
    public StompFrame toStompFrame(JmsOutboundMessageDispatch envelope) throws IOException {
        StompFrame result = new StompFrame(MESSAGE);

        return result;
    }

    /**
     * Converts a StompFrame into the appropriate JmsMessage instance for dispatch
     * to the targeted MessageConsumer.
     *
     * @param frame
     *        the StompFrame to transform into a JmsMessage type.
     * @param consumerId
     *        the JmsConsumerId of the consumer that will receive this message.
     *
     * @returns a JmsMessage object ready for dispatch.
     *
     * @throws IOException if an error occurs in the transformation.
     */
    public JmsInboundMessageDispatch toJmsMessage(StompFrame frame) throws IOException {
        return null;
    }

}
