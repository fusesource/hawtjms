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

/**
 * The StompServerAdapter defines an interface for an adapter class that can
 * augment the standard STOMP protocol with additional functionality offered by
 * various STOMP server implementations.
 */
public interface StompServerAdapter {

    /**
     * Given a STOMP destination name return true if the value represents a
     * temporary Queue instance on this STOMP server.
     *
     * @param value
     *        the destination name to examine.
     *
     * @return true if the destination is a temporary queue type.
     *
     * @throws JMSException if an error occurs while examining the destination name.
     */
    boolean isTemporaryQueue(String value) throws JMSException;

    /**
     * Given a STOMP destination name return true if the value represents a
     * temporary Topic instance on this STOMP server.
     *
     * @param value
     *        the destination name to examine.
     *
     * @return true if the destination is a temporary topic type.
     *
     * @throws JMSException if an error occurs while examining the destination name.
     */
    boolean isTemporaryTopic(String value) throws JMSException;

    StompFrame createCreditFrame(StompFrame messageFrame);

    TemporaryQueue createTemporaryQueue() throws JMSException;

    TemporaryTopic createTemporaryTopic() throws JMSException;

    void addSubscribeHeaders(Map<String, String> headerMap, boolean persistent, boolean browser, boolean noLocal, int prefetch) throws JMSException;

    StompFrame createUnsubscribeFrame(String consumerId, boolean persistent) throws JMSException;

    String getServerName();

    String getServerVersion();

}