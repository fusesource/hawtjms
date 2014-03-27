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

import org.apache.qpid.proton.amqp.messaging.Source;
import org.fusesource.amqpjms.jms.meta.JmsConsumerInfo;

/**
 * Queue Browser implementation for AMQP
 */
public class AmqpQueueBrowser extends AmqpConsumer {

    /**
     * @param session
     * @param info
     */
    public AmqpQueueBrowser(AmqpSession session, JmsConsumerInfo info) {
        super(session, info);
    }

    @Override
    protected void configureSource(Source source) {
        if (info.isBrowser()) {
            source.setDistributionMode(COPY);
        }

        super.configureSource(source);
    }

    @Override
    public boolean isBrowser() {
        return true;
    }
}
