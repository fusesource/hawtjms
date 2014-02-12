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
package org.fusesource.amqpjms.jms;

import javax.jms.TemporaryQueue;

/**
 * Temporary Queue Object
 */
public class JmsTempQueue extends JmsDestination implements TemporaryQueue {

    public JmsTempQueue() {
        super(null, null);
    }

    public JmsTempQueue(String prefix, String name) {
        super(prefix, name);
    }

    @Override
    public JmsTempQueue copy() {
        final JmsTempQueue copy = new JmsTempQueue();
        copy.setProperties(getProperties());
        return copy;
    }

    /**
     * @see javax.jms.TemporaryQueue#delete()
     */
    @Override
    public void delete() {
        // TODO:
    }

    /**
     * @return name
     * @see javax.jms.Queue#getQueueName()
     */
    @Override
    public String getQueueName() {
        return getName();
    }
}
