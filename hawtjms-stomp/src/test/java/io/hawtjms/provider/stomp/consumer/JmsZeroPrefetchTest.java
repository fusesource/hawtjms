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
package io.hawtjms.provider.stomp.consumer;

import io.hawtjms.jms.JmsConnection;
import io.hawtjms.test.support.StompTestSupport;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Session;

import org.junit.Test;

/**
 * Tests that STOMP Provider fails consumer create when prefetch is zero.
 */
public class JmsZeroPrefetchTest extends StompTestSupport {

    @Test(timeout=60000, expected=JMSException.class)
    public void testCannotCreateZeroPrefetchConsumer() throws Exception {
        connection = createStompConnection();
        ((JmsConnection)connection).getPrefetchPolicy().setAll(0);
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());
        session.createConsumer(queue);
    }
}
