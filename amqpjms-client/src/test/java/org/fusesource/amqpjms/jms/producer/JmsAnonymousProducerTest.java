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
package org.fusesource.amqpjms.jms.producer;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import javax.jms.Connection;
import javax.jms.Session;

import org.fusesource.amqpjms.util.AmqpTestSupport;
import org.junit.Test;

/**
 * Test JMS Anonymous Producer functionality.
 */
public class JmsAnonymousProducerTest extends AmqpTestSupport {

    @Test(timeout = 60000)
    public void testCreateAnonymousMessageProducer() throws Exception {
        Connection connection = createAmqpConnection();
        assertNotNull(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        session.createProducer(null);

        assertTrue(brokerService.getAdminView().getTotalProducerCount() == 0);
        connection.close();
    }
}
