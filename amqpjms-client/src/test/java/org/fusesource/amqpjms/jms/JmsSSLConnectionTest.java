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

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

/**
 * Test that we can connect to a broker over SSL.
 */
public class JmsSSLConnectionTest {

    @Test(timeout=30000)
    public void testCreateConnection() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory("amqps://localhost:5673");
        JmsConnection connection = (JmsConnection) factory.createConnection();
        assertNotNull(connection);
        connection.close();
    }

    @Test(timeout=30000)
    public void testCreateConnectionAndStart() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory("amqps://localhost:5673");
        JmsConnection connection = (JmsConnection) factory.createConnection();
        assertNotNull(connection);
        connection.start();
        connection.close();
    }
}
