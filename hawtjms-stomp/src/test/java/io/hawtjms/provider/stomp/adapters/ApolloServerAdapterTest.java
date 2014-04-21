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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import io.hawtjms.provider.stomp.StompConnection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * Test case for the Apache Apollo STOMP Server Adapter.
 */
@RunWith(MockitoJUnitRunner.class)
public class ApolloServerAdapterTest {

    @Mock
    private StompConnection connection;

    @Test
    public void testCreateServerAdapter() {
        StompServerAdapter adapter = StompServerAdapterFactory.create(connection, "apache-apollo/1.7.0");
        assertNotNull(adapter);
        assertEquals("1.7.0", adapter.getServerVersion());
        assertEquals("Apache-Apollo", adapter.getServerName());
    }
}
