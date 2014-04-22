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

import static io.hawtjms.provider.stomp.StompConstants.DESTINATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;
import io.hawtjms.jms.JmsDestination;
import io.hawtjms.jms.JmsTopic;
import io.hawtjms.jms.message.JmsMessage;
import io.hawtjms.provider.stomp.StompConnection;
import io.hawtjms.provider.stomp.StompFrame;
import io.hawtjms.provider.stomp.adapters.GenericStompServerAdaptor;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * Test the basic functionality of the STOMP message factory.
 */
@RunWith(MockitoJUnitRunner.class)
public class StompJmsMessageFactoryTest {

    @Mock
    private StompConnection connection;

    @Before
    public void setUp() throws Exception {
        GenericStompServerAdaptor adapter = new GenericStompServerAdaptor(connection);

        when(connection.getServerAdapter()).thenReturn(adapter);
        when(connection.getTempQueuePrefix()).thenReturn("temp-queue://");
        when(connection.getTempTopicPrefix()).thenReturn("temp-topic://");
        when(connection.getQueuePrefix()).thenReturn("queue://");
        when(connection.getTopicPrefix()).thenReturn("topic://");
    }

    @Test
    public void testCreate() {
        StompJmsMessageFactory factory = new StompJmsMessageFactory(connection);
        assertNotNull(factory.getStompConnection());
    }

    @Test
    public void testCreateJmsMessage() throws Exception {
        JmsTopic topic = new JmsTopic("test");
        StompJmsMessageFactory factory = new StompJmsMessageFactory(connection);
        assertNotNull(factory.getStompConnection());

        JmsMessage message = factory.createMessage();
        message.setJMSDestination(topic);
        JmsDestination destination = (JmsDestination) message.getJMSDestination();
        assertEquals("test", destination.getName());

        StompJmsMessageFacade facade = (StompJmsMessageFacade) message.getFacade();
        StompFrame frame = facade.getStompMessage();
        assertEquals("topic://test", frame.getProperty(DESTINATION));
    }
}
