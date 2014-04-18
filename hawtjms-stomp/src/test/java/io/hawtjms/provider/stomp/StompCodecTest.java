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

import static io.hawtjms.provider.stomp.StompConstants.CONNECTED;
import static io.hawtjms.provider.stomp.StompConstants.DISCONNECT;
import static io.hawtjms.provider.stomp.StompConstants.MESSAGE;
import static io.hawtjms.provider.stomp.StompConstants.V1_0;
import static io.hawtjms.provider.stomp.StompConstants.V1_1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import io.hawtjms.provider.stomp.StompCodec;
import io.hawtjms.provider.stomp.StompConstants;
import io.hawtjms.provider.stomp.StompFrame;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the STOMP Codec class to validate encode / decode integrity.
 */
public class StompCodecTest {

    private StompCodec codec;

    @Before
    public void setUp() {
        codec = new StompCodec();
    }

    @After
    public void tearDown() {
        codec = null;
    }

    @Test
    public void testContructor() {
        assertEquals(V1_0, codec.getVersion());
        assertEquals(20, codec.getMaxCommandLength());

        codec.setVersion(V1_1);
        assertEquals(V1_1, codec.getVersion());
    }

    @Test
    public void testSimpleCommandDecode() throws IOException {
        String input = "CONNECTED\n" +
                       "version:1.1\n" +
                       "server:activemq/5.9\n\n" +
                       StompConstants.NULL;

        ByteBuffer buffer = ByteBuffer.wrap(input.getBytes(StompConstants.UTF8));

        StompFrame frame = codec.decode(buffer);
        assertNotNull(frame);
        assertEquals(CONNECTED, frame.getCommand());
        assertEquals(V1_1, frame.getProperty("version"));
        assertEquals("activemq/5.9", frame.getProperty("server"));
    }

    @Test
    public void testDecodeTextMessage() throws IOException {
        final String body = "This is a test";

        String input = "MESSAGE\n" +
                       "version:1.1\n" +
                       "server:activemq/5.9\n\n" +
                       body + StompConstants.NULL;

        ByteBuffer buffer = ByteBuffer.wrap(input.getBytes(StompConstants.UTF8));

        StompFrame frame = codec.decode(buffer);
        assertNotNull(frame);
        assertEquals(MESSAGE, frame.getCommand());
        assertEquals(V1_1, frame.getProperty("version"));
        assertEquals("activemq/5.9", frame.getProperty("server"));
        assertEquals(body, frame.getContentAsString());
    }

    @Test
    public void testSimpleCommandEncode() throws IOException {
        StompFrame frame = new StompFrame("CONNECTED");
        frame.setProperty("version", V1_1);
        frame.setProperty("server", "activemq/5.9");

        ByteBuffer encoded = codec.encode(frame);
        assertNotNull(encoded);
        assertTrue(encoded.position() == 0);
        assertTrue(encoded.limit() > 0);
    }

    @Test
    public void testCommandOnlyFrameEncodeAndDecode() throws IOException {
        StompFrame frame = new StompFrame(DISCONNECT);

        ByteBuffer encoded = codec.encode(frame);
        assertNotNull(encoded);
        assertTrue(encoded.position() == 0);
        assertTrue(encoded.limit() > 0);

        frame = codec.decode(encoded);
        assertNotNull(frame);
        assertEquals(DISCONNECT, frame.getCommand());
    }

    @Test
    public void testSimpleCommandEncodeAndDecode() throws IOException {
        StompFrame frame = new StompFrame("CONNECTED");
        frame.setProperty("version", V1_1);
        frame.setProperty("server", "activemq/5.9");

        ByteBuffer encoded = codec.encode(frame);
        assertNotNull(encoded);
        assertTrue(encoded.position() == 0);
        assertTrue(encoded.limit() > 0);

        frame = codec.decode(encoded);
        assertNotNull(frame);
        assertEquals(CONNECTED, frame.getCommand());
        assertEquals(V1_1, frame.getProperty("version"));
        assertEquals("activemq/5.9", frame.getProperty("server"));
    }
}
