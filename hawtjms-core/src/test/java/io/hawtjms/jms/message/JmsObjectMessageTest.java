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
package io.hawtjms.jms.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import javax.jms.JMSException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;

import org.junit.Test;

/**
 *
 */
public class JmsObjectMessageTest {

    private final JmsMessageFactory factory = new JmsDefaultMessageFactory();

    @Test
    public void testBytes() throws JMSException, IOException {
        JmsObjectMessage msg = factory.createObjectMessage();
        String str = "testText";
        msg.setObject(str);

        msg = (JmsObjectMessage) msg.copy();
        assertEquals(msg.getObject(), str);
    }

    @Test
    public void testSetObject() throws JMSException {
        JmsObjectMessage msg = factory.createObjectMessage();
        String str = "testText";
        msg.setObject(str);
        assertTrue(msg.getObject() == str);
    }

    @Test
    public void testClearBody() throws JMSException {
        JmsObjectMessage objectMessage = factory.createObjectMessage();
        try {
            objectMessage.setObject("String");
            objectMessage.clearBody();
            assertFalse(objectMessage.isReadOnlyBody());
            assertNull(objectMessage.getObject());
            objectMessage.setObject("String");
            objectMessage.getObject();
        } catch (MessageNotWriteableException mnwe) {
            fail("should be writeable");
        }
    }

    @Test
    public void testReadOnlyBody() throws JMSException {
        JmsObjectMessage msg = factory.createObjectMessage();
        msg.setObject("test");
        msg.setReadOnlyBody(true);
        try {
            msg.getObject();
        } catch (MessageNotReadableException e) {
            fail("should be readable");
        }
        try {
            msg.setObject("test");
            fail("should throw exception");
        } catch (MessageNotWriteableException e) {
        }
    }

    @Test
    public void testWriteOnlyBody() throws JMSException { // should always be readable
        JmsObjectMessage msg = factory.createObjectMessage();
        msg.setReadOnlyBody(false);
        try {
            msg.setObject("test");
            msg.getObject();
        } catch (MessageNotReadableException e) {
            fail("should be readable");
        }
        msg.setReadOnlyBody(true);
        try {
            msg.getObject();
            msg.setObject("test");
            fail("should throw exception");
        } catch (MessageNotReadableException e) {
            fail("should be readable");
        } catch (MessageNotWriteableException mnwe) {
        }
    }
}
