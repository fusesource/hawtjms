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
package org.fusesource.amqpjms.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import javax.jms.JMSException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;

import org.fusesource.amqpjms.jms.message.JmsMessage;
import org.fusesource.amqpjms.jms.message.JmsTextMessage;
import org.fusesource.hawtbuf.Buffer;
import org.junit.Test;

/**
 *
 */
public class JmsTextMessageTest {

    @Test
    public void testShallowCopy() throws JMSException {
        JmsTextMessage msg = new JmsTextMessage();
        String string = "str";
        msg.setText(string);
        JmsTextMessage copy = (JmsTextMessage) msg.copy();
        assertTrue(msg.getText() == copy.getText());
    }

    @Test
    public void testSetText() {
        JmsTextMessage msg = new JmsTextMessage();
        String str = "testText";
        try {
            msg.setText(str);
            assertEquals(msg.getText(), str);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetBytes() throws JMSException, IOException {
        JmsTextMessage msg = new JmsTextMessage();
        String str = "testText";
        msg.setText(str);
        msg.onSend();

        Buffer bytes = msg.getContent();
        msg = new JmsTextMessage();
        msg.setContent(bytes);

        assertEquals(msg.getText(), str);
    }

    @Test
    public void testClearBody() throws JMSException, IOException {
        JmsTextMessage textMessage = new JmsTextMessage();
        textMessage.setText("string");
        textMessage.clearBody();
        assertFalse(textMessage.isReadOnlyBody());
        assertNull(textMessage.getText());
        try {
            textMessage.setText("String");
            textMessage.getText();
        } catch (MessageNotWriteableException mnwe) {
            fail("should be writeable");
        } catch (MessageNotReadableException mnre) {
            fail("should be readable");
        }
    }

    @Test
    public void testReadOnlyBody() throws JMSException {
        JmsTextMessage textMessage = new JmsTextMessage();
        textMessage.setText("test");
        textMessage.setReadOnlyBody(true);
        try {
            textMessage.getText();
        } catch (MessageNotReadableException e) {
            fail("should be readable");
        }
        try {
            textMessage.setText("test");
            fail("should throw exception");
        } catch (MessageNotWriteableException mnwe) {
        }
    }

    @Test
    public void testWriteOnlyBody() throws JMSException { // should always be readable
        JmsTextMessage textMessage = new JmsTextMessage();
        textMessage.setReadOnlyBody(false);
        try {
            textMessage.setText("test");
            textMessage.getText();
        } catch (MessageNotReadableException e) {
            fail("should be readable");
        }
        textMessage.setReadOnlyBody(true);
        try {
            textMessage.getText();
            textMessage.setText("test");
            fail("should throw exception");
        } catch (MessageNotReadableException e) {
            fail("should be readable");
        } catch (MessageNotWriteableException mnwe) {
        }
    }

    // TODO - Fix toString and null body.
    @Test
    public void testShortText() throws Exception {
        String shortText = "Content";
        JmsTextMessage shortMessage = new JmsTextMessage();
        setContent(shortMessage, shortText);
        assertTrue(shortMessage.toString().contains("text = " + shortText));
        assertTrue(shortMessage.getText().equals(shortText));

        String longText = "Very very very very veeeeeeery loooooooooooooooooooooooooooooooooong text";
        String longExpectedText = "Very very very very veeeeeeery looooooooooooo...ooooong text";
        JmsTextMessage longMessage = new JmsTextMessage();
        setContent(longMessage, longText);
        assertTrue(longMessage.toString().contains("text = " + longExpectedText));
        assertTrue(longMessage.getText().equals(longText));
    }

    // TODO - Fix toString and null body.
    @Test
    public void testNullText() throws Exception {
        JmsTextMessage nullMessage = new JmsTextMessage();
        nullMessage.setContent(null);
        assertTrue(nullMessage.toString().contains("text = null"));
    }

    protected void setContent(JmsMessage message, String text) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(baos);
        dataOut.writeUTF(text);
        dataOut.close();
        message.setContent(new Buffer(baos.toByteArray()));
    }
}
