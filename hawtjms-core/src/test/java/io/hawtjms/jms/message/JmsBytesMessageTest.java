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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;

import org.junit.Test;

/**
 *
 */
public class JmsBytesMessageTest {

    private final JmsMessageFactory factory = new JmsDefaultMessageFactory();

    @Test
    public void testGetBodyLength() {
        JmsBytesMessage msg = factory.createBytesMessage();
        int len = 10;
        try {
            for (int i = 0; i < len; i++) {
                msg.writeLong(5L);
            }
        } catch (JMSException ex) {
            ex.printStackTrace();
        }
        try {
            msg.reset();
            assertTrue(msg.getBodyLength() == (len * 8));
        } catch (Throwable e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadBoolean() {
        JmsBytesMessage msg = factory.createBytesMessage();
        try {
            msg.writeBoolean(true);
            msg.reset();
            assertTrue(msg.readBoolean());
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadByte() {
        JmsBytesMessage msg = factory.createBytesMessage();
        try {
            msg.writeByte((byte) 2);
            msg.reset();
            assertTrue(msg.readByte() == 2);
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadUnsignedByte() {
        JmsBytesMessage msg = factory.createBytesMessage();
        try {
            msg.writeByte((byte) 2);
            msg.reset();
            assertTrue(msg.readUnsignedByte() == 2);
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadShort() {
        JmsBytesMessage msg = factory.createBytesMessage();
        try {
            msg.writeShort((short) 3000);
            msg.reset();
            assertTrue(msg.readShort() == 3000);
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadUnsignedShort() {
        JmsBytesMessage msg = factory.createBytesMessage();
        try {
            msg.writeShort((short) 3000);
            msg.reset();
            assertTrue(msg.readUnsignedShort() == 3000);
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadChar() {
        JmsBytesMessage msg = factory.createBytesMessage();
        try {
            msg.writeChar('a');
            msg.reset();
            assertTrue(msg.readChar() == 'a');
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadInt() {
        JmsBytesMessage msg = factory.createBytesMessage();
        try {
            msg.writeInt(3000);
            msg.reset();
            assertTrue(msg.readInt() == 3000);
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadLong() {
        JmsBytesMessage msg = factory.createBytesMessage();
        try {
            msg.writeLong(3000);
            msg.reset();
            assertTrue(msg.readLong() == 3000);
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadFloat() {
        JmsBytesMessage msg = factory.createBytesMessage();
        try {
            msg.writeFloat(3.3f);
            msg.reset();
            assertTrue(msg.readFloat() == 3.3f);
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadDouble() {
        JmsBytesMessage msg = factory.createBytesMessage();
        try {
            msg.writeDouble(3.3d);
            msg.reset();
            assertTrue(msg.readDouble() == 3.3d);
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadUTF() {
        JmsBytesMessage msg = factory.createBytesMessage();
        try {
            String str = "this is a test";
            msg.writeUTF(str);
            msg.reset();
            assertTrue(msg.readUTF().equals(str));
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadBytesbyteArray() {
        JmsBytesMessage msg = factory.createBytesMessage();
        try {
            byte[] data = new byte[50];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) i;
            }
            msg.writeBytes(data);
            msg.reset();
            byte[] test = new byte[data.length];
            msg.readBytes(test);
            for (int i = 0; i < test.length; i++) {
                assertTrue(test[i] == i);
            }
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testWriteObject() throws JMSException {
        JmsBytesMessage msg = factory.createBytesMessage();
        try {
            msg.writeObject("fred");
            msg.writeObject(Boolean.TRUE);
            msg.writeObject(Character.valueOf('q'));
            msg.writeObject(Byte.valueOf((byte) 1));
            msg.writeObject(Short.valueOf((short) 3));
            msg.writeObject(Integer.valueOf(3));
            msg.writeObject(Long.valueOf(300L));
            msg.writeObject(new Float(3.3f));
            msg.writeObject(new Double(3.3));
            msg.writeObject(new byte[3]);
        } catch (MessageFormatException mfe) {
            fail("objectified primitives should be allowed");
        }
        try {
            msg.writeObject(new Object());
            fail("only objectified primitives are allowed");
        } catch (MessageFormatException mfe) {
        }
    }

    @Test
    public void testClearBody() throws JMSException {
        JmsBytesMessage bytesMessage = factory.createBytesMessage();
        try {
            bytesMessage.writeInt(1);
            bytesMessage.clearBody();
            assertFalse(bytesMessage.isReadOnlyBody());
            bytesMessage.writeInt(1);
            bytesMessage.readInt();
        } catch (MessageNotReadableException mnwe) {
        } catch (MessageNotWriteableException mnwe) {
            assertTrue(false);
        }
    }

    @Test
    public void testReset() throws JMSException {
        JmsBytesMessage message = factory.createBytesMessage();
        try {
            message.writeDouble(24.5);
            message.writeLong(311);
        } catch (MessageNotWriteableException mnwe) {
            fail("should be writeable");
        }
        message.reset();
        try {
            assertTrue(message.isReadOnlyBody());
            assertEquals(message.readDouble(), 24.5, 0);
            assertEquals(message.readLong(), 311);
        } catch (MessageNotReadableException mnre) {
            fail("should be readable");
        }
        try {
            message.writeInt(33);
            fail("should throw exception");
        } catch (MessageNotWriteableException mnwe) {
        }
    }

    @Test
    public void testReadOnlyBody() throws JMSException {
        JmsBytesMessage message = factory.createBytesMessage();
        try {
            message.writeBoolean(true);
            message.writeByte((byte) 1);
            message.writeByte((byte) 1);
            message.writeBytes(new byte[1]);
            message.writeBytes(new byte[3], 0, 2);
            message.writeChar('a');
            message.writeDouble(1.5);
            message.writeFloat((float) 1.5);
            message.writeInt(1);
            message.writeLong(1);
            message.writeObject("stringobj");
            message.writeShort((short) 1);
            message.writeShort((short) 1);
            message.writeUTF("utfstring");
        } catch (MessageNotWriteableException mnwe) {
            fail("Should be writeable");
        }
        message.reset();
        try {
            message.readBoolean();
            message.readByte();
            message.readUnsignedByte();
            message.readBytes(new byte[1]);
            message.readBytes(new byte[2], 2);
            message.readChar();
            message.readDouble();
            message.readFloat();
            message.readInt();
            message.readLong();
            message.readUTF();
            message.readShort();
            message.readUnsignedShort();
            message.readUTF();
        } catch (MessageNotReadableException mnwe) {
            fail("Should be readable");
        }
        try {
            message.writeBoolean(true);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeByte((byte) 1);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeBytes(new byte[1]);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeBytes(new byte[3], 0, 2);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeChar('a');
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeDouble(1.5);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeFloat((float) 1.5);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeInt(1);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeLong(1);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeObject("stringobj");
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeShort((short) 1);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeUTF("utfstring");
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
    }

    @Test
    public void testWriteOnlyBody() throws JMSException {
        JmsBytesMessage message = factory.createBytesMessage();
        message.clearBody();
        try {
            message.writeBoolean(true);
            message.writeByte((byte) 1);
            message.writeByte((byte) 1);
            message.writeBytes(new byte[1]);
            message.writeBytes(new byte[3], 0, 2);
            message.writeChar('a');
            message.writeDouble(1.5);
            message.writeFloat((float) 1.5);
            message.writeInt(1);
            message.writeLong(1);
            message.writeObject("stringobj");
            message.writeShort((short) 1);
            message.writeShort((short) 1);
            message.writeUTF("utfstring");
        } catch (MessageNotWriteableException mnwe) {
            fail("Should be writeable");
        }
        try {
            message.readBoolean();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException mnwe) {
        }
        try {
            message.readByte();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readUnsignedByte();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readBytes(new byte[1]);
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readBytes(new byte[2], 2);
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readChar();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readDouble();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readFloat();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readInt();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readLong();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readUTF();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readShort();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readUnsignedShort();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readUTF();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
    }
}
