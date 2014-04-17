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

import io.hawtjms.jms.exceptions.JmsExceptionSupport;

import java.util.ArrayList;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;
import javax.jms.StreamMessage;

import org.fusesource.hawtbuf.Buffer;

/**
 * A <CODE>StreamMessage</CODE> object is used to send a stream of primitive
 * types in the Java programming language. It is filled and read sequentially.
 * It inherits from the <CODE>Message</CODE> interface and adds a stream message
 * body. Its methods are based largely on those found in
 * <CODE>java.io.DataInputStream</CODE> and
 * <CODE>java.io.DataOutputStream</CODE>.
 * <p/>
 * <p/>
 * The primitive types can be read or written explicitly using methods for each
 * type. They may also be read or written generically as objects. For instance,
 * a call to <CODE>StreamMessage.writeInt(6)</CODE> is equivalent to
 * <CODE>StreamMessage.writeObject(new
 * Integer(6))</CODE>. Both forms are provided, because the explicit form is
 * convenient for static programming, and the object form is needed when types
 * are not known at compile time.
 * <p/>
 * <p/>
 * When the message is first created, and when <CODE>clearBody</CODE> is called,
 * the body of the message is in write-only mode. After the first call to
 * <CODE>reset</CODE> has been made, the message body is in read-only mode.
 * After a message has been sent, the client that sent it can retain and modify
 * it without affecting the message that has been sent. The same message object
 * can be sent multiple times. When a message has been received, the provider
 * has called <CODE>reset</CODE> so that the message body is in read-only mode
 * for the client.
 * <p/>
 * <p/>
 * If <CODE>clearBody</CODE> is called on a message in read-only mode, the
 * message body is cleared and the message body is in write-only mode.
 * <p/>
 * <p/>
 * If a client attempts to read a message in write-only mode, a
 * <CODE>MessageNotReadableException</CODE> is thrown.
 * <p/>
 * <p/>
 * If a client attempts to write a message in read-only mode, a
 * <CODE>MessageNotWriteableException</CODE> is thrown.
 * <p/>
 * <p/>
 * <CODE>StreamMessage</CODE> objects support the following conversion table.
 * The marked cases must be supported. The unmarked cases must throw a
 * <CODE>JMSException</CODE>. The <CODE>String</CODE>-to-primitive conversions
 * may throw a runtime exception if the primitive's <CODE>valueOf()</CODE>
 * method does not accept it as a valid <CODE>String</CODE> representation of
 * the primitive.
 * <p/>
 * <p/>
 * A value written as the row type can be read as the column type.
 * <p/>
 * <p/>
 *
 * <PRE>
 * | | boolean byte short char int long float double String byte[]
 * |----------------------------------------------------------------------
 * |boolean | X X |byte | X X X X X |short | X X X X |char | X X |int | X X X
 * |long | X X |float | X X X |double | X X |String | X X X X X X X X |byte[] |
 * X |----------------------------------------------------------------------
 * <p/>
 * </PRE>
 * <p/>
 * <p/>
 * <p/>
 * Attempting to read a null value as a primitive type must be treated as
 * calling the primitive's corresponding <code>valueOf(String)</code> conversion
 * method with a null value. Since <code>char</code> does not support a
 * <code>String</code> conversion, attempting to read a null value as a
 * <code>char</code> must throw a <code>NullPointerException</code>.
 *
 * @see javax.jms.Session#createStreamMessage()
 * @see javax.jms.BytesMessage
 * @see javax.jms.MapMessage
 * @see javax.jms.Message
 * @see javax.jms.ObjectMessage
 * @see javax.jms.TextMessage
 */
public class JmsStreamMessage extends JmsMessage implements StreamMessage {

    private final List<Object> content = new ArrayList<Object>(20);

    private Buffer bytes;
    private int remainingBytes;

    private int index;
    private List<Object> stream;

    public JmsStreamMessage(JmsMessageFacade facade) {
        super(facade);
    }

    @Override
    public JmsMessage copy() throws JMSException {
        JmsStreamMessage other = new JmsStreamMessage(facade.copy());
        other.copy(this);
        return other;
    }

    private void copy(JmsStreamMessage other) throws JMSException {
        super.copy(other);
        this.content.clear();
        this.content.addAll(other.content);
    }

    @Override
    public void onSend() throws JMSException {
        super.onSend();
        reset();
    }

    /**
     * Clears out the message body. Clearing a message's body does not clear its
     * header values or property entries.
     * <p/>
     * <p/>
     * If this message body was read-only, calling this method leaves the
     * message body in the same state as an empty body in a newly created
     * message.
     *
     * @throws JMSException
     *         if the JMS provider fails to clear the message body due to some
     *         internal error.
     */
    @Override
    public void clearBody() throws JMSException {
        super.clearBody();
        content.clear();
        index = 0;
        bytes = null;
        remainingBytes = -1;
    }

    /**
     * Reads a <code>boolean</code> from the stream message.
     *
     * @return the <code>boolean</code> value read
     * @throws JMSException
     *         if the JMS provider fails to read the message due to some
     *         internal error.
     * @throws MessageEOFException
     *         if unexpected end of message stream has been reached.
     * @throws MessageFormatException
     *         if this type conversion is invalid.
     * @throws MessageNotReadableException
     *         if the message is in write-only mode.
     */
    @Override
    public boolean readBoolean() throws JMSException {
        initializeReading();
        checkEndOfStream();

        Object value = stream.get(index++);
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof String) {
            return Boolean.valueOf((String) value);
        }

        index--;
        if (value == null) {
            throw new NullPointerException("Cannot convert NULL value to boolean.");
        } else {
            throw new MessageFormatException("stream value is not a boolean type");
        }
    }

    /**
     * Reads a <code>byte</code> value from the stream message.
     *
     * @return the next byte from the stream message as a 8-bit
     *         <code>byte</code>
     * @throws JMSException
     *         if the JMS provider fails to read the message due to some
     *         internal error.
     * @throws MessageEOFException
     *         if unexpected end of message stream has been reached.
     * @throws MessageFormatException
     *         if this type conversion is invalid.
     * @throws MessageNotReadableException
     *         if the message is in write-only mode.
     */
    @Override
    public byte readByte() throws JMSException {
        initializeReading();
        try {
            checkEndOfStream();

            Object value = stream.get(index++);
            if (value instanceof Byte) {
                return (Byte) value;
            }
            if (value instanceof String) {
                return Byte.valueOf((String) value);
            }

            index--;
            if (value == null) {
                throw new NullPointerException("Cannot convert NULL value to byte.");
            } else {
                throw new MessageFormatException("stream value is not a boolean type");
            }
        } catch (NumberFormatException nfe) {
            index--;
            throw nfe;
        }
    }

    /**
     * Reads a 16-bit integer from the stream message.
     *
     * @return a 16-bit integer from the stream message
     * @throws JMSException
     *         if the JMS provider fails to read the message due to some
     *         internal error.
     * @throws MessageEOFException
     *         if unexpected end of message stream has been reached.
     * @throws MessageFormatException
     *         if this type conversion is invalid.
     * @throws MessageNotReadableException
     *         if the message is in write-only mode.
     */
    @Override
    public short readShort() throws JMSException {
        initializeReading();
        try {
            checkEndOfStream();

            Object value = stream.get(index++);
            if (value instanceof Short) {
                return (Short) value;
            }
            if (value instanceof Byte) {
                return ((Byte) value).shortValue();
            }
            if (value instanceof String) {
                return Short.valueOf((String) value);
            }

            index--;
            if (value == null) {
                throw new NullPointerException("Cannot convert NULL value to short.");
            } else {
                throw new MessageFormatException("stream value is not a boolean type");
            }
        } catch (NumberFormatException nfe) {
            index--;
            throw nfe;
        }
    }

    /**
     * Reads a Unicode character value from the stream message.
     *
     * @return a Unicode character from the stream message
     * @throws JMSException
     *         if the JMS provider fails to read the message due to some
     *         internal error.
     * @throws MessageEOFException
     *         if unexpected end of message stream has been reached.
     * @throws MessageFormatException
     *         if this type conversion is invalid
     * @throws MessageNotReadableException
     *         if the message is in write-only mode.
     */
    @Override
    public char readChar() throws JMSException {
        initializeReading();
        checkEndOfStream();

        Object value = stream.get(index++);
        if (value instanceof Character) {
            return (Character) value;
        }

        index--;
        if (value == null) {
            throw new NullPointerException("Cannot convert NULL value to char.");
        } else {
            throw new MessageFormatException("stream value is not a boolean type");
        }
    }

    /**
     * Reads a 32-bit integer from the stream message.
     *
     * @return a 32-bit integer value from the stream message, interpreted as an
     *         <code>int</code>
     * @throws JMSException
     *         if the JMS provider fails to read the message due to some
     *         internal error.
     * @throws MessageEOFException
     *         if unexpected end of message stream has been reached.
     * @throws MessageFormatException
     *         if this type conversion is invalid.
     * @throws MessageNotReadableException
     *         if the message is in write-only mode.
     */
    @Override
    public int readInt() throws JMSException {
        initializeReading();
        try {
            checkEndOfStream();

            Object value = stream.get(index++);
            if (value instanceof Integer) {
                return (Integer) value;
            }
            if (value instanceof Short) {
                return ((Short) value).intValue();
            }
            if (value instanceof Byte) {
                return ((Byte) value).intValue();
            }
            if (value instanceof String) {
                return Integer.valueOf((String) value).shortValue();
            }

            index--;
            if (value == null) {
                throw new NullPointerException("Cannot convert NULL value to int.");
            } else {
                throw new MessageFormatException("stream value is not a boolean type");
            }
        } catch (NumberFormatException nfe) {
            index--;
            throw nfe;
        }
    }

    /**
     * Reads a 64-bit integer from the stream message.
     *
     * @return a 64-bit integer value from the stream message, interpreted as a
     *         <code>long</code>
     * @throws JMSException
     *         if the JMS provider fails to read the message due to some
     *         internal error.
     * @throws MessageEOFException
     *         if unexpected end of message stream has been reached.
     * @throws MessageFormatException
     *         if this type conversion is invalid.
     * @throws MessageNotReadableException
     *         if the message is in write-only mode.
     */
    @Override
    public long readLong() throws JMSException {
        initializeReading();
        try {
            checkEndOfStream();

            Object value = stream.get(index++);
            if (value instanceof Long) {
                return (Long) value;
            }
            if (value instanceof Integer) {
                return ((Integer) value).longValue();
            }
            if (value instanceof Short) {
                return ((Short) value).longValue();
            }
            if (value instanceof Byte) {
                return ((Byte) value).longValue();
            }
            if (value instanceof String) {
                return Long.valueOf((String) value).shortValue();
            }

            index--;
            if (value == null) {
                throw new NullPointerException("Cannot convert NULL value to long.");
            } else {
                throw new MessageFormatException("stream value is not a boolean type");
            }
        } catch (NumberFormatException nfe) {
            index--;
            throw nfe;
        }
    }

    /**
     * Reads a <code>float</code> from the stream message.
     *
     * @return a <code>float</code> value from the stream message
     * @throws JMSException
     *         if the JMS provider fails to read the message due to some
     *         internal error.
     * @throws MessageEOFException
     *         if unexpected end of message stream has been reached.
     * @throws MessageFormatException
     *         if this type conversion is invalid.
     * @throws MessageNotReadableException
     *         if the message is in write-only mode.
     */
    @Override
    public float readFloat() throws JMSException {
        initializeReading();
        try {
            checkEndOfStream();

            Object value = stream.get(index++);
            if (value instanceof Float) {
                return (Float) value;
            }
            if (value instanceof String) {
                return Float.valueOf((String) value);
            }

            index--;
            if (value == null) {
                throw new NullPointerException("Cannot convert NULL value to float.");
            } else {
                throw new MessageFormatException("stream value is not a boolean type");
            }
        } catch (NumberFormatException nfe) {
            index--;
            throw nfe;
        }
    }

    /**
     * Reads a <code>double</code> from the stream message.
     *
     * @return a <code>double</code> value from the stream message
     * @throws JMSException
     *         if the JMS provider fails to read the message due to some
     *         internal error.
     * @throws MessageEOFException
     *         if unexpected end of message stream has been reached.
     * @throws MessageFormatException
     *         if this type conversion is invalid.
     * @throws MessageNotReadableException
     *         if the message is in write-only mode.
     */
    @Override
    public double readDouble() throws JMSException {
        initializeReading();
        try {
            checkEndOfStream();

            Object value = stream.get(index++);
            if (value instanceof Double) {
                return (Double) value;
            }
            if (value instanceof Float) {
                return (Float) value;
            }
            if (value instanceof String) {
                return Double.valueOf((String) value);
            }

            index--;
            if (value == null) {
                throw new NullPointerException("Cannot convert NULL value to double.");
            } else {
                throw new MessageFormatException("stream value is not a boolean type");
            }
        } catch (NumberFormatException nfe) {
            index--;
            throw nfe;
        }
    }

    /**
     * Reads a <CODE>String</CODE> from the stream message.
     *
     * @return a Unicode string from the stream message
     * @throws JMSException
     *         if the JMS provider fails to read the message due to some
     *         internal error.
     * @throws MessageEOFException
     *         if unexpected end of message stream has been reached.
     * @throws MessageFormatException
     *         if this type conversion is invalid.
     * @throws MessageNotReadableException
     *         if the message is in write-only mode.
     */
    @Override
    public String readString() throws JMSException {
        initializeReading();
        try {
            checkEndOfStream();

            Object value = stream.get(index++);
            if (value == null) {
                return null;
            }
            if (value instanceof String) {
                return (String) value;
            }
            if (value instanceof Double) {
                return ((Double) value).toString();
            }
            if (value instanceof Float) {
                return ((Float) value).toString();
            }
            if (value instanceof Long) {
                return ((Long) value).toString();
            }
            if (value instanceof Integer) {
                return ((Integer) value).toString();
            }
            if (value instanceof Short) {
                return ((Short) value).toString();
            }
            if (value instanceof Byte) {
                return ((Byte) value).toString();
            }
            if (value instanceof Character) {
                return ((Character) value).toString();
            }
            if (value instanceof Boolean) {
                return ((Boolean) value).toString();
            }

            index--;
            throw new MessageFormatException("Cannot convert byte array to String.");
        } catch (NumberFormatException nfe) {
            index--;
            throw nfe;
        }
    }

    /**
     * Reads a byte array field from the stream message into the specified
     * <CODE>byte[]</CODE> object (the read buffer).
     * <p/>
     * <p/>
     * To read the field value, <CODE>readBytes</CODE> should be successively
     * called until it returns a value less than the length of the read buffer.
     * The value of the bytes in the buffer following the last byte read is
     * undefined.
     * <p/>
     * <p/>
     * If <CODE>readBytes</CODE> returns a value equal to the length of the
     * buffer, a subsequent <CODE>readBytes</CODE> call must be made. If there
     * are no more bytes to be read, this call returns -1.
     * <p/>
     * <p/>
     * If the byte array field value is null, <CODE>readBytes</CODE> returns -1.
     * <p/>
     * <p/>
     * If the byte array field value is empty, <CODE>readBytes</CODE> returns 0.
     * <p/>
     * <p/>
     * Once the first <CODE>readBytes</CODE> call on a <CODE>byte[]</CODE> field
     * value has been made, the full value of the field must be read before it
     * is valid to read the next field. An attempt to read the next field before
     * that has been done will throw a <CODE>MessageFormatException</CODE>.
     * <p/>
     * <p/>
     * To read the byte field value into a new <CODE>byte[]</CODE> object, use
     * the <CODE>readObject</CODE> method.
     *
     * @param value
     *        the buffer into which the data is read
     * @return the total number of bytes read into the buffer, or -1 if there is
     *         no more data because the end of the byte field has been reached
     * @throws JMSException
     *         if the JMS provider fails to read the message due to some
     *         internal error.
     * @throws MessageEOFException
     *         if unexpected end of message stream has been reached.
     * @throws MessageFormatException
     *         if this type conversion is invalid.
     * @throws MessageNotReadableException
     *         if the message is in write-only mode.
     * @see #readObject()
     */
    @Override
    public int readBytes(byte[] value) throws JMSException {
        initializeReading();
        try {
            if (value == null) {
                throw new NullPointerException();
            }

            checkEndOfStream();

            if (remainingBytes == -1) {
                Object data = stream.get(index++);
                if (!(data instanceof Buffer)) {
                    throw new MessageFormatException("Not a byte array");
                }

                bytes = (Buffer) data;
                remainingBytes = bytes.length();
            } else if (remainingBytes == 0) {
                remainingBytes = -1;
                bytes = null;
                return -1;
            }

            if (value.length <= remainingBytes) {
                // small buffer
                remainingBytes -= value.length;
                System.arraycopy(bytes.data, bytes.offset, value, 0, value.length);
                bytes.offset += value.length;
                return value.length;
            } else {
                // big buffer
                System.arraycopy(bytes.data, bytes.offset, value, 0, remainingBytes);
                remainingBytes = 0;
                return bytes.length();
            }
        } catch (Throwable e) {
            throw JmsExceptionSupport.createMessageFormatException(e);
        }
    }

    /**
     * Reads an object from the stream message.
     * <p/>
     * <p/>
     * This method can be used to return, in objectified format, an object in
     * the Java programming language ("Java object") that has been written to
     * the stream with the equivalent <CODE>writeObject</CODE> method call, or
     * its equivalent primitive <CODE>write<I>type</I></CODE> method.
     * <p/>
     * <p/>
     * Note that byte values are returned as <CODE>byte[]</CODE>, not
     * <CODE>Byte[]</CODE>.
     * <p/>
     * <p/>
     * An attempt to call <CODE>readObject</CODE> to read a byte field value
     * into a new <CODE>byte[]</CODE> object before the full value of the byte
     * field has been read will throw a <CODE>MessageFormatException</CODE>.
     *
     * @return a Java object from the stream message, in objectified format (for
     *         example, if the object was written as an <CODE>int</CODE>, an
     *         <CODE>Integer</CODE> is returned)
     * @throws JMSException
     *         if the JMS provider fails to read the message due to some
     *         internal error.
     * @throws MessageEOFException
     *         if unexpected end of message stream has been reached.
     * @throws MessageFormatException
     *         if this type conversion is invalid.
     * @throws MessageNotReadableException
     *         if the message is in write-only mode.
     * @see #readBytes(byte[] value)
     */
    @Override
    public Object readObject() throws JMSException {
        initializeReading();
        try {
            checkEndOfStream();

            Object value = stream.get(index++);
            if (value == null) {
                return null;
            }
            if (value instanceof String) {
                return value;
            }
            if (value instanceof Double) {
                return value;
            }
            if (value instanceof Float) {
                return value;
            }
            if (value instanceof Long) {
                return value;
            }
            if (value instanceof Integer) {
                return value;
            }
            if (value instanceof Short) {
                return value;
            }
            if (value instanceof Byte) {
                return value;
            }
            if (value instanceof Character) {
                return value;
            }
            if (value instanceof Boolean) {
                return value;
            }
            if (value instanceof Buffer) {
                return ((Buffer) value).toByteArray();
            } else {
                index--;
                throw new MessageFormatException("Unknown type found in stream");
            }
        } catch (Exception ex) {
            index--;
            throw JmsExceptionSupport.create(ex);
        }
    }

    /**
     * Writes a <code>boolean</code> to the stream message. The value
     * <code>true</code> is written as the value <code>(byte)1</code>; the value
     * <code>false</code> is written as the value <code>(byte)0</code>.
     *
     * @param value
     *        the <code>boolean</code> value to be written
     * @throws JMSException
     *         if the JMS provider fails to write the message due to some
     *         internal error.
     * @throws MessageNotWriteableException
     *         if the message is in read-only mode.
     */
    @Override
    public void writeBoolean(boolean value) throws JMSException {
        initializeWriting();
        stream.add(value);
    }

    /**
     * Writes a <code>byte</code> to the stream message.
     *
     * @param value
     *        the <code>byte</code> value to be written
     * @throws JMSException
     *         if the JMS provider fails to write the message due to some
     *         internal error.
     * @throws MessageNotWriteableException
     *         if the message is in read-only mode.
     */
    @Override
    public void writeByte(byte value) throws JMSException {
        initializeWriting();
        stream.add(value);
    }

    /**
     * Writes a <code>short</code> to the stream message.
     *
     * @param value
     *        the <code>short</code> value to be written
     * @throws JMSException
     *         if the JMS provider fails to write the message due to some
     *         internal error.
     * @throws MessageNotWriteableException
     *         if the message is in read-only mode.
     */
    @Override
    public void writeShort(short value) throws JMSException {
        initializeWriting();
        stream.add(value);
    }

    /**
     * Writes a <code>char</code> to the stream message.
     *
     * @param value
     *        the <code>char</code> value to be written
     * @throws JMSException
     *         if the JMS provider fails to write the message due to some
     *         internal error.
     * @throws MessageNotWriteableException
     *         if the message is in read-only mode.
     */
    @Override
    public void writeChar(char value) throws JMSException {
        initializeWriting();
        stream.add(value);
    }

    /**
     * Writes an <code>int</code> to the stream message.
     *
     * @param value
     *        the <code>int</code> value to be written
     * @throws JMSException
     *         if the JMS provider fails to write the message due to some
     *         internal error.
     * @throws MessageNotWriteableException
     *         if the message is in read-only mode.
     */
    @Override
    public void writeInt(int value) throws JMSException {
        initializeWriting();
        stream.add(value);
    }

    /**
     * Writes a <code>long</code> to the stream message.
     *
     * @param value
     *        the <code>long</code> value to be written
     * @throws JMSException
     *         if the JMS provider fails to write the message due to some
     *         internal error.
     * @throws MessageNotWriteableException
     *         if the message is in read-only mode.
     */
    @Override
    public void writeLong(long value) throws JMSException {
        initializeWriting();
        stream.add(value);
    }

    /**
     * Writes a <code>float</code> to the stream message.
     *
     * @param value
     *        the <code>float</code> value to be written
     * @throws JMSException
     *         if the JMS provider fails to write the message due to some
     *         internal error.
     * @throws MessageNotWriteableException
     *         if the message is in read-only mode.
     */
    @Override
    public void writeFloat(float value) throws JMSException {
        initializeWriting();
        stream.add(value);
    }

    /**
     * Writes a <code>double</code> to the stream message.
     *
     * @param value
     *        the <code>double</code> value to be written
     * @throws JMSException
     *         if the JMS provider fails to write the message due to some
     *         internal error.
     * @throws MessageNotWriteableException
     *         if the message is in read-only mode.
     */
    @Override
    public void writeDouble(double value) throws JMSException {
        initializeWriting();
        stream.add(value);
    }

    /**
     * Writes a <code>String</code> to the stream message.
     *
     * @param value
     *        the <code>String</code> value to be written
     * @throws JMSException
     *         if the JMS provider fails to write the message due to some
     *         internal error.
     * @throws MessageNotWriteableException
     *         if the message is in read-only mode.
     */
    @Override
    public void writeString(String value) throws JMSException {
        initializeWriting();
        stream.add(value);
    }

    /**
     * Writes a byte array field to the stream message.
     * <p/>
     * <p/>
     * The byte array <code>value</code> is written to the message as a byte
     * array field. Consecutively written byte array fields are treated as two
     * distinct fields when the fields are read.
     *
     * @param value
     *        the byte array value to be written
     * @throws JMSException
     *         if the JMS provider fails to write the message due to some
     *         internal error.
     * @throws MessageNotWriteableException
     *         if the message is in read-only mode.
     */
    @Override
    public void writeBytes(byte[] value) throws JMSException {
        writeBytes(value, 0, value.length);
    }

    /**
     * Writes a portion of a byte array as a byte array field to the stream
     * message.
     * <p/>
     * <p/>
     * The a portion of the byte array <code>value</code> is written to the
     * message as a byte array field. Consecutively written byte array fields
     * are treated as two distinct fields when the fields are read.
     *
     * @param value
     *        the byte array value to be written
     * @param offset
     *        the initial offset within the byte array
     * @param length
     *        the number of bytes to use
     * @throws JMSException
     *         if the JMS provider fails to write the message due to some
     *         internal error.
     * @throws MessageNotWriteableException
     *         if the message is in read-only mode.
     */
    @Override
    public void writeBytes(byte[] value, int offset, int length) throws JMSException {
        initializeWriting();
        Buffer temp = new Buffer(value, offset, length);
        stream.add(temp.deepCopy());
    }

    /**
     * Writes an object to the stream message.
     * <p/>
     * <p/>
     * This method works only for the objectified primitive object types (
     * <code>Integer</code>, <code>Double</code>, <code>Long</code>&nbsp;...),
     * <code>String</code> objects, and byte arrays.
     *
     * @param value
     *        the Java object to be written
     * @throws JMSException
     *         if the JMS provider fails to write the message due to some
     *         internal error.
     * @throws MessageFormatException
     *         if the object is invalid.
     * @throws MessageNotWriteableException
     *         if the message is in read-only mode.
     */
    @Override
    public void writeObject(Object value) throws JMSException {
        initializeWriting();
        if (value == null) {
            stream.add(null);
        } else if (value instanceof String) {
            writeString(value.toString());
        } else if (value instanceof Character) {
            writeChar(((Character) value).charValue());
        } else if (value instanceof Boolean) {
            writeBoolean(((Boolean) value).booleanValue());
        } else if (value instanceof Byte) {
            writeByte(((Byte) value).byteValue());
        } else if (value instanceof Short) {
            writeShort(((Short) value).shortValue());
        } else if (value instanceof Integer) {
            writeInt(((Integer) value).intValue());
        } else if (value instanceof Float) {
            writeFloat(((Float) value).floatValue());
        } else if (value instanceof Double) {
            writeDouble(((Double) value).doubleValue());
        } else if (value instanceof byte[]) {
            writeBytes((byte[]) value);
        } else if (value instanceof Long) {
            writeLong(((Long) value).longValue());
        } else {
            throw new MessageFormatException("Unsupported Object type: " + value.getClass());
        }
    }

    /**
     * Puts the message body in read-only mode and repositions the stream of
     * bytes to the beginning.
     *
     * @throws JMSException
     *         if an internal error occurs
     */
    @Override
    public void reset() throws JMSException {
        stream = null;
        index = 0;
        bytes = null;
        remainingBytes = -1;
        setReadOnlyBody(true);
    }

    private void initializeWriting() throws MessageNotWriteableException {
        checkReadOnlyBody();
        if (stream == null) {
            content.clear();
            stream = content;
        }
    }

    protected void checkWriteOnlyBody() throws MessageNotReadableException {
        if (!readOnlyBody) {
            throw new MessageNotReadableException("Message body is write-only");
        }
    }

    private void initializeReading() throws MessageNotReadableException {
        checkWriteOnlyBody();
        if (stream == null) {
            stream = content;
            index = 0;
            bytes = null;
            remainingBytes = -1;
        }
    }

    private void checkEndOfStream() throws MessageEOFException {
        if (stream.isEmpty() || index >= stream.size()) {
            throw new MessageEOFException("Cannot read beyond end of stream");
        }
    }

    @Override
    public String toString() {
        return super.toString() + " JmsStreamMessage{ " + content + " }";
    }
}
