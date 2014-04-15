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
import io.hawtjms.util.ClassLoadingAwareObjectInputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;

import org.fusesource.hawtbuf.Buffer;

/**
 * An <CODE>ObjectMessage</CODE> object is used to send a message that contains a serializable
 * object in the Java programming language ("Java object"). It inherits from the
 * <CODE>Message</CODE> interface and adds a body containing a single reference to an object.
 * Only <CODE>Serializable</CODE> Java objects can be used.
 * <p/>
 * <p/>
 * If a collection of Java objects must be sent, one of the <CODE>Collection</CODE> classes
 * provided since JDK 1.2 can be used.
 * <p/>
 * <p/>
 * When a client receives an <CODE>ObjectMessage</CODE>, it is in read-only mode. If a client
 * attempts to write to the message at this point, a <CODE>MessageNotWriteableException</CODE>
 * is thrown. If <CODE>clearBody</CODE> is called, the message can now be both read from and
 * written to.
 *
 * @see javax.jms.Session#createObjectMessage()
 * @see javax.jms.Session#createObjectMessage(Serializable)
 * @see javax.jms.BytesMessage
 * @see javax.jms.MapMessage
 * @see javax.jms.Message
 * @see javax.jms.StreamMessage
 * @see javax.jms.TextMessage
 */
public class JmsObjectMessage extends JmsMessage implements ObjectMessage {

    protected transient Serializable object;
    protected Buffer content;

    @Override
    public JmsMessage copy() throws JMSException {
        JmsObjectMessage other = new JmsObjectMessage();
        other.copy(this);
        return other;
    }

    private void copy(JmsObjectMessage other) throws JMSException {
        other.storeContent();
        super.copy(other);
        this.object = null;
        if (other.content != null) {
            this.content = other.content.deepCopy();
        } else {
            this.content = null;
        }
    }

    public Buffer getContent() {
        return content;
    }

    public void setContent(Buffer content) {
        this.content = content;
    }

    @Override
    public void storeContent() throws JMSException {
        Buffer buffer = getContent();
        if (buffer == null && object != null) {
            try {
                ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
                OutputStream os = bytesOut;
                DataOutputStream dataOut = new DataOutputStream(os);
                ObjectOutputStream objOut = new ObjectOutputStream(dataOut);
                objOut.writeObject(object);
                objOut.flush();
                objOut.reset();
                objOut.close();
                setContent(new Buffer(bytesOut.toByteArray()));
            } catch (IOException ioe) {
                throw new RuntimeException(ioe.getMessage(), ioe);
            }
        }
    }

    /**
     * Clears out the message body. Clearing a message's body does not clear its header values
     * or property entries.
     * <p/>
     * <p/>
     * If this message body was read-only, calling this method leaves the message body in the
     * same state as an empty body in a newly created message.
     *
     * @throws JMSException
     *         if the JMS provider fails to clear the message body due to some internal error.
     */
    @Override
    public void clearBody() throws JMSException {
        super.clearBody();
        this.object = null;
        this.content = null;
    }

    /**
     * Sets the serializable object containing this message's data. It is important to note that
     * an <CODE>ObjectMessage</CODE> contains a snapshot of the object at the time
     * <CODE>setObject()</CODE> is called; subsequent modifications of the object will have no
     * effect on the <CODE>ObjectMessage</CODE> body.
     *
     * @param newObject
     *        the message's data
     * @throws JMSException
     *         if the JMS provider fails to set the object due to some internal error.
     * @throws javax.jms.MessageFormatException
     *         if object serialization fails.
     * @throws javax.jms.MessageNotWriteableException
     *         if the message is in read-only mode.
     */
    @Override
    public void setObject(Serializable newObject) throws JMSException {
        checkReadOnlyBody();
        this.object = newObject;
        setContent(null);
        storeContent();
    }

    /**
     * Gets the serializable object containing this message's data. The default value is null.
     *
     * @return the serializable object containing this message's data
     * @throws JMSException
     */
    @Override
    public Serializable getObject() throws JMSException {
        Buffer buffer = getContent();
        if (this.object == null && buffer != null) {
            try {
                Buffer content = getContent();
                InputStream is = new ByteArrayInputStream(content.toByteArray());
                DataInputStream dataIn = new DataInputStream(is);
                ClassLoadingAwareObjectInputStream objIn = new ClassLoadingAwareObjectInputStream(dataIn);
                try {
                    object = (Serializable)objIn.readObject();
                } catch (ClassNotFoundException ce) {
                    throw JmsExceptionSupport.create("Failed to build body from content. Serializable class not available to broker. Reason: " + ce, ce);
                } finally {
                    objIn.close();
                    dataIn.close();
                }
            } catch (IOException e) {
                throw JmsExceptionSupport.create("Failed to build body from bytes. Reason: " + e, e);
            }
        }
        return this.object;
    }

    @Override
    public String toString() {
        try {
            getObject();
        } catch (JMSException e) {
        }
        return super.toString();
    }
}
