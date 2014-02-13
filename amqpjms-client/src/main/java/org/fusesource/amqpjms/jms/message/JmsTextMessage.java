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
package org.fusesource.amqpjms.jms.message;

import java.io.UnsupportedEncodingException;

import javax.jms.JMSException;
import javax.jms.MessageNotWriteableException;
import javax.jms.TextMessage;

import org.fusesource.amqpjms.jms.JmsExceptionSupport;
import org.fusesource.hawtbuf.Buffer;

public class JmsTextMessage extends JmsMessage implements TextMessage {

    protected String text;

    @Override
    public JmsMsgType getMsgType() {
        if (text == null) {
            return JmsMsgType.TEXT_NULL;
        } else {
            return JmsMsgType.TEXT;
        }
    }

    @Override
    public JmsMessage copy() throws JMSException {
        JmsTextMessage other = new JmsTextMessage();
        other.copy(this);
        return other;
    }

    private void copy(JmsTextMessage other) {
        super.copy(other);
        this.text = other.text;
    }

    @Override
    public void setText(String text) throws MessageNotWriteableException {
        checkReadOnlyBody();
        this.text = text;
        setContent(null);
    }

    @Override
    public String getText() throws JMSException {
        Buffer buffer = getContent();
        if (text == null && buffer != null) {
            this.text = new String(buffer.getData(), buffer.getOffset(), buffer.getLength());
            setContent(null);
        }
        return text;
    }

    @Override
    public void storeContent() throws JMSException {
        try {
            if (text == null) {
                setContent(new Buffer(0));
            } else {
                setContent(new Buffer(text.getBytes("UTF-8")));
            }
        } catch (UnsupportedEncodingException e) {
            throw JmsExceptionSupport.create(e.getMessage(), e);
        }
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
        this.text = null;
    }

    @Override
    public String toString() {
        return super.toString() + ":text=" + text;
    }
}
