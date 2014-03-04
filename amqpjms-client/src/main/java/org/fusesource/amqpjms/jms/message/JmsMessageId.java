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

import org.fusesource.amqpjms.jms.meta.JmsProducerId;
import org.fusesource.amqpjms.jms.meta.JmsProducerInfo;

/**
 * JMS Message Id class used to uniquely identify messages for the JMS Framework.
 */
public class JmsMessageId implements Comparable<JmsMessageId> {

    protected String textView;
    protected JmsProducerId producerId;
    protected long producerSequenceId;

    private transient String key;
    private transient int hashCode;
    private transient Object providerHint;

    public JmsMessageId(JmsProducerInfo producerInfo, long producerSequenceId) {
        this.producerId = producerInfo.getProducerId();
        this.producerSequenceId = producerSequenceId;
    }

    public JmsMessageId(String messageKey) {
        setValue(messageKey);
    }

    public JmsMessageId(String producerId, long producerSequenceId) {
        this(new JmsProducerId(producerId), producerSequenceId);
    }

    public JmsMessageId(JmsProducerId producerId, long producerSequenceId) {
        this.producerId = producerId;
        this.producerSequenceId = producerSequenceId;
    }

    private JmsMessageId() {
    }

    public static JmsMessageId wrapForeignMessageId(String view) {
        JmsMessageId id = new JmsMessageId();
        id.setTextView(view);
        return id;
    }

    public JmsMessageId copy() {
        JmsMessageId copy = new JmsMessageId(producerId, producerSequenceId);
        copy.key = key;
        copy.textView = textView;
        return copy;
    }

    /**
     * Sets the value as a String
     */
    public void setValue(String messageKey) {
        key = messageKey;
        // Parse off the sequenceId
        int p = messageKey.lastIndexOf(":");
        if (p >= 0) {
            producerSequenceId = Long.parseLong(messageKey.substring(p + 1));
            messageKey = messageKey.substring(0, p);
        } else {
            throw new NumberFormatException();
        }
        producerId = new JmsProducerId(messageKey);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || o.getClass() != getClass()) {
            return false;
        }

        JmsMessageId id = (JmsMessageId) o;
        return producerSequenceId == id.producerSequenceId && producerId.equals(id.producerId);
    }

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            hashCode = producerId.hashCode() ^ (int) producerSequenceId;
        }
        return hashCode;
    }

    public String toProducerKey() {
        if (textView == null) {
            return toString();
        } else {
            return producerId.toString() + ":" + producerSequenceId;
        }
    }

    @Override
    public int compareTo(JmsMessageId other) {
        int result = -1;
        if (other != null) {
            result = this.toString().compareTo(other.toString());
        }
        return result;
    }

    @Override
    public String toString() {
        if (key == null) {
            if (textView != null) {
                if (textView.startsWith("ID:")) {
                    key = textView;
                } else {
                    key = "ID:" + textView;
                }
            } else {
                key = producerId.toString() + ":" + producerSequenceId;
            }
        }
        return key;
    }

    /**
     * Sets the transient text view of the message which will be ignored if the message is
     * marshaled on a transport; so is only for in-JVM changes to accommodate foreign JMS
     * message IDs
     */
    public void setTextView(String key) {
        this.textView = key;
    }

    public String getTextView() {
        return textView;
    }

    public JmsProducerId getProducerId() {
        return producerId;
    }

    public void setProducerId(JmsProducerId producerId) {
        this.producerId = producerId;
    }

    public long getProducerSequenceId() {
        return producerSequenceId;
    }

    public void setProducerSequenceId(long producerSequenceId) {
        this.producerSequenceId = producerSequenceId;
    }

    public void setProviderHint(Object hint) {
        this.providerHint = hint;
    }

    public Object getProviderHint() {
        return this.providerHint;
    }
}
