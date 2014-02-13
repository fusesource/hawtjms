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
package org.fusesource.amqpjms.jms;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

import javax.jms.JMSException;

import org.fusesource.amqpjms.jms.jndi.JNDIStorable;
import org.fusesource.hawtbuf.AsciiBuffer;

/**
 * Jms Destination
 */
public class JmsDestination extends JNDIStorable implements Externalizable, javax.jms.Destination, Comparable<JmsDestination> {

    protected transient String prefix;
    protected transient String name;
    protected transient boolean topic;
    protected transient boolean temporary;
    protected transient int hashValue;
    protected transient String toString;
    protected transient AsciiBuffer buffer;
    protected transient Map<String, String> subscribeHeaders;

    protected transient JmsConnection connection;

    public JmsDestination() {
    }

    public JmsDestination(String name) {
        this("", name);
    }

    public JmsDestination(String prefix, String name) {
        this.prefix = prefix;
        setName(name);
    }

    public JmsDestination copy() {
        final JmsDestination copy = new JmsDestination();
        copy.setProperties(getProperties());
        return copy;
    }

    @Override
    public String toString() {
        if (toString == null) {
            toString = getPrefix() + getName();
        }
        return toString;
    }

    public AsciiBuffer toBuffer() {
        if (buffer == null) {
            // TODO
            // buffer = StompFrame.encodeHeader(toString());
        }
        return buffer;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    /**
     * @return name of destination
     */
    public String getName() {
        return this.name;
    }

    private void setName(String name) {
        this.name = name;
        this.toString = null;
        this.buffer = null;
    }

    /**
     * @return the topic
     */
    public boolean isTopic() {
        return this.topic;
    }

    /**
     * @return the temporary
     */
    public boolean isTemporary() {
        return this.temporary;
    }

    /**
     * @return true if a Topic
     */
    public boolean isQueue() {
        return !this.topic;
    }

    /**
     * @param props
     */
    @Override
    protected void buildFromProperties(Map<String, String> props) {
        setPrefix(getProperty(props, "prefix", ""));
        setName(getProperty(props, "name", ""));
        Boolean bool = Boolean.valueOf(getProperty(props, "topic", Boolean.TRUE.toString()));
        this.topic = bool.booleanValue();
        bool = Boolean.valueOf(getProperty(props, "temporary", Boolean.FALSE.toString()));
        this.temporary = bool.booleanValue();
    }

    /**
     * @param props
     */
    @Override
    protected void populateProperties(Map<String, String> props) {
        props.put("prefix", getPrefix());
        props.put("name", getName());
        props.put("topic", Boolean.toString(isTopic()));
        props.put("temporary", Boolean.toString(isTemporary()));
    }

    /**
     * @param other
     *        the Object to be compared.
     * @return a negative integer, zero, or a positive integer as this object is
     *         less than, equal to, or greater than the specified object.
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(JmsDestination other) {
        if (other != null) {
            if (isTemporary() == other.isTemporary()) {
                return getName().compareTo(other.getName());
            }
            return -1;
        }
        return -1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JmsDestination d = (JmsDestination) o;
        return getName().equals(d.getName());
    }

    @Override
    public int hashCode() {
        if (hashValue == 0) {
            hashValue = getName().hashCode();
        }
        return hashValue;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(getPrefix());
        out.writeUTF(getName());
        out.writeBoolean(isTopic());
        out.writeBoolean(isTemporary());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        setPrefix(in.readUTF());
        setName(in.readUTF());
        this.topic = in.readBoolean();
        this.temporary = in.readBoolean();
    }

    public static JmsDestination createDestination(JmsConnection connection, String name) throws JMSException {

        // TODO
        // JmsDestination x = connection.isTemporaryQueue(name);
        // if (x != null) {
        // return x;
        // }
        // x = connection.isTemporaryTopic(name);
        // if (x != null) {
        // return x;
        // }

        if (name.startsWith(connection.getTopicPrefix())) {
            return new JmsTopic(connection, name.substring(connection.getTopicPrefix().length()));
        }

        if (name.startsWith(connection.getQueuePrefix())) {
            return new JmsQueue(connection, name.substring(connection.getQueuePrefix().length()));
        }

        return new JmsDestination("", name);
    }

    public Map<String, String> getSubscribeHeaders() {
        return subscribeHeaders;
    }

    public void setSubscribeHeaders(Map<String, String> subscribeHeaders) {
        this.subscribeHeaders = subscribeHeaders;
    }

    void setConnection(JmsConnection connection) {
        this.connection = connection;
    }

    JmsConnection getConnection() {
        return this.connection;
    }

    /**
     * Attempts to delete the destination if there is an assigned Connection object.
     *
     * @throws JMSException if an error occurs or the provider doesn't support
     *         delete of destinations from the client.
     */
    protected void tryDelete() throws JMSException {
        if (connection != null) {
            connection.deleteDestination(this);
        }
    }
}
