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

import io.hawtjms.jms.JmsConnection;
import io.hawtjms.jms.JmsDestination;
import io.hawtjms.jms.exceptions.JmsExceptionSupport;
import io.hawtjms.jms.meta.JmsMessageId;
import io.hawtjms.util.PropertyExpression;
import io.hawtjms.util.TypeConversionSupport;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.Callable;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;

public class JmsMessage implements javax.jms.Message {

    private static final Map<String, PropertySetter> JMS_PROPERTY_SETERS =
        new HashMap<String, PropertySetter>();

    protected transient Callable<Void> acknowledgeCallback;
    protected transient JmsConnection connection;

    protected final JmsMessageFacade facade;
    protected boolean readOnlyBody;
    protected boolean readOnlyProperties;

    public JmsMessage(JmsMessageFacade facade) {
        this.facade = facade;
    }

    public JmsMessage copy() throws JMSException {
        JmsMessage other = new JmsMessage(facade.copy());
        other.copy(this);
        return other;
    }

    protected void copy(JmsMessage other) {
        this.readOnlyBody = other.readOnlyBody;
        this.readOnlyProperties = other.readOnlyBody;
        this.acknowledgeCallback = other.acknowledgeCallback;
        this.connection = other.connection;
    }

    @Override
    public int hashCode() {
        String id = getJMSMessageID();
        if (id != null) {
            return id.hashCode();
        } else {
            return super.hashCode();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || o.getClass() != getClass()) {
            return false;
        }

        JmsMessage msg = (JmsMessage) o;
        JmsMessageId oMsg = msg.facade.getMessageId();
        JmsMessageId thisMsg = facade.getMessageId();
        return thisMsg != null && oMsg != null && oMsg.equals(thisMsg);
    }

    @Override
    public void acknowledge() throws JMSException {
        if (acknowledgeCallback != null) {
            try {
                acknowledgeCallback.call();
            } catch (Throwable e) {
                throw JmsExceptionSupport.create(e);
            }
        }
    }

    @Override
    public void clearBody() throws JMSException {
        readOnlyBody = false;
    }

    public boolean isReadOnlyBody() {
        return this.readOnlyBody;
    }

    public void setReadOnlyBody(boolean readOnlyBody) {
        this.readOnlyBody = readOnlyBody;
    }

    public void setReadOnlyProperties(boolean readOnlyProperties) {
        this.readOnlyProperties = readOnlyProperties;
    }

    @Override
    public String getJMSMessageID() {
        if (facade.getMessageId() == null) {
            return null;
        }
        return facade.getMessageId().toString();
    }

    @Override
    public void setJMSMessageID(String value) {
        if (value != null) {
            JmsMessageId id = new JmsMessageId(value);
            facade.setMessageId(id);
        } else {
            facade.setMessageId(null);
        }
    }

    public void setJMSMessageID(JmsMessageId messageId) {
        facade.setMessageId(messageId);
    }

    @Override
    public long getJMSTimestamp() {
        return facade.getTimestamp();
    }

    @Override
    public void setJMSTimestamp(long timestamp) {
        facade.setTimestamp(timestamp);
    }

    @Override
    public String getJMSCorrelationID() {
        return facade.getCorrelationId();
    }

    @Override
    public void setJMSCorrelationID(String correlationId) {
        facade.setCorrelationId(correlationId);
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
        return encodeString(facade.getCorrelationId());
    }

    @Override
    public void setJMSCorrelationIDAsBytes(byte[] correlationId) throws JMSException {
        facade.setCorrelationId(decodeString(correlationId));
    }

    @Override
    public Destination getJMSReplyTo() throws JMSException {
        return facade.getReplyTo();
    }

    @Override
    public void setJMSReplyTo(Destination destination) throws JMSException {
        facade.setReplyTo(JmsMessageTransformation.transformDestination(connection, destination));
    }

    @Override
    public Destination getJMSDestination() throws JMSException {
        return facade.getDestination();
    }

    @Override
    public void setJMSDestination(Destination destination) throws JMSException {
        facade.setDestination(JmsMessageTransformation.transformDestination(connection, destination));
    }

    @Override
    public int getJMSDeliveryMode() {
        return facade.isPersistent() ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
    }

    @Override
    public void setJMSDeliveryMode(int mode) {
        facade.setPersistent(mode == DeliveryMode.PERSISTENT);
    }

    @Override
    public boolean getJMSRedelivered() {
        return this.isRedelivered();
    }

    @Override
    public void setJMSRedelivered(boolean redelivered) {
        this.setRedelivered(redelivered);
    }

    @Override
    public String getJMSType() {
        return facade.getType();
    }

    @Override
    public void setJMSType(String type) {
        facade.setType(type);
    }

    @Override
    public long getJMSExpiration() {
        return facade.getExpiration();
    }

    @Override
    public void setJMSExpiration(long expiration) {
        facade.setExpiration(expiration);
    }

    @Override
    public int getJMSPriority() {
        return facade.getPriority();
    }

    @Override
    public void setJMSPriority(int priority) {
        byte scaled = 0;

        if (priority < 0) {
            scaled = 0;
        } else if (priority > 9) {
            scaled = 9;
        } else {
            scaled = (byte) priority;
        }

        facade.setPriority(scaled);
    }

    @Override
    public void clearProperties() {
        facade.clearProperties();
    }

    @Override
    public boolean propertyExists(String name) throws JMSException {
        try {
            return (facade.propertyExists(name) || getObjectProperty(name) != null);
        } catch (Exception e) {
            throw JmsExceptionSupport.create(e);
        }
    }

    /**
     * Returns an unmodifiable Map containing the properties contained within the message.
     *
     * @return unmodifiable Map of the current properties in the message.
     *
     * @throws Exception if there is an error accessing the message properties.
     */
    public Map<String, Object> getProperties() throws IOException {
        return Collections.unmodifiableMap(facade.getProperties());
    }

    /**
     * Allows for a direct put of an Object value into the message properties.
     *
     * This method bypasses the normal JMS type checking for properties being set on
     * the message and should be used with great care.
     *
     * @param key
     *        the property name to use when setting the value.
     * @param value
     *        the value to insert into the message properties.
     *
     * @throws IOException if an error occurs while accessing the Message properties.
     */
    public void setProperty(String key, Object value) throws IOException {
        this.facade.setProperty(key, value);
    }

    /**
     * Returns the Object value referenced by the given key.
     *
     * @param key
     *        the name of the property being accessed.
     *
     * @return the value stored at the given location or null if non set.
     *
     * @throws IOException if an error occurs while accessing the Message properties.
     */
    public Object getProperty(String key) throws IOException {
        return this.facade.getProperty(key);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Enumeration getPropertyNames() throws JMSException {
        try {
            Vector<String> result = new Vector<String>(facade.getProperties().keySet());
            if (getFacade().getRedeliveryCounter() != 0) {
                result.add("JMSXDeliveryCount");
            }
            if (getFacade().getGroupId() != null) {
                result.add("JMSXGroupID");
            }
            if (getFacade().getGroupId() != null) {
                result.add("JMSXGroupSeq");
            }
            if (getFacade().getUserId() != null) {
                result.add("JMSXUserID");
            }
            return result.elements();
        } catch (IOException e) {
            throw JmsExceptionSupport.create(e);
        }
    }

    /**
     * return all property names, including standard JMS properties and JMSX
     * properties
     *
     * @return Enumeration of all property names on this message
     * @throws JMSException
     */
    @SuppressWarnings("rawtypes")
    public Enumeration getAllPropertyNames() throws JMSException {
        try {
            Vector<String> result = new Vector<String>(facade.getProperties().keySet());
            result.addAll(JMS_PROPERTY_SETERS.keySet());
            return result.elements();
        } catch (IOException e) {
            throw JmsExceptionSupport.create(e);
        }
    }

    interface PropertySetter {
        void set(JmsConnection connection, JmsMessage message, Object value) throws MessageFormatException;
    }

    static {
        JMS_PROPERTY_SETERS.put("JMSXDeliveryCount", new PropertySetter() {
            @Override
            public void set(JmsConnection connection, JmsMessage message, Object value) throws MessageFormatException {
                Integer rc = (Integer) TypeConversionSupport.convert(connection, value, Integer.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSXDeliveryCount cannot be set from a " + value.getClass().getName() + ".");
                }
                message.getFacade().setRedeliveryCounter(rc.intValue() - 1);
            }
        });
        JMS_PROPERTY_SETERS.put("JMSXGroupID", new PropertySetter() {
            @Override
            public void set(JmsConnection connection, JmsMessage message, Object value) throws MessageFormatException {
                String rc = (String) TypeConversionSupport.convert(connection, value, String.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSXGroupID cannot be set from a " + value.getClass().getName() + ".");
                }
                message.getFacade().setGroupId(rc);
            }
        });
        JMS_PROPERTY_SETERS.put("JMSXGroupSeq", new PropertySetter() {
            @Override
            public void set(JmsConnection connection, JmsMessage message, Object value) throws MessageFormatException {
                Integer rc = (Integer) TypeConversionSupport.convert(connection, value, Integer.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSXGroupSeq cannot be set from a " + value.getClass().getName() + ".");
                }
                message.getFacade().setGroupSequence(rc.intValue());
            }
        });
        JMS_PROPERTY_SETERS.put("JMSCorrelationID", new PropertySetter() {
            @Override
            public void set(JmsConnection connection, JmsMessage message, Object value) throws MessageFormatException {
                String rc = (String) TypeConversionSupport.convert(connection, value, String.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSCorrelationID cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setJMSCorrelationID(rc);
            }
        });
        JMS_PROPERTY_SETERS.put("JMSDeliveryMode", new PropertySetter() {
            @Override
            public void set(JmsConnection connection, JmsMessage message, Object value) throws MessageFormatException {
                Integer rc = (Integer) TypeConversionSupport.convert(connection, value, Integer.class);
                if (rc == null) {
                    Boolean bool = (Boolean) TypeConversionSupport.convert(connection, value, Boolean.class);
                    if (bool == null) {
                        throw new MessageFormatException("Property JMSDeliveryMode cannot be set from a " + value.getClass().getName() + ".");
                    } else {
                        rc = bool.booleanValue() ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
                    }
                }
                message.setJMSDeliveryMode(rc);
            }
        });
        JMS_PROPERTY_SETERS.put("JMSExpiration", new PropertySetter() {
            @Override
            public void set(JmsConnection connection, JmsMessage message, Object value) throws MessageFormatException {
                Long rc = (Long) TypeConversionSupport.convert(connection, value, Long.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSExpiration cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setJMSExpiration(rc.longValue());
            }
        });
        JMS_PROPERTY_SETERS.put("JMSPriority", new PropertySetter() {
            @Override
            public void set(JmsConnection connection, JmsMessage message, Object value) throws MessageFormatException {
                Integer rc = (Integer) TypeConversionSupport.convert(connection, value, Integer.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSPriority cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setJMSPriority(rc.intValue());
            }
        });
        JMS_PROPERTY_SETERS.put("JMSRedelivered", new PropertySetter() {
            @Override
            public void set(JmsConnection connection, JmsMessage message, Object value) throws MessageFormatException {
                Boolean rc = (Boolean) TypeConversionSupport.convert(connection, value, Boolean.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSRedelivered cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setJMSRedelivered(rc.booleanValue());
            }
        });
        JMS_PROPERTY_SETERS.put("JMSReplyTo", new PropertySetter() {
            @Override
            public void set(JmsConnection connection, JmsMessage message, Object value) throws MessageFormatException {
                JmsDestination rc = (JmsDestination) TypeConversionSupport.convert(connection, value, JmsDestination.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSReplyTo cannot be set from a " + value.getClass().getName() + ".");
                }
                message.getFacade().setReplyTo(rc);
            }
        });
        JMS_PROPERTY_SETERS.put("JMSTimestamp", new PropertySetter() {
            @Override
            public void set(JmsConnection connection, JmsMessage message, Object value) throws MessageFormatException {
                Long rc = (Long) TypeConversionSupport.convert(connection, value, Long.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSTimestamp cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setJMSTimestamp(rc.longValue());
            }
        });
        JMS_PROPERTY_SETERS.put("JMSType", new PropertySetter() {
            @Override
            public void set(JmsConnection connection, JmsMessage message, Object value) throws MessageFormatException {
                String rc = (String) TypeConversionSupport.convert(connection, value, String.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSType cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setJMSType(rc);
            }
        });
    }

    @Override
    public void setObjectProperty(String name, Object value) throws JMSException {
        setObjectProperty(name, value, true);
    }

    public void setObjectProperty(String name, Object value, boolean checkReadOnly) throws JMSException {

        if (checkReadOnly) {
            checkReadOnlyProperties();
        }
        if (name == null || name.equals("")) {
            throw new IllegalArgumentException("Property name cannot be empty or null");
        }

        checkValidObject(value);
        PropertySetter setter = JMS_PROPERTY_SETERS.get(name);

        if (setter != null && value != null) {
            setter.set(connection, this, value);
        } else {
            try {
                facade.setProperty(name, value);
            } catch (Exception e) {
                throw JmsExceptionSupport.create(e);
            }
        }
    }

    public void setProperties(Map<String, Object> properties) throws JMSException {
        for (Iterator<Map.Entry<String, Object>> iter = properties.entrySet().iterator(); iter.hasNext();) {
            Map.Entry<String, Object> entry = iter.next();
            setObjectProperty(entry.getKey(), entry.getValue());
        }
    }

    protected void checkValidObject(Object value) throws MessageFormatException {
        boolean valid = value instanceof Boolean || value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long;
        valid = valid || value instanceof Float || value instanceof Double || value instanceof Character || value instanceof String || value == null;

        if (!valid) {
            throw new MessageFormatException("Only objectified primitive objects and String types are allowed but was: " + value + " type: " + value.getClass());
        }
    }

    @Override
    public Object getObjectProperty(String name) throws JMSException {
        if (name == null) {
            throw new NullPointerException("Property name cannot be null");
        }

        // PropertyExpression handles converting message headers to properties.
        PropertyExpression expression = new PropertyExpression(name);
        return expression.evaluate(this);
    }

    @Override
    public boolean getBooleanProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            return false;
        }
        Boolean rc = (Boolean) TypeConversionSupport.convert(connection, value, Boolean.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a boolean");
        }
        return rc.booleanValue();
    }

    @Override
    public byte getByteProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            throw new NumberFormatException("property " + name + " was null");
        }
        Byte rc = (Byte) TypeConversionSupport.convert(connection, value, Byte.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a byte");
        }
        return rc.byteValue();
    }

    @Override
    public short getShortProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            throw new NumberFormatException("property " + name + " was null");
        }
        Short rc = (Short) TypeConversionSupport.convert(connection, value, Short.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a short");
        }
        return rc.shortValue();
    }

    @Override
    public int getIntProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            throw new NumberFormatException("property " + name + " was null");
        }
        Integer rc = (Integer) TypeConversionSupport.convert(connection, value, Integer.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as an integer");
        }
        return rc.intValue();
    }

    @Override
    public long getLongProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            throw new NumberFormatException("property " + name + " was null");
        }
        Long rc = (Long) TypeConversionSupport.convert(connection, value, Long.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a long");
        }
        return rc.longValue();
    }

    @Override
    public float getFloatProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            throw new NullPointerException("property " + name + " was null");
        }
        Float rc = (Float) TypeConversionSupport.convert(connection, value, Float.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a float");
        }
        return rc.floatValue();
    }

    @Override
    public double getDoubleProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            throw new NullPointerException("property " + name + " was null");
        }
        Double rc = (Double) TypeConversionSupport.convert(connection, value, Double.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a double");
        }
        return rc.doubleValue();
    }

    @Override
    public String getStringProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            return null;
        }
        String rc = (String) TypeConversionSupport.convert(connection, value, String.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a String");
        }
        return rc;
    }

    @Override
    public void setBooleanProperty(String name, boolean value) throws JMSException {
        setBooleanProperty(name, value, true);
    }

    public void setBooleanProperty(String name, boolean value, boolean checkReadOnly) throws JMSException {
        setObjectProperty(name, Boolean.valueOf(value), checkReadOnly);
    }

    @Override
    public void setByteProperty(String name, byte value) throws JMSException {
        setObjectProperty(name, Byte.valueOf(value));
    }

    @Override
    public void setShortProperty(String name, short value) throws JMSException {
        setObjectProperty(name, Short.valueOf(value));
    }

    @Override
    public void setIntProperty(String name, int value) throws JMSException {
        setObjectProperty(name, Integer.valueOf(value));
    }

    @Override
    public void setLongProperty(String name, long value) throws JMSException {
        setObjectProperty(name, Long.valueOf(value));
    }

    @Override
    public void setFloatProperty(String name, float value) throws JMSException {
        setObjectProperty(name, new Float(value));
    }

    @Override
    public void setDoubleProperty(String name, double value) throws JMSException {
        setObjectProperty(name, new Double(value));
    }

    @Override
    public void setStringProperty(String name, String value) throws JMSException {
        setObjectProperty(name, value);
    }

    public Callable<Void> getAcknowledgeCallback() {
        return acknowledgeCallback;
    }

    public void setAcknowledgeCallback(Callable<Void> acknowledgeCallback) {
        this.acknowledgeCallback = acknowledgeCallback;
    }

    /**
     * Send operation event listener. Used to get the message ready to be sent.
     *
     * @throws JMSException
     */
    public void onSend() throws JMSException {
        setReadOnlyBody(true);
        setReadOnlyProperties(true);
        facade.onSend();
    }

    public JmsConnection getConnection() {
        return connection;
    }

    public void setConnection(JmsConnection connection) {
        this.connection = connection;
    }

    public boolean isExpired() {
        long expireTime = facade.getExpiration();
        return expireTime > 0 && System.currentTimeMillis() > expireTime;
    }

    public void incrementRedeliveryCount() {
         facade.setRedeliveryCounter(facade.getRedeliveryCounter() + 1);
    }

    public JmsMessageFacade getFacade() {
        return this.facade;
    }

    public boolean isRedelivered() {
        return facade.getRedeliveryCounter() > 0;
    }

    public void setRedelivered(boolean redelivered) {
        if (redelivered) {
            if (!isRedelivered()) {
                facade.setRedeliveryCounter(1);
            }
        } else {
            if (isRedelivered()) {
                facade.setRedeliveryCounter(0);
            }
        }
    }

    protected static String decodeString(byte[] data) throws JMSException {
        try {
            if (data == null) {
                return null;
            }
            return new String(data, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new JMSException("Invalid UTF-8 encoding: " + e.getMessage());
        }
    }

    protected static byte[] encodeString(String data) throws JMSException {
        try {
            if (data == null) {
                return null;
            }
            return data.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new JMSException("Invalid UTF-8 encoding: " + e.getMessage());
        }
    }

    protected void checkReadOnlyProperties() throws MessageNotWriteableException {
        if (readOnlyProperties) {
            throw new MessageNotWriteableException("Message properties are read-only");
        }
    }

    protected void checkReadOnlyBody() throws MessageNotWriteableException {
        if (readOnlyBody) {
            throw new MessageNotWriteableException("Message body is read-only");
        }
    }
}
