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
package io.hawtjms.util;

import io.hawtjms.jms.JmsDestination;
import io.hawtjms.jms.exceptions.JmsExceptionSupport;
import io.hawtjms.jms.message.JmsMessage;

import java.util.HashMap;
import java.util.Map;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;

/**
 * Represents a property expression
 */
public class PropertyExpression {

    private static final Map<String, SubExpression> JMS_PROPERTY_EXPRESSIONS = new HashMap<String, SubExpression>();

    interface SubExpression {
        Object evaluate(JmsMessage message);
    }

    static {
        JMS_PROPERTY_EXPRESSIONS.put("JMSDestination", new SubExpression() {

            @Override
            public Object evaluate(JmsMessage message) {
                try {
                    JmsDestination dest = (JmsDestination) message.getJMSDestination();
                    if (dest == null) {
                        return null;
                    }
                    return dest.toString();
                } catch (JMSException e) {
                    return null;
                }
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSReplyTo", new SubExpression() {

            @Override
            public Object evaluate(JmsMessage message) {
                try {
                    JmsDestination dest = (JmsDestination) message.getJMSReplyTo();
                    if (dest == null) {
                        return null;
                    }
                    return dest.toString();
                } catch (JMSException e) {
                    return null;
                }
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSType", new SubExpression() {

            @Override
            public Object evaluate(JmsMessage message) {
                return message.getJMSType();
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSDeliveryMode", new SubExpression() {

            @Override
            public Object evaluate(JmsMessage message) {
                return Integer.valueOf(message.getFacade().isPersistent() ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSPriority", new SubExpression() {

            @Override
            public Object evaluate(JmsMessage message) {
                return Integer.valueOf(message.getJMSPriority());
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSJmsMessageID", new SubExpression() {

            @Override
            public Object evaluate(JmsMessage message) {
                if (message.getJMSMessageID() == null) {
                    return null;
                }
                return message.getJMSMessageID().toString();
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSTimestamp", new SubExpression() {

            @Override
            public Object evaluate(JmsMessage message) {
                return Long.valueOf(message.getJMSTimestamp());
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSCorrelationID", new SubExpression() {

            @Override
            public Object evaluate(JmsMessage message) {
                return message.getJMSCorrelationID();
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSExpiration", new SubExpression() {

            @Override
            public Object evaluate(JmsMessage message) {
                return Long.valueOf(message.getJMSExpiration());
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSRedelivered", new SubExpression() {

            @Override
            public Object evaluate(JmsMessage message) {
                return Boolean.valueOf(message.isRedelivered());
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSXDeliveryCount", new SubExpression() {

            @Override
            public Object evaluate(JmsMessage message) {
                return Integer.valueOf(message.getFacade().getRedeliveryCounter() + 1);
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSXGroupID", new SubExpression() {

            @Override
            public Object evaluate(JmsMessage message) {
                return message.getFacade().getGroupId();
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSXGroupSeq", new SubExpression() {

            @Override
            public Object evaluate(JmsMessage message) {
                return new Integer(message.getFacade().getGroupSequence());
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSXUserID", new SubExpression() {

            @Override
            public Object evaluate(JmsMessage message) {
                Object userId = message.getFacade().getUserId();
                if (userId == null) {
                    try {
                        userId = message.getFacade().getProperty("JMSXUserID");
                    } catch (Exception e) {
                    }
                }

                return userId;
            }
        });
    }

    private final String name;
    private final SubExpression jmsPropertyExpression;

    public PropertyExpression(String name) {
        this.name = name;
        jmsPropertyExpression = JMS_PROPERTY_EXPRESSIONS.get(name);
    }

    public Object evaluate(JmsMessage message) throws JMSException {
        if (jmsPropertyExpression != null) {
            return jmsPropertyExpression.evaluate(message);
        }
        try {
            return message.getFacade().getProperty(name);
        } catch (Exception ioe) {
            throw JmsExceptionSupport.create(ioe);
        }
    }

    public String getName() {
        return name;
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return name;
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return name.hashCode();
    }

    /**
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object o) {

        if (o == null || !this.getClass().equals(o.getClass())) {
            return false;
        }
        return name.equals(((PropertyExpression) o).name);
    }
}
