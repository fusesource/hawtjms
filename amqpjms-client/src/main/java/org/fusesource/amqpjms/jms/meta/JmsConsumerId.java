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
package org.fusesource.amqpjms.jms.meta;

public class JmsConsumerId implements Comparable<JmsConsumerId> {

    protected String connectionId;
    protected long sessionId;
    protected long value;

    protected transient int hashCode;
    protected transient String key;
    protected transient JmsSessionId parentId;

    public JmsConsumerId() {
    }

    public JmsConsumerId(String str){
        if (str != null){
            String[] splits = str.split(":");
            if (splits != null && splits.length >= 3){
                this.connectionId = splits[0];
                this.sessionId =  Long.parseLong(splits[1]);
                this.value = Long.parseLong(splits[2]);
            }
        }
    }

    public JmsConsumerId(JmsSessionId sessionId, long consumerId) {
        this.connectionId = sessionId.getConnectionId();
        this.sessionId = sessionId.getValue();
        this.value = consumerId;
    }

    public JmsConsumerId(JmsConsumerId id) {
        this.connectionId = id.getConnectionId();
        this.sessionId = id.getSessionId();
        this.value = id.getValue();
    }

    public JmsSessionId getParentId() {
        if (parentId == null) {
            parentId = new JmsSessionId(this);
        }
        return parentId;
    }

    public String getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(String connectionId) {
        this.connectionId = connectionId;
    }

    public long getSessionId() {
        return sessionId;
    }

    public void setSessionId(long sessionId) {
        this.sessionId = sessionId;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long consumerId) {
        this.value = consumerId;
    }


    @Override
    public int hashCode() {
        if (hashCode == 0) {
            hashCode = connectionId.hashCode() ^ (int)sessionId ^ (int)value;
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || o.getClass() != JmsConsumerId.class) {
            return false;
        }
        JmsConsumerId id = (JmsConsumerId)o;
        return sessionId == id.sessionId && value == id.value && connectionId.equals(id.connectionId);
    }

    @Override
    public String toString() {
        if (key == null) {
            key = connectionId + ":" + sessionId + ":" + value;
        }
        return key;
    }

    @Override
    public int compareTo(JmsConsumerId other) {
        return toString().compareTo(other.toString());
    }
}