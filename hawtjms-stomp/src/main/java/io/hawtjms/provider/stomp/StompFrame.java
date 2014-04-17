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
package io.hawtjms.provider.stomp;

import static io.hawtjms.provider.stomp.StompConstants.CONTENT_LENGTH;
import static io.hawtjms.provider.stomp.StompConstants.MESSAGE_HEADER;
import static io.hawtjms.provider.stomp.StompConstants.UTF8;

import java.util.HashMap;
import java.util.Map;

import org.fusesource.hawtbuf.Buffer;

/**
 * Represents all the data in a STOMP frame.
 */
public class StompFrame {

    public static final Buffer NO_DATA = new Buffer(new byte[0]);

    private String command;
    private HashMap<String, String> propertiesMap = new HashMap<String, String>(16);
    private Buffer content = NO_DATA;

    public StompFrame() {
    }

    public StompFrame(String action) {
        this.command = action;
    }

    @Override
    public StompFrame clone() {
        StompFrame rc = new StompFrame(command);
        rc.propertiesMap = new HashMap<String, String>(propertiesMap);
        rc.content = content;
        return rc;
    }

    public String getCommand() {
        return command;
    }

    public StompFrame setCommand(String command) {
        assert command != null;
        this.command = command;
        return this;
    }

    public Buffer getContent() {
        return this.content;
    }

    public StompFrame setContent(Buffer content) {
        this.content = content;
        return this;
    }

    public String getContentAsString() {
        return new String(content.data, content.offset, content.length, UTF8);
    }

    public String getErrorMessage() {
        String value = getProperty(MESSAGE_HEADER);
        if (value != null) {
            return value;
        } else {
            return getContentAsString();
        }
    }

    public Map<String, String> getProperties() {
        return propertiesMap;
    }

    public void setProperty(String key, String value) {
        propertiesMap.put(key, value);
    }

    public String getProperty(String key) {
        return propertiesMap.get(key);
    }

    public String removeProperty(String key) {
        return this.propertiesMap.remove(key);
    }

    public void clearProperties() {
        propertiesMap.clear();
    }

    void setContentLength() {
        setProperty(CONTENT_LENGTH, Integer.toString(content.length()));
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(this.command).append(": {");
        builder.append("Properties: [").append(propertiesMap.size()).append("] ");
        builder.append(" Content: [").append(content.length()).append("] ");
        builder.append("}");

        return builder.toString();
    }
}
