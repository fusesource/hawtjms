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

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import org.fusesource.hawtbuf.Buffer;

/**
 * Represents all the data in a STOMP frame.
 */
public class StompFrame {

    public static final ByteBuffer NO_DATA = ByteBuffer.allocate(0);

    private String command;
    private HashMap<String, String> propertiesMap = new HashMap<String, String>(16);
    private ByteBuffer content = NO_DATA;

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

    public ByteBuffer getContent() {
        return this.content;
    }

    public StompFrame setContent(ByteBuffer content) {
        assert content != null;
        this.content = content;
        return this;
    }

    public String getContentAsString() {
        return new String(content.array(), Charset.forName("UTF-8"));
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

    public void clearProperties() {
        propertiesMap.clear();
    }

    public ByteBuffer toBuffer() {
        return toBuffer(true);
    }

    public ByteBuffer toBuffer(boolean includeBody) {
//        try {
//            DataByteArrayOutputStream out = new DataByteArrayOutputStream();
//            write(out, includeBody);
//            return out.toBuffer();
//        } catch (IOException e) {
//            throw new RuntimeException(e); // not expected to occur.
//        }
        return null;
    }

    private void write(DataOutput out, Buffer buffer) throws IOException {
        out.write(buffer.data, buffer.offset, buffer.length);
    }

    public void write(DataOutput out) throws IOException {
//        write(out, true);
    }

    public void setContentLength() {
        setProperty(CONTENT_LENGTH, Integer.toString(content.remaining()));
    }

//    public void write(DataOutput out, boolean includeBody) throws IOException {
//        write(out, action);
//        out.writeByte(NEWLINE_BYTE);
//
//        if (headerList != null) {
//            for (HeaderEntry entry : headerList) {
//                write(out, entry.getKey());
//                out.writeByte(COLON_BYTE);
//                write(out, entry.getValue());
//                out.writeByte(NEWLINE_BYTE);
//            }
//        } else {
//            for (Map.Entry<String, String> entry : propertiesMap.entrySet()) {
//                write(out, entry.getKey());
//                out.writeByte(COLON_BYTE);
//                write(out, entry.getValue());
//                out.writeByte(NEWLINE_BYTE);
//            }
//        }
//
//        // denotes end of headers with a new line
//        out.writeByte(NEWLINE_BYTE);
//        if (includeBody) {
//            write(out, content);
//            out.writeByte(NULL_BYTE);
//            out.writeByte(NEWLINE_BYTE);
//        }
//    }

    @Override
    public String toString() {
        return new String(toBuffer(false).array(), Charset.forName("UTF-8"));
    }

//    public String errorMessage() {
//        String value = getHeader(MESSAGE_HEADER);
//        if (value != null) {
//            return decodeHeader(value);
//        } else {
//            return getContentAsString();
//        }
//    }
//
//    public static String decodeHeader(Buffer value) {
//        if (value == null) {
//            return null;
//        }
//
//        ByteArrayOutputStream rc = new ByteArrayOutputStream(value.length);
//        Buffer pos = new Buffer(value);
//        int max = value.offset + value.length;
//        while (pos.offset < max) {
//            if (pos.startsWith(ESCAPE_ESCAPE_SEQ)) {
//                rc.write(ESCAPE_BYTE);
//                pos.moveHead(2);
//            } else if (pos.startsWith(COLON_ESCAPE_SEQ)) {
//                rc.write(COLON_BYTE);
//                pos.moveHead(2);
//            } else if (pos.startsWith(NEWLINE_ESCAPE_SEQ)) {
//                rc.write(NEWLINE_BYTE);
//                pos.moveHead(2);
//            } else {
//                rc.write(pos.data[pos.offset]);
//                pos.moveHead(1);
//            }
//        }
//
//        try {
//            return new String(rc.toByteArray(), "UTF-8");
//        } catch (UnsupportedEncodingException e) {
//            throw new RuntimeException(e); // not expected.
//        }
//    }
//
//    public static String encodeHeader(String value) {
//        if (value == null) {
//            return null;
//        }
//
//        try {
//            byte[] data = value.getBytes("UTF-8");
//            ByteArrayOutputStream rc = new ByteArrayOutputStream(data.length);
//            for (byte d : data) {
//                switch (d) {
//                    case ESCAPE_BYTE:
//                        rc.write(ESCAPE_ESCAPE_SEQ);
//                        break;
//                    case COLON_BYTE:
//                        rc.write(COLON_ESCAPE_SEQ);
//                        break;
//                    case NEWLINE_BYTE:
//                        rc.write(COLON_ESCAPE_SEQ);
//                        break;
//                    default:
//                        rc.write(d);
//                }
//            }
//            return rc.toBuffer().ascii().toString();
//        } catch (UnsupportedEncodingException e) {
//            throw new RuntimeException(e); // not expected.
//        }
//    }
//
//    public static Map<String, String> encodeHeaders(Map<String, String> headers) {
//        if (headers == null) {
//            return null;
//        }
//        HashMap<String, String> rc = new HashMap<String, String>(headers.size());
//        for (Map.Entry<String, String> entry : headers.entrySet()) {
//            rc.put(StompFrame.encodeHeader(entry.getKey()), StompFrame.encodeHeader(entry.getValue()));
//        }
//        return rc;
//    }
}
