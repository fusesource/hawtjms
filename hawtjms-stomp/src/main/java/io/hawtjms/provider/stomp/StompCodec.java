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

import static io.hawtjms.provider.stomp.StompConstants.COLON_BYTE;
import static io.hawtjms.provider.stomp.StompConstants.COLON_ESCAPE_BYTE;
import static io.hawtjms.provider.stomp.StompConstants.COLON_ESCAPE_SEQ;
import static io.hawtjms.provider.stomp.StompConstants.ESCAPE_BYTE;
import static io.hawtjms.provider.stomp.StompConstants.ESCAPE_ESCAPE_BYTE;
import static io.hawtjms.provider.stomp.StompConstants.ESCAPE_ESCAPE_SEQ;
import static io.hawtjms.provider.stomp.StompConstants.NEWLINE_BYTE;
import static io.hawtjms.provider.stomp.StompConstants.NEWLINE_ESCAPE_BYTE;
import static io.hawtjms.provider.stomp.StompConstants.NEWLINE_ESCAPE_SEQ;
import static io.hawtjms.provider.stomp.StompConstants.NULL_BYTE;
import static io.hawtjms.provider.stomp.StompConstants.UTF8;
import static io.hawtjms.provider.stomp.StompConstants.V1_0;

import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.fusesource.hawtbuf.AsciiBuffer;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.BufferOutputStream;
import org.fusesource.hawtbuf.ByteArrayOutputStream;
import org.fusesource.hawtbuf.DataByteArrayOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Codec class used to handle incoming byte packets from the broker and
 * build a STOMP command from it.
 */
public class StompCodec {

    private static final Logger LOG = LoggerFactory.getLogger(StompCodec.class);

    /**
     * Pair like object used to hold parsed Header key / value entries.
     *
     * TODO - value could be an UTF8Buffer to reduce decode time if performance
     *        tuning becomes necessary.
     */
    static private class HeaderEntry {
        public final String key;
        public final String value;

        public HeaderEntry(String key, String value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString() {
            return "" + key + "=" + value;
        }
    }

    private final int maxCommandLength = 20;
    private int maxHeaderLength = 10 * 1024;
    private int maxHeaders = 10000;
    private int maxContentSize = 100 * 1024 * 1024;

    private FrameParser currentParser;
    private long lastReadTime = System.currentTimeMillis();
    private String version = V1_0;

    /*
     * Scratch buffer used for header and content decoding.  If the sent
     * value can fit into scratch we don't need to allocate anything.
     */
    private final ByteBuffer scratch = ByteBuffer.allocate(maxHeaderLength);

    // Internal parsers implement this and we switch to the next as we go.
    private interface FrameParser {
        StompFrame parse(ByteBuffer data) throws IOException;
    }

    /**
     * Reads the incoming Buffer and builds a new StompFrame from the data.
     *
     * Since a frame can come in in partial packets or one frame can start after
     * the end of a frame in the same packet.  We read only one StompFrame leaving
     * any extra data in the buffer, the caller should perform additional processInput
     * calls to consume all data.
     *
     * The parser will maintain state in-between calls in the case of a partial frame
     * being read from the last incoming buffer.  Once the full frame is read it will
     * be returned.
     *
     * @param buffer
     *        the next incoming data buffer.
     *
     * @return a newly parsed StompFrame instance or null if frame is stil incomplete.
     *
     * @throws IOException if an error occurs while parsing the incoming data.
     */
    public StompFrame decode(ByteBuffer data) throws IOException {
        lastReadTime = System.currentTimeMillis();

        if (currentParser == null) {
            currentParser = commandParser;
        }

        return currentParser.parse(data);
    }

    /**
     * Encodes the given StompFrame into a ByteBuffer using an encoding that matches
     * the current protocol version that is in use.
     *
     * @param frame
     *
     * @return a ByteBuffer ready for transmission.
     *
     * @throws IOException if an error occurs while encoding the StompFrame.
     */
    public ByteBuffer encode(StompFrame frame) throws IOException {
        DataByteArrayOutputStream dataOut = new DataByteArrayOutputStream();

        try {
            write(dataOut, frame);
        } finally {
            dataOut.close();
        }

        return dataOut.toBuffer().toByteBuffer();
    }

    //--------- STOMP Frame decode methods -----------------------------------//

    private final FrameParser commandParser = new FrameParser() {

        @Override
        public StompFrame parse(ByteBuffer data) throws IOException {

            while (data.hasRemaining()) {
                byte nextByte = data.get();

                // As of STOMP v1.2 lines can end with CRLF or just LF.
                // Any extra LF before start of the frame command are keep alive values.
                if (nextByte == '\r') {
                    continue;
                }

                if (nextByte != '\n') {
                    try {
                        scratch.put(nextByte);
                        continue;
                    } catch (BufferOverflowException e) {
                        throw new IOException("The maximum command length was exceeded");
                    }
                }

                // Once we hit the true end of line and have read Command data we can
                // move onto the next stage, header parsing.
                if (scratch.position() != 0) {
                    scratch.flip();
                    AsciiBuffer command = new Buffer(scratch).trim().ascii();
                    LOG.trace("New incoming STOMP frame, command := {}", command);
                    StompFrame frame = new StompFrame(command.toString());
                    currentParser = initiateHeaderRead(frame);
                    return currentParser.parse(data);
                }
            }

            return null;
        }
    };

    private FrameParser initiateHeaderRead(final StompFrame frame) {
        scratch.clear();
        final String[] contentLengthValue = new String[1];
        final ArrayList<HeaderEntry> headers = new ArrayList<HeaderEntry>(10);

        return new FrameParser() {
            ByteBuffer headerLine = scratch;

            @Override
            public StompFrame parse(ByteBuffer data) throws IOException {

                while (data.hasRemaining()) {
                    byte nextByte = data.get();

                    // As of STOMP v1.2 lines can end with CRLF or just LF.
                    // Any extra LF before start of the frame command are keep alive values.
                    if (nextByte == '\r') {
                        continue;
                    }

                    if (nextByte != '\n') {
                        try {
                            headerLine.put(nextByte);
                            continue;
                        } catch (BufferOverflowException e) {
                            headerLine = tryIncrease(headerLine, headerLine.limit() * 2, getMaxHeaderLength(), "Max size of header exceeded");
                        }
                    }

                    // Either we've hit the end of the line and have a header to parse
                    // or we've read our second newline and the body starts next.
                    if (headerLine.position() != 0) {
                        headerLine.flip();
                        HeaderEntry entry = parseHeaderLine(headerLine);
                        if (entry.key.equals(StompConstants.CONTENT_LENGTH)) {
                            contentLengthValue[0] = entry.value;
                        }
                        headerLine.clear();
                        headers.add(entry);
                        if (headers.size() > getMaxHeaders()) {
                            throw new IOException("Maximum number of headers exceeded.");
                        }
                    } else {
                        applyHeaders(frame, headers);
                        String contentLength = contentLengthValue[0];
                        if (contentLength != null) {
                            int length = 0;
                            try {
                                length = Integer.parseInt(contentLength);
                            } catch (NumberFormatException e) {
                                throw new IOException("Specified content-length is not a valid integer");
                            }

                            if (getMaxContentSize() != -1 && length > getMaxContentSize()) {
                                throw new IOException("Message payload exceeds maximum size setting.");
                            }

                            currentParser = intitBytesMessageRead(frame,length);
                            return currentParser.parse(data);
                        } else {
                            currentParser = initTextMessageRead(frame);
                            return currentParser.parse(data);
                        }
                    }
                }

                return null;
            }
        };
    }

    private FrameParser intitBytesMessageRead(final StompFrame frame, final int length) {
        scratch.clear();
        return new FrameParser() {

            ByteBuffer content = scratch;

            @Override
            public StompFrame parse(ByteBuffer data) throws IOException {
                while (data.hasRemaining() && content.position() < length) {
                    byte nextByte = data.get();
                    try {
                        content.put(nextByte);
                    } catch (BufferOverflowException e) {
                        content = tryIncrease(content, content.limit() * 2, getMaxCommandLength(), "Max content size exceeded");
                    }
                }

                if (content.position() == length) {
                    byte terminus = data.get();
                    if (terminus != NULL_BYTE) {
                        throw new IOException("Expected zero byte after binary content.");
                    }

                    applyContent(frame, content);
                    currentParser = commandParser;
                    return frame;
                }

                return null;
            }
        };
    }

    private FrameParser initTextMessageRead(final StompFrame frame) {
        scratch.clear();
        return new FrameParser() {

            ByteBuffer content = scratch;

            @Override
            public StompFrame parse(ByteBuffer data) throws IOException {
                while (data.hasRemaining()) {
                    byte nextByte = data.get();
                    if (nextByte != NULL_BYTE) {
                        try {
                            content.put(nextByte);
                            if (content.position() > getMaxContentSize()) {
                                throw new IOException("Content size exceeds maximum allowed size.");
                            }
                        } catch (BufferOverflowException e) {
                            content = tryIncrease(content, content.limit() * 2, getMaxCommandLength(), "Max content size exceeded");
                        }
                    } else {
                        applyContent(frame, content);
                        scratch.clear();
                        currentParser = commandParser;
                        return frame;
                    }
                }

                return null;
            }
        };
    }

    private ByteBuffer tryIncrease(ByteBuffer source, int newSize, int maxSize, String errorMessage) throws IOException {
        if (source.limit() == maxSize) {
            throw new IOException(errorMessage);
        }

        int scaled = Math.min(newSize, maxSize);

        ByteBuffer newBuffer = ByteBuffer.allocate(scaled);
        source.flip();
        newBuffer.put(source);

        return newBuffer;
    }

    private void applyContent(StompFrame frame, ByteBuffer content) throws IOException {
        content.flip();
        if (content == scratch) {
            Buffer copy = new Buffer(content.limit());
            BufferOutputStream loader = copy.out();
            while (content.hasRemaining()) {
                loader.write(content.get());
            }
            frame.setContent(copy);
        } else {
            frame.setContent(new Buffer(content));
        }
    }

    private HeaderEntry parseHeaderLine(ByteBuffer headerLine) throws IOException {

        ByteBuffer name = ByteBuffer.allocate(headerLine.limit());
        while (headerLine.hasRemaining()) {
            byte nextByte = headerLine.get();
            if (nextByte == ':') {
                break;
            }

            name.put(nextByte);
        }

        String key = new String(name.array(), 0, name.position(), UTF8);
        String value = decodeHeader(headerLine);

        return new HeaderEntry(key, value);
    }

    private String decodeHeader(ByteBuffer header) throws IOException {

        ByteBuffer decoded = ByteBuffer.allocate(header.limit());

        while (header.hasRemaining()) {
            byte nextByte = header.get();

            if (nextByte == ESCAPE_BYTE) {
                if (!header.hasRemaining()) {
                    decoded.put(nextByte);
                } else {
                    byte escaped = header.get();
                    switch (escaped) {
                        case COLON_ESCAPE_BYTE:
                            decoded.put(COLON_BYTE);
                            break;
                        case ESCAPE_ESCAPE_BYTE:
                            decoded.put(ESCAPE_BYTE);
                            break;
                        case NEWLINE_ESCAPE_BYTE:
                            decoded.put(NEWLINE_BYTE);
                            break;
                    }
                }
            } else {
                decoded.put(nextByte);
            }
        }

        return new String(decoded.array(), 0, decoded.position(), UTF8);
    }

    private void applyHeaders(StompFrame frame, List<HeaderEntry> headers) {
        // STOMP frames can have repeating properties applied on the Broker.
        // We must use only the first one and can ignore the rest.
        Map<String, String> properties = frame.getProperties();
        for (HeaderEntry entry : headers) {
            String old = properties.put(entry.key, entry.value);
            if (old != null) {
                properties.put(entry.key, old);
            }
        }
    }

    //--------- STOMP Frame encode methods -----------------------------------//

    public void write(DataOutput out, StompFrame frame) throws IOException {
        write(out, new AsciiBuffer(frame.getCommand()));
        out.writeByte(NEWLINE_BYTE);

        for (Map.Entry<String, String> entry : frame.getProperties().entrySet()) {
            write(out, encodeHeader(entry.getKey()));
            out.writeByte(COLON_BYTE);
            write(out, encodeHeader(entry.getValue()));
            out.writeByte(NEWLINE_BYTE);
        }

        // denotes end of headers with a new line
        out.writeByte(NEWLINE_BYTE);
        write(out, frame.getContent());
        out.writeByte(NULL_BYTE);
        out.writeByte(NEWLINE_BYTE);
    }

    private void write(DataOutput out, Buffer buffer) throws IOException {
        out.write(buffer.data, buffer.offset, buffer.length);
    }

    public static Buffer encodeHeader(String value) throws IOException {
        if (value == null) {
            return null;
        }

        ByteArrayOutputStream out = null;
        try {
            byte[] data = value.getBytes(UTF8);
            out = new ByteArrayOutputStream(data.length);
            for (byte d : data) {
                switch (d) {
                    case ESCAPE_BYTE:
                        out.write(ESCAPE_ESCAPE_SEQ.getBytes(UTF8));
                        break;
                    case COLON_BYTE:
                        out.write(COLON_ESCAPE_SEQ.getBytes(UTF8));
                        break;
                    case NEWLINE_BYTE:
                        out.write(NEWLINE_ESCAPE_SEQ.getBytes(UTF8));
                        break;
                    default:
                        out.write(d);
                }
            }
            return out.toBuffer().utf8();
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e); // not expected.
        } finally {
            if (out != null) {
                out.close();
            }
        }
    }

    //---------- Property Getters and Setters --------------------------------//

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public long getLastReadTime() {
        return lastReadTime;
    }

    public int getMaxCommandLength() {
        return maxCommandLength;
    }

    public int getMaxHeaderLength() {
        return maxHeaderLength;
    }

    public void setMaxHeaderLength(int maxHeaderLength) {
        this.maxHeaderLength = maxHeaderLength;
    }

    public int getMaxHeaders() {
        return maxHeaders;
    }

    public void setMaxHeaders(int maxHeaders) {
        this.maxHeaders = maxHeaders;
    }

    public int getMaxContentSize() {
        return maxContentSize;
    }

    public void setMaxContentSize(int maxContentSize) {
        this.maxContentSize = maxContentSize;
    }
}
