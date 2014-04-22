/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"; you may not use this file except in compliance with
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

import java.nio.charset.Charset;

/**
 * A series of constant values used by the STOMP protocol.
 */
public interface StompConstants {

    final Charset UTF8 = Charset.forName("UTF-8");

    final String NULL = "\u0000";
    final byte NULL_BYTE = 0;

    final String NEWLINE = "\n";
    final byte NEWLINE_BYTE = '\n';

    final String COLON = ":";
    final byte COLON_BYTE = ':';

    final byte ESCAPE_BYTE = '\\';

    final byte ESCAPE_ESCAPE_BYTE = 92;
    final byte COLON_ESCAPE_BYTE = 99;
    final byte NEWLINE_ESCAPE_BYTE = 110;

    final String ESCAPE_ESCAPE_SEQ = "\\\\";
    final String COLON_ESCAPE_SEQ = "\\c";
    final String NEWLINE_ESCAPE_SEQ = "\\n";

    // Commands
    final String STOMP = "STOMP";
    final String CONNECT = "CONNECT";
    final String SEND = "SEND";
    final String DISCONNECT = "DISCONNECT";
    final String SUBSCRIBE = "SUBSCRIBE";
    final String UNSUBSCRIBE = "UNSUBSCRIBE";
    final String MESSAGE = "MESSAGE";

    final String BEGIN_TRANSACTION = "BEGIN";
    final String COMMIT_TRANSACTION = "COMMIT";
    final String ABORT_TRANSACTION = "ABORT";
    final String BEGIN = "BEGIN";
    final String COMMIT = "COMMIT";
    final String ABORT = "ABORT";
    final String ACK = "ACK";

    // Responses
    final String CONNECTED = "CONNECTED";
    final String ERROR = "ERROR";
    final String RECEIPT = "RECEIPT";

    // Headers
    final String RECEIPT_REQUESTED = "receipt";
    final String TRANSACTION = "transaction";
    final String CONTENT_LENGTH = "content-length";
    final String CONTENT_TYPE = "content-type";
    final String TRANSFORMATION = "transformation";
    final String TRANSFORMATION_ERROR = "transformation-error";

    /**
     * This header is used to instruct ActiveMQ to construct the message
     * based with a specific type.
     */
    final String AMQ_MESSAGE_TYPE = "amq-msg-type";
    final String RECEIPT_ID = "receipt-id";
    final String PERSISTENT = "persistent";
    final String MESSAGE_HEADER = "message";
    final String MESSAGE_ID = "message-id";
    final String CORRELATION_ID = "correlation-id";
    final String EXPIRATION_TIME = "expires";
    final String REPLY_TO = "reply-to";
    final String PRIORITY = "priority";
    final String REDELIVERED = "redelivered";
    final String TIMESTAMP = "timestamp";
    final String TYPE = "type";
    final String SUBSCRIPTION = "subscription";
    final String USERID = "JMSXUserID";
    final String PROPERTIES = "JMSXProperties";
    final String ACK_MODE = "ack";
    final String ACK_ID = "ack";
    final String ID = "id";
    final String SELECTOR = "selector";
    final String BROWSER = "browser";
    final String AUTO = "auto";
    final String CLIENT = "client";
    final String INDIVIDUAL = "client-individual";
    final String DESTINATION = "destination";
    final String LOGIN = "login";
    final String PASSCODE = "passcode";
    final String CLIENT_ID = "client-id";
    final String REQUEST_ID = "request-id";
    final String SESSION = "session";
    final String RESPONSE_ID = "response-id";
    final String ACCEPT_VERSION = "accept-version";
    final String V1_2 = "1.2";
    final String V1_1 = "1.1";
    final String V1_0 = "1.0";
    final String HOST = "host";
    final String TRUE = "true";
    final String FALSE = "false";
    final String END = "end";
    final String HOST_ID = "host-id";
    final String SERVER = "server";
    final String CREDIT = "credit";
    final String JMSX_DELIVERY_COUNT = "JMSXDeliveryCount";
    final String JMSX_GROUP_ID = "JMSXGroupID";
    final String JMSX_GROUP_SEQUENCE = "JMSXGroupSequence";
    final String HEARTBEAT = "heart-beat";
    final String VERSION = "version";

    /**
     * Well known JMSException types that we want to map when throwing from
     * the provider on ERROR frames.
     */
    final String INVALID_SELECTOR_EXCEPTION = "InvalidSelectorException";
    final String INVALID_CLIENTID_EXCEPTION = "InvalidClientIDException";
    final String JMS_SECURITY_EXCEPTION = "JmsSecurityException";
    final String SECURITY_EXCEPTION = "SecurityException";
}
