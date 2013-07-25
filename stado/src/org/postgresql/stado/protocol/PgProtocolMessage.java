/*****************************************************************************
 * Copyright (C) 2008 EnterpriseDB Corporation.
 * Copyright (C) 2011 Stado Global Development Group.
 *
 * This file is part of Stado.
 *
 * Stado is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Stado is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Stado.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can find Stado at http://www.stado.us
 *
 ****************************************************************************/
/**
 *
 */
package org.postgresql.stado.protocol;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.postgresql.stado.common.util.Property;


/**
 *  encapsulates message of Postgres Protocol v. 3.0
 */
public class PgProtocolMessage {
    /**
     * Postgress uses null-terminated C-style string, so size of a string is
     * initially unknown. This value is initial buffer size to read it in
     */
    private static final int INITIAL_CAPACITY = 256;

    private static final String CHARSET_NAME = Property.get("xdb.charset",
            "ISO-8859-1");

    // Message types to connect/disconnect
    public static final byte MESSAGE_TYPE_INITIAL = 0;

    public static final byte MESSAGE_TYPE_AUTHENTICATION = 'R';

    public static final byte MESSAGE_TYPE_ERROR_RESPONSE = 'E';

    public static final byte MESSAGE_TYPE_BACKEND_KEY_DATA = 'K';

    public static final byte MESSAGE_TYPE_PARAMETER_STATUS = 'S';

    public static final byte MESSAGE_TYPE_READY_FOR_QUERY = 'Z';

    public static final byte MESSAGE_TYPE_PASSWORD_MESSAGE = 'p';

    public static final byte MESSAGE_TYPE_TERMINATE = 'X';

    public static final byte MESSAGE_TYPE_SSL_YES = 'S';

    public static final byte MESSAGE_TYPE_SSL_NO = 'N';

    // Message types to execute a query
    public static final byte MESSAGE_TYPE_QUERY = 'Q';

    public static final byte MESSAGE_TYPE_PARSE = 'P';

    public static final byte MESSAGE_TYPE_PARSE_COMPLETE = '1';

    public static final byte MESSAGE_TYPE_EMPTY_QUERY_RESPONSE = 'I';

    public static final byte MESSAGE_TYPE_BIND = 'B';

    public static final byte MESSAGE_TYPE_BIND_COMPLETE = '2';

    public static final byte MESSAGE_TYPE_EXECUTE = 'E';

    public static final byte MESSAGE_TYPE_COMMAND_COMPLETE = 'C';

    public static final byte MESSAGE_TYPE_DESCRIBE = 'D';

    public static final byte MESSAGE_TYPE_PARAMETER_DESCRIPTION = 't';

    public static final byte MESSAGE_TYPE_NO_DATA = 'n';

    public static final byte MESSAGE_TYPE_ROW_DESCRIPTION = 'T';

    public static final byte MESSAGE_TYPE_SYNC = 'S';

    public static final byte MESSAGE_TYPE_DATA_ROW = 'D';

    public static final byte MESSAGE_TYPE_PORTAL_SUSPENDED = 's';

    public static final byte MESSAGE_TYPE_CLOSE = 'C';

    public static final byte MESSAGE_TYPE_CLOSE_COMPLETE = '3';

    // Function calls
    public static final byte MESSAGE_TYPE_FUNCTION_CALL = 'F';

    public static final byte MESSAGE_TYPE_FUNCTION_CALL_RESPONSE = 'V';

    // COPY
    public static final byte MESSAGE_TYPE_COPY_IN_RESPONSE = 'G';

    public static final byte MESSAGE_TYPE_COPY_OUT_RESPONSE = 'H';

    public static final byte MESSAGE_TYPE_COPY_DATA = 'd';

    public static final byte MESSAGE_TYPE_COPY_DONE = 'c';

    public static final byte MESSAGE_TYPE_COPY_FAIL = 'f';

    // Some immutable message classes
    public static final PgProtocolMessage MSG_AUTHENTICATION_OK = new PgProtocolMessage(
            MESSAGE_TYPE_AUTHENTICATION, new byte[] { 0, 0, 0, 0 });

    public static final PgProtocolMessage MSG_AUTHENTICATION_KERBEROS_V4 = new PgProtocolMessage(
            MESSAGE_TYPE_AUTHENTICATION, new byte[] { 0, 0, 0, 1 });

    public static final PgProtocolMessage MSG_AUTHENTICATION_KERBEROS_V5 = new PgProtocolMessage(
            MESSAGE_TYPE_AUTHENTICATION, new byte[] { 0, 0, 0, 2 });

    public static final PgProtocolMessage MSG_AUTHENTICATION_CLEARTEXT_PASSWORD = new PgProtocolMessage(
            MESSAGE_TYPE_AUTHENTICATION, new byte[] { 0, 0, 0, 3 });

    public static final PgProtocolMessage MSG_SSL_YES = new PgProtocolMessage(
            MESSAGE_TYPE_SSL_YES, null);

    public static final PgProtocolMessage MSG_SSL_NO = new PgProtocolMessage(
            MESSAGE_TYPE_SSL_NO, null);

    public static final PgProtocolMessage MSG_PARSE_COMPLETE = new PgProtocolMessage(
            MESSAGE_TYPE_PARSE_COMPLETE, new byte[0]);

    public static final PgProtocolMessage MSG_EMPTY_QUERY_RESPONSE = new PgProtocolMessage(
            MESSAGE_TYPE_EMPTY_QUERY_RESPONSE, new byte[0]);

    public static final PgProtocolMessage MSG_BIND_COMPLETE = new PgProtocolMessage(
            MESSAGE_TYPE_BIND_COMPLETE, new byte[0]);

    public static final PgProtocolMessage MSG_NO_DATA = new PgProtocolMessage(
            MESSAGE_TYPE_NO_DATA, new byte[0]);

    public static final PgProtocolMessage MSG_PORTAL_SUSPENDED = new PgProtocolMessage(
            MESSAGE_TYPE_PORTAL_SUSPENDED, new byte[0]);

    public static final PgProtocolMessage MSG_CLOSE_COMPLETE = new PgProtocolMessage(
            MESSAGE_TYPE_CLOSE_COMPLETE, new byte[0]);

    public static final PgProtocolMessage MSG_COPY_DONE = new PgProtocolMessage(
            MESSAGE_TYPE_COPY_DONE, new byte[0]);

    private byte messageType;

    private int messageLength;

    private byte[] messageBody;

    private int currentPos = 0;

    public static final byte[] encodeString(String value) {
        try {
            return value == null ? null : value.getBytes(CHARSET_NAME);
        } catch (UnsupportedEncodingException e) {
            // TODO: handle exception
            return null;
        }
    }

    public static final String decodeString(byte[] value, int offset, int length) {
        try {
            return value == null ? null : new String(value, offset, length,
                    CHARSET_NAME);
        } catch (UnsupportedEncodingException e) {
            // TODO: handle exception
            return null;
        }
    }

    public static final String decodeString(byte[] value) {
        try {
            return value == null ? null : new String(value, CHARSET_NAME);
        } catch (UnsupportedEncodingException e) {
            // TODO: handle exception
            return null;
        }
    }

    /**
     * Construct new protocol message
     *
     * @param messageType
     * @param messageBody
     */
    public PgProtocolMessage(byte messageType, byte[] messageBody) {
        this.messageType = messageType;
        this.messageBody = messageBody;
        messageLength = messageBody == null ? messageLength = -1
                : messageBody.length;
    }

    /**
     * Length of message data part
     *
     * @return
     */
    public int getLength() {
        return messageLength;
    }

    /**
     * Size of message data buffer
     *
     * @return
     */
    public int getCapacity() {
        return messageBody == null ? 0 : messageBody.length;
    }

    /**
     * Set size of message data buffer equal or greater then specified
     *
     * @param capacity
     */
    public void ensureCapacity(int capacity) {
        if (messageBody == null) {
            messageBody = new byte[capacity > INITIAL_CAPACITY ? capacity
                    : INITIAL_CAPACITY];
            messageLength = 0;
        } else if (messageBody.length < capacity) {
            byte[] newMsgBody = new byte[capacity > messageBody.length * 2 ? capacity
                    : messageBody.length * 2];
            System.arraycopy(messageBody, 0, newMsgBody, 0, messageLength);
            messageBody = newMsgBody;
        }
    }

    /**
     * Write the message provided ByteBuffer or to the new ByteBuffer if bbuf is
     * null to send it over a channel
     *
     * @param bbuf
     * @return
     */
    public ByteBuffer getAsByteBuffer(ByteBuffer bbuf) {
        if (bbuf == null) {
            if (messageLength < 0) {
                bbuf = ByteBuffer.allocate(1);
            } else {
                bbuf = ByteBuffer.allocate(5 + messageLength);
            }
        }
        bbuf.put(messageType);
        if (messageLength >= 0) {
            bbuf.putInt(messageLength + 4);
            if (messageBody != null && messageBody.length > 0) {
                bbuf.put(messageBody, 0, messageLength);
            }
        }
        bbuf.flip();
        return bbuf;
    }

    /**
     *
     * @return
     */
    public byte getMessageType() {
        return messageType;
    }

    /**
     * Current read position in the data buffer
     *
     * @return
     */
    public int getPosition() {
        return currentPos;
    }

    /**
     * Move current read position in the data buffer
     *
     * @param newPos
     */
    public void setPosition(int newPos) {
        currentPos = newPos > messageLength ? messageLength : newPos;
    }

    /**
     * Get next int8 (byte) value from the data buffer
     *
     * @return
     */
    public int getInt8() {
        return messageBody[currentPos++] & 0xff;
    }

    /**
     * Write value as int8 (byte) into the data buffer
     *
     * @param value
     */
    public void putInt8(int value) {
        ensureCapacity(messageLength + 1);
        messageBody[currentPos++] = (byte) value;
        if (messageLength < currentPos) {
            messageLength = currentPos;
        }
    }

    /**
     * Get next int16 (short) value from the data buffer
     *
     * @return
     */
    public int getInt16() {
        return getInt8() << 8 | getInt8();
    }

    /**
     * Write value as int16 (short) into the data buffer
     *
     * @param value
     */
    public void putInt16(int value) {
        ensureCapacity(messageLength + 2);
        messageBody[currentPos++] = (byte) (value >> 8);
        messageBody[currentPos++] = (byte) value;
        if (messageLength < currentPos) {
            messageLength = currentPos;
        }
    }

    /**
     * Get next int32 (int) value from the data buffer
     *
     * @return
     */
    public int getInt32() {
        return getInt8() << 24 | getInt8() << 16 | getInt8() << 8 | getInt8();
    }

    /**
     * Write value as int32 (int) into the data buffer
     *
     * @param value
     */
    public void putInt32(int value) {
        ensureCapacity(messageLength + 4);
        messageBody[currentPos++] = (byte) (value >> 24);
        messageBody[currentPos++] = (byte) (value >> 16);
        messageBody[currentPos++] = (byte) (value >> 8);
        messageBody[currentPos++] = (byte) value;
        if (messageLength < currentPos) {
            messageLength = currentPos;
        }
    }

    /**
     * Get specified number of int8 (byte) values from the data buffer into an
     * array
     *
     * @return
     */
    public int[] getInt8Array(int length) {
        if (length < 0) {
            return null;
        }
        int[] result = new int[length];
        for (int i = 0; i < length; i++) {
            result[i] = getInt8();
        }
        return result;
    }

    /**
     * Get specified number of int16 (short) values from the data buffer into an
     * array
     *
     * @return
     */
    public int[] getInt16Array(int length) {
        if (length < 0) {
            return null;
        }
        int[] result = new int[length];
        for (int i = 0; i < length; i++) {
            result[i] = getInt16();
        }
        return result;
    }

    /**
     * Get specified number of int32 (int) values from the data buffer into an
     * array
     *
     * @return
     */
    public int[] getInt32Array(int length) {
        if (length < 0) {
            return null;
        }
        int[] result = new int[length];
        for (int i = 0; i < length; i++) {
            result[i] = getInt32();
        }
        return result;
    }

    /**
     * Get specified number of bytes from the data buffer into a byte array
     *
     * @return
     */
    public byte[] getBytes(int length) {
        if (length < 0) {
            return null;
        }
        byte[] result = new byte[length];
        if (length > 0) {
            System.arraycopy(messageBody, currentPos, result, 0, length);
            currentPos += length;
        }
        return result;
    }

    /**
     * Write contents of supplied byte array into the data buffer
     *
     * @param value
     */
    public void putBytes(byte[] value) {
        putBytes(value, 0, value.length);
    }

    /**
     * Write contents of supplied byte array into the data buffer
     *
     * @param value
     * @param offset
     * @param length
     */
    public void putBytes(byte[] value, int offset, int length) {
        if (value == null || value.length == 0) {
            return;
        }
        ensureCapacity(messageLength + length);
        System.arraycopy(value, offset, messageBody, currentPos, length);
        currentPos += length;
        if (messageLength < currentPos) {
            messageLength = currentPos;
        }
    }

    /**
     * Get next null-terminated String from the data buffer
     *
     * @return
     */
    public String getString() {
        byte[] bbuf = new byte[INITIAL_CAPACITY];
        byte value = (byte) getInt8();
        int idx = 0;
        for (; value != 0; idx++) {
            if (idx == bbuf.length) {
                byte[] newbuf = new byte[bbuf.length * 2];
                System.arraycopy(bbuf, 0, newbuf, 0, idx);
                bbuf = newbuf;
            }
            bbuf[idx] = value;
            value = (byte) getInt8();
        }
        return decodeString(bbuf, 0, idx);
    }

    /**
     * Write contents of supplied String into the data buffer
     *
     * @param value
     */
    public void putString(String value) {
        if (value != null) {
            putBytes(encodeString(value));
        }
        putInt8(0);
    }

    /**
     * Returns true if data buffer has unread bytes in the data buffer
     *
     * @return
     */
    public boolean hasMoreData() {
        return messageLength > currentPos;
    }
}
