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
package org.postgresql.stado.engine.io;

import java.io.UnsupportedEncodingException;

import org.postgresql.stado.common.util.Property;
import org.postgresql.stado.common.util.XLogger;


/**
 * base class for the messages
 * 
 *  
 */
public abstract class XMessage {
    private static final XLogger logger = XLogger.getLogger(XMessage.class);

    /**
     * Character set to encode strings and characters.
     */
    protected static final String CHARSET_NAME = Property.get("xdb.charset",
            "ISO-8859-1");

    /**
     * Use Unicode (two bytes per character) to encode strings. If
     * <code>true</code> CHARSET_NAME is ignored
     */
    protected static final boolean USE_UNICODE = Property.getBoolean(
            "xdb.unicode", false);

    // 2 pow 24 - 1
    public static final int MAX_LENGTH = 16777215;

    public static final int HEADER_SIZE = 8;

    public static final String ARGS_DELIMITER = "|";

    // for use with cmd
    public static char SOH = 0x01;

    public static char STX = 0x02;

    public static char ETX = 0x03;

    public static final String STR_DELIMITER = "|" + SOH + "|" + STX + "|"
            + ETX; // string delimiter

    protected static final int STRING_NULL_SIZE = -2;// indicate the string
                                                        // is a null string

    public static final int INTEGER_SIZE = 4;

    // for the details
    protected byte[] message = null;// quick messages

    protected int currentPos = 0;// place where we should start writing the
                                    // next bytes

    protected int readPos = 0;

    // header info
    // 1. 3 bytes
    protected int packetLength = HEADER_SIZE;// minimum is the header size

    // 2. 1 byte
    protected byte type = MessageTypes.UNKNOWN;

    // 3. 4 bytes
    protected int requestId = 0;// unique id for the request (original request?)

    protected XMessage() {
    }

    public int getPacketLength() {
        return this.packetLength;
    }

    public void increasePacketLength(int len) {
        this.packetLength += len;
    }

    public byte getType() {
        return this.type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public int getRequestId() {
        return this.requestId;
    }

    public void setRequestId(int requestId) {
        this.requestId = requestId;
    }

    /** this will overwrite all previous values */
    public void setHeaderBytes(byte[] header) {
        this.packetLength = (header[0] & 0xff) | ((header[1] & 0xff) << 8)
                | ((header[2] & 0xff) << 16);

        this.requestId = (header[3] & 0xff) | ((header[4] & 0xff) << 8)
                | ((header[5] & 0xff) << 16) | ((header[6] & 0xff) << 24);

        this.type = header[7];
    }

    /** convenient method returning this header in byte[] */
    public byte[] getHeaderBytes() {
        byte[] header = new byte[HEADER_SIZE];
        header[0] = (byte) (packetLength & 0xff);
        header[1] = (byte) (packetLength >> 8);
        header[2] = (byte) (packetLength >> 16);
        header[3] = (byte) (requestId & 0xff);
        header[4] = (byte) (requestId >> 8);
        header[5] = (byte) (requestId >> 16);
        header[6] = (byte) (requestId >> 24);
        header[7] = type;

        return header;
    }

    // now for the details
    public void setMessage(byte[] message) {
        this.message = message;
        readPos = 0;
    }

    public byte[] getMessage() {
        return message;
    }

    public int getCurrentMessageSize() {
        return currentPos;// this is also the size being used
    }

    // make sure message[] has enough room for the new data
    public void initSize(int size) {
        if (message == null) {
            message = new byte[size];
        } else if ((currentPos + size) > message.length) {
            // Grab a extra to save on array copying and extra allocs
            if (size < 8192) {
                size = 8192;
            }

            int newSize = currentPos + size;
            byte[] tmp = new byte[newSize];
            System.arraycopy(message, 0, tmp, 0, message.length);
            message = tmp;
        }
    }

    // storers - convert the value into bytes and store them to message[],
    // allocating
    // new memory as needed. currentPos marks the last position a byte was
    // written to - this
    // is of course the size of the message so far.
    public final void storeByte(byte v) {
        initSize(1);
        message[currentPos++] = v;
        increasePacketLength(1);
        // setPacketLength(getPacketLength() + 1);
    }

    // for binary data.
    // let's mark the size to read.
    public final void storeBytes(byte[] bytes) {
        if (bytes == null) {
            storeInt(STRING_NULL_SIZE);
            return;
        }
        storeBytes(bytes, 0, bytes.length, true);
    }

    // storeLength specifies whether we should store the length first
    // NOTE: must use getBytes(int) to retrieve this!!
    public final void storeBytes(byte[] bytes, int start, int length,
            boolean storeLength) {
        if (storeLength) {
            storeInt(length);
        }
        initSize(length);
        System.arraycopy(bytes, start, message, currentPos, length);
        currentPos += length;
        increasePacketLength(length);
    }

    public final void storeShort(short v) {
        initSize(2);
        message[currentPos++] = (byte) (v & 0xff);
        message[currentPos++] = (byte) (v >> 8);
        // setPacketLength(getPacketLength() + 2);
        increasePacketLength(2);
    }

    public final void storeShort(int v) {
        storeShort((short) v);
    }

    // store 4-byte integer
    public final void storeInt(int v) {
        initSize(XMessage.INTEGER_SIZE);
        message[currentPos++] = (byte) (v & 0xff);
        message[currentPos++] = (byte) (v >> 8);
        message[currentPos++] = (byte) (v >> 16);
        message[currentPos++] = (byte) (v >> 24);
        // setPacketLength(getPacketLength() + XMessage.INTEGER_SIZE);
        increasePacketLength(XMessage.INTEGER_SIZE);
    }

    public final void storeLong(long v) {
        initSize(8);
        message[currentPos++] = (byte) (v & 0xff);
        message[currentPos++] = (byte) (v >> 8);
        message[currentPos++] = (byte) (v >> 16);
        message[currentPos++] = (byte) (v >> 24);
        message[currentPos++] = (byte) (v >> 32);
        message[currentPos++] = (byte) (v >> 40);
        message[currentPos++] = (byte) (v >> 48);
        message[currentPos++] = (byte) (v >> 56);
        // setPacketLength(getPacketLength() + 8);
        increasePacketLength(8);
    }

    public final void storeFloat(float v) {
        storeInt(Float.floatToIntBits(v));
    }

    public final void storeDouble(double v) {
        storeLong(Double.doubleToLongBits(v));
    }

    // Store the size first so we know exactly how many to read later:
    // Note: 0 size or "STRING_NULL_SIZE" are special cases.
    public final void storeString(String s) {
        // Special cases: NULL...
        if (s == null) {
            storeInt(STRING_NULL_SIZE);
            return;
        }
        // ... and empty string
        int len = s.length();
        if (len == 0) {
            storeInt(len);
            return;
        }

        if (USE_UNICODE) {
            storeInt(len * 2);
            initSize(len * 2);
            for (int i = 0; i < len; i++) {
                char ch = s.charAt(i);
                message[currentPos++] = (byte) (ch & 0xff);
                message[currentPos++] = (byte) (ch >> 8);
            }
            increasePacketLength(len * 2);
        } else {
            try {
                byte[] bytes = s.getBytes(CHARSET_NAME);
                storeBytes(bytes);
            } catch (UnsupportedEncodingException uee) {
                logger.catching(uee);
                storeInt(STRING_NULL_SIZE);
            }
        }
    }

    // readers - read the next bytes and convert to the requested type

    // any more data on message[]
    public boolean hasMoreDataToRead() {
        return readPos < packetLength - XMessage.HEADER_SIZE;
    }

    // readPos marks where last byte read

    public final byte readByte() {
        return message[readPos++];
    }

    // automatically read the next number of bytes
    public final byte[] readBytes() {
        int size = readInt();
        return readBytes(size);
    }

    // i.e. for use with storeBytes(..) where storeLength was false
    public final byte[] readBytes(int size) {
        if (size < 0) {
            return null;
        }
        byte[] b = new byte[size];
        if (size > 0) {
            System.arraycopy(message, readPos, b, 0, size);
            readPos += size;
        }
        return b;
    }

    public final short readShort() {
        return (short) ((message[readPos++] & 0xff) | ((message[readPos++] & 0xff) << 8));
    }

    public final int readInt() {
        return (message[readPos++] & 0xff) | ((message[readPos++] & 0xff) << 8)
                | ((message[readPos++] & 0xff) << 16)
                | ((message[readPos++] & 0xff) << 24);
    }

    public final long readLong() {
        return (message[readPos++] & 0xff) | ((message[readPos++] & 0xff) << 8)
                | ((message[readPos++] & 0xff) << 16)
                | ((message[readPos++] & 0xff) << 24)
                | ((message[readPos++] & 0xff) << 32)
                | ((message[readPos++] & 0xff) << 40)
                | ((message[readPos++] & 0xff) << 48)
                | ((message[readPos++] & 0xff) << 56);
    }

    public final float readFloat() {
        return Float.intBitsToFloat(readInt());
    }

    public final double readDouble() {
        return Double.longBitsToDouble(readLong());
    }

    // Read the next string, by first checking its size.
    // The length (integer) is stored just before the actual string.
    public final String readString() {
        int size = readInt();

        // don't need to read cases
        if (size == STRING_NULL_SIZE) {
            return null;
        } else if (size == 0) {
            return "";
        }
        String out = null;
        byte[] bytes = readBytes(size);
        if (USE_UNICODE) {
            char[] val = new char[size / 2];
            int pos = 0;
            for (int i = 0; i < val.length; i++) {
                val[i] = (char) ((bytes[pos++] & 0xff) | ((bytes[pos++] & 0xff) << 8));
            }
            out = new String(val);
        } else {

            try {
                out = new String(bytes, CHARSET_NAME);
            } catch (UnsupportedEncodingException uee) {
                logger.catching(uee);
            }
        }
        return out;
    }

}
