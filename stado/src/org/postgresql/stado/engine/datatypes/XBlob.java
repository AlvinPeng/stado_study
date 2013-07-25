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
/*
 * XBlob.java
 *
 *  
 */

package org.postgresql.stado.engine.datatypes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.SQLException;

import org.postgresql.stado.common.util.SQLTypeConverter;


/**
 * 
 *  
 */
public class XBlob implements java.sql.Blob {
    private byte[] val = null;

    /** Creates a new instance of XBlob */
    protected XBlob() {
    }

    public XBlob(byte[] bytes) {
        this.val = bytes;
    }

    /**
     * Retrieves the <code>BLOB</code> value designated by this
     * <code>Blob</code> instance as a stream.
     * 
     * @return a stream containing the <code>BLOB</code> data
     * @exception SQLException
     *                if there is an error accessing the <code>BLOB</code>
     *                value
     * @see #setBinaryStream
     * @since 1.2
     * 
     */
    public java.io.InputStream getBinaryStream() throws SQLException {
        if (val == null) {
            return null;
        }
        return new ByteArrayInputStream(val);
    }

    /**
     * Retrieves all or part of the <code>BLOB</code> value that this
     * <code>Blob</code> object represents, as an array of bytes. This
     * <code>byte</code> array contains up to <code>length</code>
     * consecutive bytes starting at position <code>pos</code>.
     * 
     * @param pos
     *            the ordinal position of the first byte in the
     *            <code>BLOB</code> value to be extracted; the first byte is
     *            at position 1
     * @param length
     *            the number of consecutive bytes to be copied
     * @return a byte array containing up to <code>length</code> consecutive
     *         bytes from the <code>BLOB</code> value designated by this
     *         <code>Blob</code> object, starting with the byte at position
     *         <code>pos</code>
     * @exception SQLException
     *                if there is an error accessing the <code>BLOB</code>
     *                value
     * @see #setBytes
     * @since 1.2
     * 
     */
    public byte[] getBytes(long pos, int length) throws SQLException {
        if (val == null) {
            return null;
        }
        if (pos + length - 1 > val.length) {
            length = val.length - (int) pos + 1;
        }
        if (length > 0) {
            byte[] bs = new byte[length];
            System.arraycopy(val, (int) pos - 1, bs, 0, length);
            return bs;
        } else {
            return new byte[0];
        }
    }

    /**
     * Returns the number of bytes in the <code>BLOB</code> value designated
     * by this <code>Blob</code> object.
     * 
     * @return length of the <code>BLOB</code> in bytes
     * @exception SQLException
     *                if there is an error accessing the length of the
     *                <code>BLOB</code>
     * @since 1.2
     * 
     */
    public long length() throws SQLException {
        return val == null ? 0L : val.length;
    }

    /**
     * Retrieves the byte position at which the specified byte array
     * <code>pattern</code> begins within the <code>BLOB</code> value that
     * this <code>Blob</code> object represents. The search for
     * <code>pattern</code> begins at position <code>start</code>.
     * 
     * @param pattern
     *            the byte array for which to search
     * @param start
     *            the position at which to begin searching; the first position
     *            is 1
     * @return the position at which the pattern appears, else -1
     * @exception SQLException
     *                if there is an error accessing the <code>BLOB</code>
     * @since 1.2
     * 
     */
    public long position(byte[] pattern, long start) throws SQLException {
        if (pattern == null || pattern.length == 0) {
            return start;
        }
        if (start > val.length) {
            return -1;
        }
        int currentPos = (int) start - 1;
        while (currentPos < val.length - pattern.length) {
            boolean found = true;
            for (int i = 0; i < pattern.length; i++) {
                if (val[currentPos + i] != pattern[i]) {
                    found = false;
                    break;
                }
            }
            currentPos++;
            if (found) {
                return currentPos;
            }
        }
        return -1;
    }

    /**
     * Retrieves the byte position in the <code>BLOB</code> value designated
     * by this <code>Blob</code> object at which <code>pattern</code>
     * begins. The search begins at position <code>start</code>.
     * 
     * @param pattern
     *            the <code>Blob</code> object designating the
     *            <code>BLOB</code> value for which to search
     * @param start
     *            the position in the <code>BLOB</code> value at which to
     *            begin searching; the first position is 1
     * @return the position at which the pattern begins, else -1
     * @exception SQLException
     *                if there is an error accessing the <code>BLOB</code>
     *                value
     * @since 1.2
     * 
     */
    public long position(Blob pattern, long start) throws SQLException {
        byte[] pBytes = pattern.getBytes(0, (int) pattern.length());
        if (pattern == null) {
            return -1;
        }
        return position(pBytes, start);
    }

    /**
     * Retrieves a stream that can be used to write to the <code>BLOB</code>
     * value that this <code>Blob</code> object represents. The stream begins
     * at position <code>pos</code>.
     * 
     * @param pos
     *            the position in the <code>BLOB</code> value at which to
     *            start writing
     * @return a <code>java.io.OutputStream</code> object to which data can be
     *         written
     * @exception SQLException
     *                if there is an error accessing the <code>BLOB</code>
     *                value
     * @see #getBinaryStream
     * @since 1.4
     * 
     */
    public java.io.OutputStream setBinaryStream(long pos) throws SQLException {
        return new XBlobOutputStream(pos);
    }

    /**
     * Writes the given array of bytes to the <code>BLOB</code> value that
     * this <code>Blob</code> object represents, starting at position
     * <code>pos</code>, and returns the number of bytes written.
     * 
     * @param pos
     *            the position in the <code>BLOB</code> object at which to
     *            start writing
     * @param bytes
     *            the array of bytes to be written to the <code>BLOB</code>
     *            value that this <code>Blob</code> object represents
     * @return the number of bytes written
     * @exception SQLException
     *                if there is an error accessing the <code>BLOB</code>
     *                value
     * @see #getBytes
     * @since 1.4
     * 
     */
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        return setBytes(pos, bytes, 0, bytes.length);
    }

    /**
     * Writes all or part of the given <code>byte</code> array to the
     * <code>BLOB</code> value that this <code>Blob</code> object represents
     * and returns the number of bytes written. Writing starts at position
     * <code>pos</code> in the <code>BLOB</code> value; <code>len</code>
     * bytes from the given byte array are written.
     * 
     * @param pos
     *            the position in the <code>BLOB</code> object at which to
     *            start writing
     * @param bytes
     *            the array of bytes to be written to this <code>BLOB</code>
     *            object
     * @param offset
     *            the offset into the array <code>bytes</code> at which to
     *            start reading the bytes to be set
     * @param len
     *            the number of bytes to be written to the <code>BLOB</code>
     *            value from the array of bytes <code>bytes</code>
     * @return the number of bytes written
     * @exception SQLException
     *                if there is an error accessing the <code>BLOB</code>
     *                value
     * @see #getBytes
     * @since 1.4
     * 
     */
    public int setBytes(long pos, byte[] bytes, int offset, int len)
            throws SQLException {
        byte[] newVal = new byte[(int) pos - 1 + len];
        if (pos - 1 > val.length) {
            System.arraycopy(val, 0, newVal, 0, val.length);
            for (int i = val.length; i < (int) pos; i++) {
                newVal[i] = 0;
            }
            System.arraycopy(bytes, offset, newVal, (int) pos, len);
            val = newVal;
        } else {
            System.arraycopy(val, 0, newVal, 0, (int) pos - 1);
            System.arraycopy(bytes, offset, val, (int) pos, len);
        }
        return len;
    }

    /**
     * Truncates the <code>BLOB</code> value that this <code>Blob</code>
     * object represents to be <code>len</code> bytes in length.
     * 
     * @param len
     *            the length, in bytes, to which the <code>BLOB</code> value
     *            that this <code>Blob</code> object represents should be
     *            truncated
     * @exception SQLException
     *                if there is an error accessing the <code>BLOB</code>
     *                value
     * @since 1.4
     * 
     */
    public void truncate(long len) throws SQLException {
        byte[] newVal = new byte[(int) len];
        System.arraycopy(val, 0, newVal, 0, (int) len);
        val = newVal;
    }

    @Override
    public String toString() {
        if (val == null) {
            return null;
        }
        return SQLTypeConverter.getHexString(val);
    }

    private class XBlobOutputStream extends ByteArrayOutputStream {
        private long pos;

        private XBlobOutputStream(long pos) {
            super();
            this.pos = pos;
        }

        @Override
        public void flush() throws IOException {
            try {
                setBytes(pos, toByteArray());
            } catch (SQLException se) {
                throw new IOException("Can not write data to BLOB field: "
                        + se.getMessage());
            }
            super.flush();
        }

        @Override
        public void close() throws IOException {
            flush();
            super.close();
        }
    }

    public void free() throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    public InputStream getBinaryStream(long pos, long length)
            throws SQLException {
        // TODO Added for 1.6 compatibility
        return null;
    }
}
