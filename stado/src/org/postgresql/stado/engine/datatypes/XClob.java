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
 * XClob.java
 *
 *  
 */

package org.postgresql.stado.engine.datatypes;

import java.io.*;
import java.sql.*;

/**
 * 
 *  
 */
public class XClob implements java.sql.Clob {

    private String val = null;

    /** Creates a new instance of XClob */
    public XClob(String val) {
        this.val = val;
    }

    /**
     * Retrieves the <code>CLOB</code> value designated by this
     * <code>Clob</code> object as an ascii stream.
     * 
     * @return a <code>java.io.InputStream</code> object containing the
     *         <code>CLOB</code> data
     * @exception SQLException
     *                if there is an error accessing the <code>CLOB</code>
     *                value
     * @see #setAsciiStream
     * @since 1.2
     * 
     */
    public java.io.InputStream getAsciiStream() throws SQLException {
        if (val == null) {
            return null;
        }
        try {
            return new ByteArrayInputStream(val.getBytes("US-ASCII"));
        } catch (Throwable t) {
            throw new SQLException("error converting to ascii charset");
        }
    }

    /**
     * Retrieves the <code>CLOB</code> value designated by this
     * <code>Clob</code> object as a <code>java.io.Reader</code> object (or
     * as a stream of characters).
     * 
     * @return a <code>java.io.Reader</code> object containing the
     *         <code>CLOB</code> data
     * @exception SQLException
     *                if there is an error accessing the <code>CLOB</code>
     *                value
     * @see #setCharacterStream
     * @since 1.2
     * 
     */
    public java.io.Reader getCharacterStream() throws SQLException {
        if (val == null) {
            return null;
        }
        return new java.io.StringReader(val);
    }

    /**
     * Retrieves a copy of the specified substring in the <code>CLOB</code>
     * value designated by this <code>Clob</code> object. The substring begins
     * at position <code>pos</code> and has up to <code>length</code>
     * consecutive characters.
     * 
     * @param pos
     *            the first character of the substring to be extracted. The
     *            first character is at position 1.
     * @param length
     *            the number of consecutive characters to be copied
     * @return a <code>String</code> that is the specified substring in the
     *         <code>CLOB</code> value designated by this <code>Clob</code>
     *         object
     * @exception SQLException
     *                if there is an error accessing the <code>CLOB</code>
     *                value
     * @since 1.2
     * 
     */
    public String getSubString(long pos, int length) throws SQLException {
        if (val == null) {
            return null;
        }
        return val.substring((int) pos, (int) pos + length - 1);
    }

    /**
     * Retrieves the number of characters in the <code>CLOB</code> value
     * designated by this <code>Clob</code> object.
     * 
     * @return length of the <code>CLOB</code> in characters
     * @exception SQLException
     *                if there is an error accessing the length of the
     *                <code>CLOB</code> value
     * @since 1.2
     * 
     */
    public long length() throws SQLException {
        if (val == null) {
            return 0L;
        }
        return val.length();
    }

    /**
     * Retrieves the character position at which the specified <code>Clob</code>
     * object <code>searchstr</code> appears in this <code>Clob</code>
     * object. The search begins at position <code>start</code>.
     * 
     * @param searchstr
     *            the <code>Clob</code> object for which to search
     * @param start
     *            the position at which to begin searching; the first position
     *            is 1
     * @return the position at which the <code>Clob</code> object appears or
     *         -1 if it is not present; the first position is 1
     * @exception SQLException
     *                if there is an error accessing the <code>CLOB</code>
     *                value
     * @since 1.2
     * 
     */
    public long position(Clob searchstr, long start) throws SQLException {
        return position(searchstr.getSubString(0, (int) searchstr.length()),
                start);
    }

    /**
     * Retrieves the character position at which the specified substring
     * <code>searchstr</code> appears in the SQL <code>CLOB</code> value
     * represented by this <code>Clob</code> object. The search begins at
     * position <code>start</code>.
     * 
     * @param searchstr
     *            the substring for which to search
     * @param start
     *            the position at which to begin searching; the first position
     *            is 1
     * @return the position at which the substring appears or -1 if it is not
     *         present; the first position is 1
     * @exception SQLException
     *                if there is an error accessing the <code>CLOB</code>
     *                value
     * @since 1.2
     * 
     */
    public long position(String searchstr, long start) throws SQLException {
        if (val == null) {
            return -1;
        }
        int foundIdx = val.indexOf(searchstr, (int) start - 1);
        if (foundIdx >= 0) {
            return ++foundIdx;
        }
        return -1;
    }

    /**
     * Retrieves a stream to be used to write Ascii characters to the
     * <code>CLOB</code> value that this <code>Clob</code> object
     * represents, starting at position <code>pos</code>.
     * 
     * @param pos
     *            the position at which to start writing to this
     *            <code>CLOB</code> object
     * @return the stream to which ASCII encoded characters can be written
     * @exception SQLException
     *                if there is an error accessing the <code>CLOB</code>
     *                value
     * @see #getAsciiStream
     * 
     * @since 1.4
     * 
     */
    public java.io.OutputStream setAsciiStream(long pos) throws SQLException {
        return new XClobOutputStream(pos);
    }

    /**
     * Retrieves a stream to be used to write a stream of Unicode characters to
     * the <code>CLOB</code> value that this <code>Clob</code> object
     * represents, at position <code>pos</code>.
     * 
     * @param pos
     *            the position at which to start writing to the
     *            <code>CLOB</code> value
     * 
     * @return a stream to which Unicode encoded characters can be written
     * @exception SQLException
     *                if there is an error accessing the <code>CLOB</code>
     *                value
     * @see #getCharacterStream
     * 
     * @since 1.4
     * 
     */
    public java.io.Writer setCharacterStream(long pos) throws SQLException {
        return new XClobWriter(pos);
    }

    /**
     * Writes the given Java <code>String</code> to the <code>CLOB</code>
     * value that this <code>Clob</code> object designates at the position
     * <code>pos</code>.
     * 
     * @param pos
     *            the position at which to start writing to the
     *            <code>CLOB</code> value that this <code>Clob</code> object
     *            represents
     * @param str
     *            the string to be written to the <code>CLOB</code> value that
     *            this <code>Clob</code> designates
     * @return the number of characters written
     * @exception SQLException
     *                if there is an error accessing the <code>CLOB</code>
     *                value
     * 
     * @since 1.4
     * 
     */
    public int setString(long pos, String str) throws SQLException {
        throw new SQLException("not supported with this version");
    }

    /**
     * Writes <code>len</code> characters of <code>str</code>, starting at
     * character <code>offset</code>, to the <code>CLOB</code> value that
     * this <code>Clob</code> represents.
     * 
     * @param pos
     *            the position at which to start writing to this
     *            <code>CLOB</code> object
     * @param str
     *            the string to be written to the <code>CLOB</code> value that
     *            this <code>Clob</code> object represents
     * @param offset
     *            the offset into <code>str</code> to start reading the
     *            characters to be written
     * @param len
     *            the number of characters to be written
     * @return the number of characters written
     * @exception SQLException
     *                if there is an error accessing the <code>CLOB</code>
     *                value
     * 
     * @since 1.4
     * 
     */
    public int setString(long pos, String str, int offset, int len)
            throws SQLException {
        throw new SQLException("not supported with this version");
    }

    /**
     * Truncates the <code>CLOB</code> value that this <code>Clob</code>
     * designates to have a length of <code>len</code> characters.
     * 
     * @param len
     *            the length, in bytes, to which the <code>CLOB</code> value
     *            should be truncated
     * @exception SQLException
     *                if there is an error accessing the <code>CLOB</code>
     *                value
     * 
     * @since 1.4
     * 
     */
    public void truncate(long len) throws SQLException {
        throw new SQLException("not supported with this version");
    }

    @Override
    public String toString() {
        return val;
    }

    private class XClobOutputStream extends ByteArrayOutputStream {
        private long pos;

        private XClobOutputStream(long pos) {
            super();
            this.pos = pos;
        }

        @Override
        public void flush() throws IOException {
            try {
                setString(pos, new String(toByteArray(), "US-ASCII"));
            } catch (Exception e) {
                throw new IOException("Can not write data to CLOB field: "
                        + e.getMessage());
            }
            super.flush();
        }

        @Override
        public void close() throws IOException {
            flush();
            super.close();
        }
    }

    private class XClobWriter extends StringWriter {
        private long pos;

        private XClobWriter(long pos) {
            super();
            this.pos = pos;
        }

        @Override
        public void flush()// throws IOException
        {
            try {
                setString(pos, toString());
            } catch (Exception e) {
                // StringWriter do not declare an exception thrown so we have to
                // ignore the error
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

    public Reader getCharacterStream(long pos, long length) throws SQLException {
        // TODO Added for 1.6 compatibility
        return null;
    }
}
