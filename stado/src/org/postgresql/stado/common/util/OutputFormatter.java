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
package org.postgresql.stado.common.util;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.postgresql.stado.util.XdbImpEx;


/**
 *
 *
 */
public class OutputFormatter {
    public String field[];

    private int staPos[];

    String colNames[];

    ResultSet rs;

    private boolean trimTrailingSpaces = false;

    private boolean hasTrailingDelimiter = false;

    private char sFieldSep = Props.XDB_LOADER_NODEWRITER_DEFAULT_DELIMITER.charAt(0);

    private byte[] sTerminator = System.getProperty("line.separator").getBytes();

    private byte[] nullValue = Props.XDB_LOADER_NODEWRITER_DEFAULT_NULL.getBytes();

    private char quoteChar = 0;

    private char quoteEscape = 0;

    private HashSet<String> forceQuoteColumns = null;

    private byte[] rowBuffer = new byte[1024];

    private int position = 0;

    private int notifyInterval;

    private PrintStream notifyWriter;


    /**
     * Ensure the length of the row buffer is not less then specified
     * Increase buffer if necessary.
     * @param capacity
     */
    private void ensureCapacity(int capacity) {
        if (rowBuffer.length < capacity) {
            byte[] newRowBuffer = new byte[Math.max(capacity, rowBuffer.length * 2)];
            System.arraycopy(rowBuffer, 0, newRowBuffer, 0, position);
            rowBuffer = newRowBuffer;
        }
    }

    /**
     * Append bytes to data currently in the row buffer
     * @param data
     */
    private void toRowBuffer(byte[] data) {
        if (data != null && data.length > 0) {
            ensureCapacity(position + data.length);
            System.arraycopy(data, 0, rowBuffer, position, data.length);
            position += data.length;
        }
    }

    /**
     * Append byte to data currently in the row buffer
     * @param data
     */
    private void toRowBuffer(byte data) {
        ensureCapacity(position + 1);
        rowBuffer[position++] = data;
    }

    /**
     * @return the trimTrailingSpaces
     */
    public boolean isTrimTrailingSpaces() {
        return trimTrailingSpaces;
    }

    /**
     * @param trimTrailingSpaces
     *                the trimTrailingSpaces to set
     */
    public void setTrimTrailingSpaces(boolean trimTrailingSpaces) {
        this.trimTrailingSpaces = trimTrailingSpaces;
    }

    /**
     * @return the hasTrailingDelimiter
     */
    public boolean isHasTrailingDelimiter() {
        return hasTrailingDelimiter;
    }

    /**
     * @param hasTrailingDelimiter
     *                the hasTrailingDelimiter to set
     */
    public void setHasTrailingDelimiter(boolean hasTrailingDelimiter) {
        this.hasTrailingDelimiter = hasTrailingDelimiter;
    }

    /**
     * @param fieldSep
     *                the sFieldSep to set
     */
    public void setSFieldSep(String fieldSep) {
        sFieldSep = fieldSep == null || fieldSep.length() == 0 ?
                Props.XDB_LOADER_NODEWRITER_DEFAULT_DELIMITER.charAt(0)
                : fieldSep.charAt(0);
    }

    /**
     * @param terminator
     *                the sTerminator to set
     */
    public void setSTerminator(String terminator) {
        sTerminator = terminator.getBytes();
    }

    public void setNullValue(String nullValue) {
        this.nullValue = nullValue == null ? Props.XDB_LOADER_NODEWRITER_DEFAULT_NULL.getBytes()
                : nullValue.getBytes();
    }

    /**
     *
     * @param posFormatInfo
     * @param quotes
     * @param rs
     * @throws java.sql.SQLException
     */
    public OutputFormatter(String posFormatInfo) {
        if (posFormatInfo != null) {
            StringTokenizer tk = new StringTokenizer(posFormatInfo);
            String token;
            field = new String[tk.countTokens() / 2];
            staPos = new int[tk.countTokens() / 2];
            int tkCount = 0;
            while (tk.hasMoreTokens()) {
                token = tk.nextToken(",");
                token = token.trim();
                int end = token.indexOf(" ", 0);
                field[tkCount] = token.substring(0, end);
                int sta = XdbImpEx.skipWhiteSpaces(end, token);
                end = token.indexOf(":", sta);
                String from = token.substring(sta, end);
                sta = end + 1;
                end = token.length();
                String to = token.substring(sta, end);
                from = from.trim();
                to = to.trim();
                staPos[tkCount] = Integer.parseInt(from);
                tkCount++;
            }
        }

    }

    public long printRS(ResultSet rs, OutputStream outStream)
            throws SQLException, IOException {
        this.rs = rs;
        ResultSetMetaData meta = rs.getMetaData();
        colNames = new String[meta.getColumnCount()];

        for (int col = 0; col < colNames.length; col++) {
            colNames[col] = meta.getColumnName(col + 1);
        }

        long lines = 0;
        if (field != null) {
            while (rs.next()) {
                printFormattedRow();
                outStream.write(rowBuffer, 0, position);
                position = 0;
                lines++;
                if (notifyInterval > 0 && lines % notifyInterval == 0) {
                    notifyWriter.println("Processed " + lines + " rows...");
                }
            }
        } else if (quoteChar == 0) {
            while (rs.next()) {
                printRow();
                outStream.write(rowBuffer, 0, position);
                position = 0;
                lines++;
                if (notifyInterval > 0 && lines % notifyInterval == 0) {
                    notifyWriter.println("Processed " + lines + " rows...");
                }
            }
        } else { // csv
            for (int i = 0; i < colNames.length; i++) {
                if (forceQuoteColumns.contains(colNames[i].toLowerCase())) {
                    colNames[i] = null;
                }
            }
            while (rs.next()) {
                printCsvRow();
                outStream.write(rowBuffer, 0, position);
                position = 0;
                lines++;
                if (notifyInterval > 0 && lines % notifyInterval == 0) {
                    notifyWriter.println("Processed " + lines + " rows...");
                }
            }
        }
        return lines;
    }

    /**
     * output the text export data to row buffer
     *
     * @throws java.sql.SQLException
     */
    private void printRow() throws SQLException {
        String outBuffer = null;
        for (int col = 0; col < colNames.length; col++) {
            outBuffer = rs.getString(col + 1);
            if (outBuffer == null) {
                toRowBuffer(nullValue);
            } else {
                if (trimTrailingSpaces) {
                    outBuffer = trimTrailing(outBuffer);
                }

                // escape the escape sequences before writing to the outStream
                outBuffer = ParseCmdLine.escape(outBuffer, sFieldSep, false);
                toRowBuffer(outBuffer.getBytes());
            }
            if (hasTrailingDelimiter || col < colNames.length - 1) {
                toRowBuffer((byte) sFieldSep);
            }
        }
        toRowBuffer(sTerminator);
    }

    /**
     * Check if specified value contains end-of-value or end-of-line charachter
     * so should be quoted
     * @param value
     * @return
     */
    private boolean hasSpecialCharsCSV(String value) {
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch == sFieldSep || ch == '\n' || ch == '\r') {
                return true;
            }
        }
        return false;
    }

    /**
     * output the CSV export data to row buffer
     *
     * @throws java.sql.SQLException
     */
    private void printCsvRow() throws SQLException {
        String outBuffer = null;
        for (int col = 0; col < colNames.length; col++) {
            outBuffer = rs.getString(col + 1);
            if (outBuffer == null) {
                toRowBuffer(nullValue);
            } else {
                boolean needQuote = colNames[col] == null ||
                    outBuffer.length() > 0 && outBuffer.charAt(0) == quoteChar ||
                    Arrays.equals(outBuffer.getBytes(), nullValue) ||
                    hasSpecialCharsCSV(outBuffer);
                if (needQuote) {
                    toRowBuffer((byte) quoteChar);
                    int startPos = 0;
                    int endPos = outBuffer.indexOf(quoteChar, startPos);
                    while (endPos >= 0) {
                        toRowBuffer(outBuffer.substring(startPos, endPos).getBytes());
                        toRowBuffer((byte) quoteEscape);
                        toRowBuffer((byte) quoteChar);
                        startPos = endPos + 1;
                        endPos = outBuffer.indexOf(quoteChar, startPos);
                    }
                    toRowBuffer(outBuffer.substring(startPos).getBytes());
                    toRowBuffer((byte) quoteChar);
                } else {
                    toRowBuffer(outBuffer.getBytes());
                }
            }
            if (col < colNames.length - 1) {
                toRowBuffer((byte) sFieldSep);
            }
        }
        toRowBuffer(sTerminator);
    }

    /**
     * output the position formatted export data to row buffer
     *
     * @throws java.sql.SQLException
     */
    private void printFormattedRow() throws SQLException,
            IOException {
        // position formatted
        // handle accordingly
        String outBuffer = null;
        int bufLength = 0;
        for (int t = 0; t < field.length; t++) {
            for (int col = 0; col < colNames.length; col++) {
                bufLength = 0;
                if (field[t].equals(colNames[col])) {
                    outBuffer = rs.getString(col + 1);
                    if (outBuffer == null) {
                        toRowBuffer(nullValue);
                    } else if (trimTrailingSpaces) {
                        outBuffer = trimTrailing(outBuffer);
                    }
                    toRowBuffer(outBuffer.getBytes());
                    bufLength = outBuffer.length();

                    int numSpaces = 0;
                    if (t < field.length - 1) {
                        numSpaces = staPos[t + 1] - staPos[t] - bufLength;
                    }
                    for (int i = 0; i < numSpaces; i++) {
                        toRowBuffer((byte) ' ');
                    }
                    break;
                }
            }
        }
        toRowBuffer(sTerminator);
    }

    /**
     * We can't use String.trim() since it trims leading spaces, too, so we use
     * our own here.
     *
     * @param inString
     * @return
     */
    private String trimTrailing(String inString) {
        int pos;

        for (pos = inString.length(); pos > 0; pos--) {
            if (inString.charAt(pos - 1) != ' ') {
                break;
            }
        }
        if (pos == 0) {
            return "";
        } else {
            return inString.substring(0, pos);
        }
    }

    /**
     *
     * @param quoteChar
     * @param quoteEscape
     * @param forceQuoteColumns
     */
    public void setQuoteInfo(String quoteChar, String quoteEscape,
            Collection<String> forceQuoteColumns) {
        this.quoteChar = quoteChar.charAt(0);
        this.quoteEscape = quoteEscape == null ? this.quoteChar : quoteEscape.charAt(0);
        this.forceQuoteColumns = new HashSet<String>();
        if (forceQuoteColumns != null) {
            for (String colName : forceQuoteColumns) {
                this.forceQuoteColumns.add(colName.toLowerCase());
            }
        }
    }

    public void setNotification(int interval, PrintStream out) {
        notifyWriter = out;
        notifyInterval = notifyWriter == null ? 0 : interval;
    }

}
