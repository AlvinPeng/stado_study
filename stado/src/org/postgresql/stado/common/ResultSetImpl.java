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
package org.postgresql.stado.common;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import org.postgresql.stado.common.util.ErrorCodes;
import org.postgresql.stado.common.util.SQLErrorHandler;
import org.postgresql.stado.common.util.SQLTypeConverter;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.datatypes.XData;
import org.postgresql.stado.engine.io.ResultSetResponse;


/**
 * 
 *  
 */
public class ResultSetImpl implements java.sql.ResultSet {
    private static final XLogger logger = XLogger
            .getLogger(ResultSetImpl.class);

    protected ResultSetResponse responseMessage = null;// created in the
                                                        // Statement

    protected ColumnMetaData[] columnMeta = null;

    private String[] columns;// column names

    private List<XData[]> rows = null;

    private int totalRows = 0;

    private XData[] currentRow = {};

    private int currentRowIndex = -1;

    private boolean hasMoreRowsToFetch = false;

    private SQLWarning sqlWarning = null;

    private int concurrency = ResultSet.CONCUR_READ_ONLY;

    private int scrollType = ResultSet.TYPE_FORWARD_ONLY;// can be scrollable

    private int fetchSize = 200;

    private int fetchDirection = ResultSet.FETCH_FORWARD;// forward only,
                                                            // don't see a use
                                                            // for reverse
                                                            // fetching

    private boolean wasLastValueNull = true;// was the last valued retrieved
                                            // null?

    protected ResultSetImpl() {
        this.scrollType = ResultSet.TYPE_FORWARD_ONLY;
        fetchDirection = ResultSet.FETCH_FORWARD;
    }

    /**
     * for some a quick resultset
     * @param columnMeta 
     * @param rows 
     */
    public ResultSetImpl(ColumnMetaData[] columnMeta, List<XData[]> rows) {
        setColumnMetaData(columnMeta);
        this.rows = rows;
        this.totalRows = rows != null ? rows.size() : 0;
        this.hasMoreRowsToFetch = false;
    }

    /**
     * 
     * @param columnMeta 
     */
    public void setColumnMetaData(ColumnMetaData[] columnMeta) {
        this.columnMeta = columnMeta;

        columns = new String[columnMeta.length];
        for (int i = 0; i < columnMeta.length; i++) {
            columns[i] = columnMeta[i].alias;
        }
    }

    /**
     * Moves the cursor to the given row number in this <code>ResultSet</code>
     * object.
     * 
     * <p>
     * If the row number is positive, the cursor moves to the given row number
     * with respect to the beginning of the result set. The first row is row 1,
     * the second is row 2, and so on.
     * 
     * <p>
     * If the given row number is negative, the cursor moves to an absolute row
     * position with respect to the end of the result set. For example, calling
     * the method <code>absolute(-1)</code> positions the cursor on the last
     * row; calling the method <code>absolute(-2)</code> moves the cursor to
     * the next-to-last row, and so on.
     * 
     * <p>
     * An attempt to position the cursor beyond the first/last row in the result
     * set leaves the cursor before the first row or after the last row.
     * 
     * <p>
     * <B>Note:</B> Calling <code>absolute(1)</code> is the same as calling
     * <code>first()</code>. Calling <code>absolute(-1)</code> is the same
     * as calling <code>last()</code>.
     * 
     * @param row
     *            the number of the row to which the cursor should move. A
     *            positive number indicates the row number counting from the
     *            beginning of the result set; a negative number indicates the
     *            row number counting from the end of the result set
     * @return <code>true</code> if the cursor is on the result set;
     *         <code>false</code> otherwise
     * @exception SQLException
     *                if a database access error occurs, or the result set type
     *                is <code>TYPE_FORWARD_ONLY</code>
     * @since 1.2
     * 
     */
    public boolean absolute(int row) throws SQLException {
        if (this.scrollType == ResultSet.TYPE_FORWARD_ONLY) {
            throw SQLErrorHandler
                    .getError(ErrorCodes.METHOD_INVALID_OPERATION,
                            "operation absolute() cannot be done with scroll type FORWARD ONLY");
        }
        if (row == 0) {
            throw SQLErrorHandler.getError(ErrorCodes.METHOD_INVALID_OPERATION,
                    "invalid index to move: " + row);
        }
        if (rows == null || rows.size() == 0) {
            logger.debug("no rows in absolute() - ignore");
            return false;
        }

        if (Math.abs(row) > this.totalRows) {
            getAllPackets();
        }
        if (Math.abs(row) > this.totalRows) {
            throw SQLErrorHandler.getError(ErrorCodes.METHOD_INVALID_OPERATION,
                    "invalid index to move: " + row);
        }

        if (row < 0) {
            // counting from the end
            this.resetCurrentRow(totalRows + row - 1);
        } else {
            this.resetCurrentRow(row - 1);// array idx starts at 0
        }
        return true;
    }

    /**
     * Moves the cursor to the end of this <code>ResultSet</code> object, just
     * after the last row. This method has no effect if the result set contains
     * no rows.
     * 
     * @exception SQLException
     *                if a database access error occurs or the result set type
     *                is <code>TYPE_FORWARD_ONLY</code>
     * @since 1.2
     * 
     */
    public void afterLast() throws SQLException {
        if (this.scrollType == ResultSet.TYPE_FORWARD_ONLY) {
            throw SQLErrorHandler
                    .getError(ErrorCodes.METHOD_INVALID_OPERATION,
                            "operation afterLast() cannot be done with scroll type FORWARD ONLY");
        }
        if (rows == null || totalRows == 0) {
            return;
        }
        this.currentRowIndex = totalRows;
        this.currentRow = null;
    }

    /**
     * Moves the cursor to the front of this <code>ResultSet</code> object,
     * just before the first row. This method has no effect if the result set
     * contains no rows.
     * 
     * @exception SQLException
     *                if a database access error occurs or the result set type
     *                is <code>TYPE_FORWARD_ONLY</code>
     * @since 1.2
     * 
     */
    public void beforeFirst() throws SQLException {
        if (this.scrollType == ResultSet.TYPE_FORWARD_ONLY) {
            throw SQLErrorHandler
                    .getError(ErrorCodes.METHOD_INVALID_OPERATION,
                            "operation beforeFirst() cannot be done with scroll type FORWARD ONLY");
        }
        if (rows == null || totalRows == 0) {
            return;
        }
        this.currentRowIndex = -1;
        this.currentRow = null;
    }

    /**
     * Cancels the updates made to the current row in this
     * <code>ResultSet</code> object. This method may be called after calling
     * an updater method(s) and before calling the method <code>updateRow</code>
     * to roll back the updates made to a row. If no updates have been made or
     * <code>updateRow</code> has already been called, this method has no
     * effect.
     * 
     * @exception SQLException
     *                if a database access error occurs or if this method is
     *                called when the cursor is on the insert row
     * @since 1.2
     * 
     */
    public void cancelRowUpdates() throws SQLException {
    }

    /**
     * Clears all warnings reported on this <code>ResultSet</code> object.
     * After this method is called, the method <code>getWarnings</code>
     * returns <code>null</code> until a new warning is reported for this
     * <code>ResultSet</code> object.
     * 
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public void clearWarnings() throws SQLException {
        this.sqlWarning = null;
    }

    /**
     * Releases this <code>ResultSet</code> object's database and JDBC
     * resources immediately instead of waiting for this to happen when it is
     * automatically closed.
     * 
     * <P>
     * <B>Note:</B> A <code>ResultSet</code> object is automatically closed
     * by the <code>Statement</code> object that generated it when that
     * <code>Statement</code> object is closed, re-executed, or is used to
     * retrieve the next result from a sequence of multiple results. A
     * <code>ResultSet</code> object is also automatically closed when it is
     * garbage collected.
     * 
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public void close() throws SQLException {
    }

    /**
     * Deletes the current row from this <code>ResultSet</code> object and
     * from the underlying database. This method cannot be called when the
     * cursor is on the insert row.
     * 
     * @exception SQLException
     *                if a database access error occurs or if this method is
     *                called when the cursor is on the insert row
     * @since 1.2
     * 
     */
    public void deleteRow() throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Maps the given <code>ResultSet</code> column name to its
     * <code>ResultSet</code> column index.
     * 
     * @param columnName
     *            the name of the column
     * @return the column index of the given column name
     * @exception SQLException
     *                if the <code>ResultSet</code> object does not contain
     *                <code>columnName</code> or a database access error
     *                occurs
     * 
     */
    public int findColumn(String columnName) throws SQLException {
        int idx = -1;
        for (int i = 0, len = columns.length; i < len; i++) {
            if (columnName.equalsIgnoreCase(columns[i])) {
                idx = i + 1;
            }
        }
        if (idx < 1) {
            throw SQLErrorHandler.getError(ErrorCodes.AN_ERROR_HAS_OCCURRED,
                    "column name " + columnName + " not found in resultset");
        }
        return idx;
    }

    /**
     * Moves the cursor to the first row in this <code>ResultSet</code>
     * object.
     * 
     * @return <code>true</code> if the cursor is on a valid row;
     *         <code>false</code> if there are no rows in the result set
     * @exception SQLException
     *                if a database access error occurs or the result set type
     *                is <code>TYPE_FORWARD_ONLY</code>
     * @since 1.2
     * 
     */
    public boolean first() throws SQLException {
        if (this.scrollType == ResultSet.TYPE_FORWARD_ONLY) {
            throw SQLErrorHandler.getError(ErrorCodes.METHOD_INVALID_OPERATION,
                    "operation not permitted with FORWARD_ONLY scroll type");
        }
        if (this.totalRows == 0) {
            return false;
        }
        resetCurrentRow(0);
        return true;
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as an <code>Array</code> object in the
     * Java programming language.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @return an <code>Array</code> object representing the SQL
     *         <code>ARRAY</code> value in the specified column
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public Array getArray(int columnIndex) throws SQLException {
        XData x = currentRow[columnIndex - 1];
        wasLastValueNull = x == null || x.isValueNull();
        return wasLastValueNull ? null : SQLTypeConverter.getArray(x
                .getObject());
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as an <code>Array</code> object in the
     * Java programming language.
     * 
     * @param colName
     *            the name of the column from which to retrieve the value
     * @return an <code>Array</code> object representing the SQL
     *         <code>ARRAY</code> value in the specified column
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public Array getArray(String colName) throws SQLException {
        throw new SQLException("not supported yet");
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a stream of ASCII characters. The
     * value can then be read in chunks from the stream. This method is
     * particularly suitable for retrieving large <char>LONGVARCHAR</char>
     * values. The JDBC driver will do any necessary conversion from the
     * database format into ASCII.
     * 
     * <P>
     * <B>Note:</B> All the data in the returned stream must be read prior to
     * getting the value of any other column. The next call to a getter method
     * implicitly closes the stream. Also, a stream may return <code>0</code>
     * when the method <code>InputStream.available</code> is called whether
     * there is data available or not.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @return a Java input stream that delivers the database column value as a
     *         stream of one-byte ASCII characters; if the value is SQL
     *         <code>NULL</code>, the value returned is <code>null</code>
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public java.io.InputStream getAsciiStream(int columnIndex)
            throws SQLException {
        XData x = currentRow[columnIndex - 1];
        wasLastValueNull = x == null || x.isValueNull();
        return wasLastValueNull ? null : SQLTypeConverter.getAsciiStream(x
                .getObject());
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a stream of ASCII characters. The
     * value can then be read in chunks from the stream. This method is
     * particularly suitable for retrieving large <code>LONGVARCHAR</code>
     * values. The JDBC driver will do any necessary conversion from the
     * database format into ASCII.
     * 
     * <P>
     * <B>Note:</B> All the data in the returned stream must be read prior to
     * getting the value of any other column. The next call to a getter method
     * implicitly closes the stream. Also, a stream may return <code>0</code>
     * when the method <code>available</code> is called whether there is data
     * available or not.
     * 
     * @param columnName
     *            the SQL name of the column
     * @return a Java input stream that delivers the database column value as a
     *         stream of one-byte ASCII characters. If the value is SQL
     *         <code>NULL</code>, the value returned is <code>null</code>.
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public java.io.InputStream getAsciiStream(String columnName)
            throws SQLException {
        return getAsciiStream(findColumn(columnName));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>java.math.BigDecimal</code>
     * with full precision.
     * 
     * @param columnName
     *            the column name
     * @return the column value (full precision); if the value is SQL
     *         <code>NULL</code>, the value returned is <code>null</code>
     *         in the Java programming language.
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     * 
     */
    public BigDecimal getBigDecimal(String columnName) throws SQLException {
        return getBigDecimal(findColumn(columnName));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>java.math.BigDecimal</code>
     * with full precision.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @return the column value (full precision); if the value is SQL
     *         <code>NULL</code>, the value returned is <code>null</code>
     *         in the Java programming language.
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        XData x = currentRow[columnIndex - 1];
        wasLastValueNull = x == null || x.isValueNull();
        return wasLastValueNull ? null : SQLTypeConverter.getBigDecimal(x
                .getObject());
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>java.sql.BigDecimal</code>
     * in the Java programming language.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @param scale
     *            the number of digits to the right of the decimal point
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>null</code>
     * @exception SQLException
     *                if a database access error occurs
     * @deprecated
     * 
     */
    @Deprecated
    public BigDecimal getBigDecimal(int columnIndex, int scale)
            throws SQLException {
        return getBigDecimal(columnIndex);
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>java.math.BigDecimal</code>
     * in the Java programming language.
     * 
     * @param columnName
     *            the SQL name of the column
     * @param scale
     *            the number of digits to the right of the decimal point
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>null</code>
     * @exception SQLException
     *                if a database access error occurs
     * @deprecated
     * 
     */
    @Deprecated
    public BigDecimal getBigDecimal(String columnName, int scale)
            throws SQLException {
        return getBigDecimal(findColumn(columnName));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a binary stream of uninterpreted
     * bytes. The value can then be read in chunks from the stream. This method
     * is particularly suitable for retrieving large <code>LONGVARBINARY</code>
     * values.
     * 
     * <P>
     * <B>Note:</B> All the data in the returned stream must be read prior to
     * getting the value of any other column. The next call to a getter method
     * implicitly closes the stream. Also, a stream may return <code>0</code>
     * when the method <code>InputStream.available</code> is called whether
     * there is data available or not.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @return a Java input stream that delivers the database column value as a
     *         stream of uninterpreted bytes; if the value is SQL
     *         <code>NULL</code>, the value returned is <code>null</code>
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public java.io.InputStream getBinaryStream(int columnIndex)
            throws SQLException {
        XData x = currentRow[columnIndex - 1];
        wasLastValueNull = x == null || x.isValueNull();
        return wasLastValueNull ? null : SQLTypeConverter.getBinaryStream(x
                .getObject());
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a stream of uninterpreted
     * <code>byte</code>s. The value can then be read in chunks from the
     * stream. This method is particularly suitable for retrieving large
     * <code>LONGVARBINARY</code> values.
     * 
     * <P>
     * <B>Note:</B> All the data in the returned stream must be read prior to
     * getting the value of any other column. The next call to a getter method
     * implicitly closes the stream. Also, a stream may return <code>0</code>
     * when the method <code>available</code> is called whether there is data
     * available or not.
     * 
     * @param columnName
     *            the SQL name of the column
     * @return a Java input stream that delivers the database column value as a
     *         stream of uninterpreted bytes; if the value is SQL
     *         <code>NULL</code>, the result is <code>null</code>
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public java.io.InputStream getBinaryStream(String columnName)
            throws SQLException {
        return getBinaryStream(findColumn(columnName));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>Blob</code> object in the
     * Java programming language.
     * 
     * @param colName
     *            the name of the column from which to retrieve the value
     * @return a <code>Blob</code> object representing the SQL
     *         <code>BLOB</code> value in the specified column
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public Blob getBlob(String colName) throws SQLException {
        return getBlob(findColumn(colName));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>Blob</code> object in the
     * Java programming language.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @return a <code>Blob</code> object representing the SQL
     *         <code>BLOB</code> value in the specified column
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public Blob getBlob(int columnIndex) throws SQLException {
        XData x = currentRow[columnIndex - 1];
        wasLastValueNull = x == null || x.isValueNull();
        return wasLastValueNull ? null : SQLTypeConverter
                .getBlob(x.getObject());
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>boolean</code> in the Java
     * programming language.
     * 
     * @param columnName
     *            the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>false</code>
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public boolean getBoolean(String columnName) throws SQLException {
        return getBoolean(findColumn(columnName));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>boolean</code> in the Java
     * programming language.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>false</code>
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public boolean getBoolean(int columnIndex) throws SQLException {
        XData x = currentRow[columnIndex - 1];
        wasLastValueNull = x == null || x.isValueNull();
        return wasLastValueNull ? false : SQLTypeConverter.getBoolean(x
                .getObject());
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>byte</code> in the Java
     * programming language.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>0</code>
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public byte getByte(int columnIndex) throws SQLException {
        XData x = currentRow[columnIndex - 1];
        wasLastValueNull = x == null || x.isValueNull();
        return wasLastValueNull ? 0 : SQLTypeConverter.getByte(x.getObject());
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>byte</code> in the Java
     * programming language.
     * 
     * @param columnName
     *            the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>0</code>
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public byte getByte(String columnName) throws SQLException {
        return getByte(findColumn(columnName));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>byte</code> array in the
     * Java programming language. The bytes represent the raw values returned by
     * the driver.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>null</code>
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public byte[] getBytes(int columnIndex) throws SQLException {
        XData x = currentRow[columnIndex - 1];
        wasLastValueNull = x == null || x.isValueNull();
        return wasLastValueNull ? null : SQLTypeConverter.getBytes(x
                .getObject());
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>byte</code> array in the
     * Java programming language. The bytes represent the raw values returned by
     * the driver.
     * 
     * @param columnName
     *            the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>null</code>
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public byte[] getBytes(String columnName) throws SQLException {
        return getBytes(findColumn(columnName));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>java.io.Reader</code>
     * object.
     * 
     * @return a <code>java.io.Reader</code> object that contains the column
     *         value; if the value is SQL <code>NULL</code>, the value
     *         returned is <code>null</code> in the Java programming language.
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public java.io.Reader getCharacterStream(int columnIndex)
            throws SQLException {
        XData x = currentRow[columnIndex - 1];
        wasLastValueNull = x == null || x.isValueNull();
        return wasLastValueNull ? null : SQLTypeConverter.getCharacterStream(x
                .getObject());
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>java.io.Reader</code>
     * object.
     * 
     * @param columnName
     *            the name of the column
     * @return a <code>java.io.Reader</code> object that contains the column
     *         value; if the value is SQL <code>NULL</code>, the value
     *         returned is <code>null</code> in the Java programming language
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public java.io.Reader getCharacterStream(String columnName)
            throws SQLException {
        return getCharacterStream(findColumn(columnName));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>Clob</code> object in the
     * Java programming language.
     * 
     * @param colName
     *            the name of the column from which to retrieve the value
     * @return a <code>Clob</code> object representing the SQL
     *         <code>CLOB</code> value in the specified column
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public Clob getClob(String colName) throws SQLException {
        return getClob(findColumn(colName));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>Clob</code> object in the
     * Java programming language.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @return a <code>Clob</code> object representing the SQL
     *         <code>CLOB</code> value in the specified column
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public Clob getClob(int columnIndex) throws SQLException {
        XData x = currentRow[columnIndex - 1];
        wasLastValueNull = x == null || x.isValueNull();
        return wasLastValueNull ? null : SQLTypeConverter
                .getClob(x.getObject());
    }

    /**
     * Retrieves the concurrency mode of this <code>ResultSet</code> object.
     * The concurrency used is determined by the <code>Statement</code> object
     * that created the result set.
     * 
     * @return the concurrency type, either
     *         <code>ResultSet.CONCUR_READ_ONLY</code> or
     *         <code>ResultSet.CONCUR_UPDATABLE</code>
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public int getConcurrency() throws SQLException {
        return concurrency;
    }

    /**
     * Retrieves the name of the SQL cursor used by this <code>ResultSet</code>
     * object.
     * 
     * <P>
     * In SQL, a result table is retrieved through a cursor that is named. The
     * current row of a result set can be updated or deleted using a positioned
     * update/delete statement that references the cursor name. To insure that
     * the cursor has the proper isolation level to support update, the cursor's
     * <code>SELECT</code> statement should be of the form
     * <code>SELECT FOR UPDATE</code>. If <code>FOR UPDATE</code> is
     * omitted, the positioned updates may fail.
     * 
     * <P>
     * The JDBC API supports this SQL feature by providing the name of the SQL
     * cursor used by a <code>ResultSet</code> object. The current row of a
     * <code>ResultSet</code> object is also the current row of this SQL
     * cursor.
     * 
     * <P>
     * <B>Note:</B> If positioned update is not supported, a
     * <code>SQLException</code> is thrown.
     * 
     * @return the SQL name for this <code>ResultSet</code> object's cursor
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public String getCursorName() throws SQLException {
        // to do?
        return null;
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>java.sql.Date</code> object
     * in the Java programming language.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>null</code>
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public java.sql.Date getDate(int columnIndex) throws SQLException {
        return getDate(columnIndex, Calendar.getInstance());
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>java.sql.Date</code> object
     * in the Java programming language.
     * 
     * @param columnName
     *            the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>null</code>
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public java.sql.Date getDate(String columnName) throws SQLException {
        return getDate(findColumn(columnName), Calendar.getInstance());
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>java.sql.Date</code> object
     * in the Java programming language. This method uses the given calendar to
     * construct an appropriate millisecond value for the date if the underlying
     * database does not store timezone information.
     * 
     * @param columnName
     *            the SQL name of the column from which to retrieve the value
     * @param cal
     *            the <code>java.util.Calendar</code> object to use in
     *            constructing the date
     * @return the column value as a <code>java.sql.Date</code> object; if the
     *         value is SQL <code>NULL</code>, the value returned is
     *         <code>null</code> in the Java programming language
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public java.sql.Date getDate(String columnName, Calendar cal)
            throws SQLException {
        return getDate(findColumn(columnName), cal);
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>java.sql.Date</code> object
     * in the Java programming language. This method uses the given calendar to
     * construct an appropriate millisecond value for the date if the underlying
     * database does not store timezone information.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @param cal
     *            the <code>java.util.Calendar</code> object to use in
     *            constructing the date
     * @return the column value as a <code>java.sql.Date</code> object; if the
     *         value is SQL <code>NULL</code>, the value returned is
     *         <code>null</code> in the Java programming language
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public java.sql.Date getDate(int columnIndex, Calendar cal)
            throws SQLException {
        XData x = currentRow[columnIndex - 1];
        wasLastValueNull = x == null || x.isValueNull();
        return wasLastValueNull ? null : SQLTypeConverter.getDate(
                x.getObject(), cal);
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>double</code> in the Java
     * programming language.
     * 
     * @param columnName
     *            the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>0</code>
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public double getDouble(String columnName) throws SQLException {
        return getDouble(findColumn(columnName));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>double</code> in the Java
     * programming language.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>0</code>
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public double getDouble(int columnIndex) throws SQLException {
        XData x = currentRow[columnIndex - 1];
        wasLastValueNull = x == null || x.isValueNull();
        return wasLastValueNull ? 0 : SQLTypeConverter.getDouble(x.getObject());
    }

    /**
     * Retrieves the fetch direction for this <code>ResultSet</code> object.
     * 
     * @return the current fetch direction for this <code>ResultSet</code>
     *         object
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * @see #setFetchDirection
     * 
     */
    public int getFetchDirection() throws SQLException {
        return this.fetchDirection;
    }

    /**
     * Retrieves the fetch size for this <code>ResultSet</code> object.
     * 
     * @return the current fetch size for this <code>ResultSet</code> object
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * @see #setFetchSize
     * 
     */
    public int getFetchSize() throws SQLException {
        return this.fetchSize;
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>float</code> in the Java
     * programming language.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>0</code>
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public float getFloat(int columnIndex) throws SQLException {
        XData x = currentRow[columnIndex - 1];
        wasLastValueNull = x == null || x.isValueNull();
        return wasLastValueNull ? 0 : SQLTypeConverter.getFloat(x.getObject());
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>float</code> in the Java
     * programming language.
     * 
     * @param columnName
     *            the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>0</code>
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public float getFloat(String columnName) throws SQLException {
        return getFloat(findColumn(columnName));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as an <code>int</code> in the Java
     * programming language.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>0</code>
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public int getInt(int columnIndex) throws SQLException {
        XData x = currentRow[columnIndex - 1];
        wasLastValueNull = x == null || x.isValueNull();
        return wasLastValueNull ? 0 : SQLTypeConverter.getInt(x.getObject());
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as an <code>int</code> in the Java
     * programming language.
     * 
     * @param columnName
     *            the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>0</code>
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public int getInt(String columnName) throws SQLException {
        return getInt(findColumn(columnName));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>long</code> in the Java
     * programming language.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>0</code>
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public long getLong(int columnIndex) throws SQLException {
        XData x = currentRow[columnIndex - 1];
        wasLastValueNull = x == null || x.isValueNull();
        return wasLastValueNull ? 0 : SQLTypeConverter.getLong(x.getObject());
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>long</code> in the Java
     * programming language.
     * 
     * @param columnName
     *            the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>0</code>
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public long getLong(String columnName) throws SQLException {
        return getLong(findColumn(columnName));
    }

    /**
     * Retrieves the number, types and properties of this <code>ResultSet</code>
     * object's columns.
     * 
     * @return the description of this <code>ResultSet</code> object's columns
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public ResultSetMetaData getMetaData() throws SQLException {
        ResultSetMetaData meta = new XDBResultSetMetaData(this.columnMeta);
        return meta;
    }

    /**
     * <p>
     * Gets the value of the designated column in the current row of this
     * <code>ResultSet</code> object as an <code>Object</code> in the Java
     * programming language.
     * 
     * <p>
     * This method will return the value of the given column as a Java object.
     * The type of the Java object will be the default Java object type
     * corresponding to the column's SQL type, following the mapping for
     * built-in types specified in the JDBC specification. If the value is an
     * SQL <code>NULL</code>, the driver returns a Java <code>null</code>.
     * <P>
     * This method may also be used to read datatabase-specific abstract data
     * types.
     * <P>
     * In the JDBC 2.0 API, the behavior of the method <code>getObject</code>
     * is extended to materialize data of SQL user-defined types. When a column
     * contains a structured or distinct value, the behavior of this method is
     * as if it were a call to: <code>getObject(columnIndex,
     * this.getStatement().getConnection().getTypeMap())</code>.
     * 
     * @param columnName
     *            the SQL name of the column
     * @return a <code>java.lang.Object</code> holding the column value
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public Object getObject(String columnName) throws SQLException {
        return getObject(findColumn(columnName));
    }

    /**
     * <p>
     * Gets the value of the designated column in the current row of this
     * <code>ResultSet</code> object as an <code>Object</code> in the Java
     * programming language.
     * 
     * <p>
     * This method will return the value of the given column as a Java object.
     * The type of the Java object will be the default Java object type
     * corresponding to the column's SQL type, following the mapping for
     * built-in types specified in the JDBC specification. If the value is an
     * SQL <code>NULL</code>, the driver returns a Java <code>null</code>.
     * 
     * <p>
     * This method may also be used to read datatabase-specific abstract data
     * types.
     * 
     * In the JDBC 2.0 API, the behavior of method <code>getObject</code> is
     * extended to materialize data of SQL user-defined types. When a column
     * contains a structured or distinct value, the behavior of this method is
     * as if it were a call to: <code>getObject(columnIndex,
     * this.getStatement().getConnection().getTypeMap())</code>.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @return a <code>java.lang.Object</code> holding the column value
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public Object getObject(int columnIndex) throws SQLException {
        XData x = currentRow[columnIndex - 1];
        wasLastValueNull = x == null || x.isValueNull();
        return wasLastValueNull ? null : x.getObject();
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as an <code>Object</code> in the Java
     * programming language. If the value is an SQL <code>NULL</code>, the
     * driver returns a Java <code>null</code>. This method uses the
     * specified <code>Map</code> object for custom mapping if appropriate.
     * 
     * @param colName
     *            the name of the column from which to retrieve the value
     * @param map
     *            a <code>java.util.Map</code> object that contains the
     *            mapping from SQL type names to classes in the Java programming
     *            language
     * @return an <code>Object</code> representing the SQL value in the
     *         specified column
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public Object getObject(String colName, Map<String, Class<?>> map)
            throws SQLException {
        return getObject(findColumn(colName), map);
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as an <code>Object</code> in the Java
     * programming language. If the value is an SQL <code>NULL</code>, the
     * driver returns a Java <code>null</code>. This method uses the given
     * <code>Map</code> object for the custom mapping of the SQL structured or
     * distinct type that is being retrieved.
     * 
     * @param i
     *            the first column is 1, the second is 2, ...
     * @param map
     *            a <code>java.util.Map</code> object that contains the
     *            mapping from SQL type names to classes in the Java programming
     *            language
     * @return an <code>Object</code> in the Java programming language
     *         representing the SQL value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public Object getObject(int i, Map<String, Class<?>> map)
            throws SQLException {
        throw new SQLException("Method is not implemented");
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>Ref</code> object in the
     * Java programming language.
     * 
     * @param colName
     *            the column name
     * @return a <code>Ref</code> object representing the SQL <code>REF</code>
     *         value in the specified column
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public Ref getRef(String colName) throws SQLException {
        return getRef(findColumn(colName));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>Ref</code> object in the
     * Java programming language.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @return a <code>Ref</code> object representing an SQL <code>REF</code>
     *         value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public Ref getRef(int columnIndex) throws SQLException {
        XData x = currentRow[columnIndex - 1];
        wasLastValueNull = x == null || x.isValueNull();
        return wasLastValueNull ? null : SQLTypeConverter.getRef(x.getObject());
    }

    /**
     * Retrieves the current row number. The first row is number 1, the second
     * number 2, and so on.
     * 
     * @return the current row number; <code>0</code> if there is no current
     *         row
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public int getRow() throws SQLException {
        return (currentRowIndex + 1);
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>short</code> in the Java
     * programming language.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>0</code>
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public short getShort(int columnIndex) throws SQLException {
        XData x = currentRow[columnIndex - 1];
        wasLastValueNull = x == null || x.isValueNull();
        return wasLastValueNull ? 0 : SQLTypeConverter.getShort(x.getObject());
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>short</code> in the Java
     * programming language.
     * 
     * @param columnName
     *            the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>0</code>
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public short getShort(String columnName) throws SQLException {
        return getShort(findColumn(columnName));
    }

    /**
     * Retrieves the <code>Statement</code> object that produced this
     * <code>ResultSet</code> object. If the result set was generated some
     * other way, such as by a <code>DatabaseMetaData</code> method, this
     * method returns <code>null</code>.
     * 
     * @return the <code>Statment</code> object that produced this
     *         <code>ResultSet</code> object or <code>null</code> if the
     *         result set was produced some other way
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public Statement getStatement() throws SQLException {
        return null;
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>String</code> in the Java
     * programming language.
     * 
     * @param columnName
     *            the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>null</code>
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public String getString(String columnName) throws SQLException {
        return getString(findColumn(columnName));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>String</code> in the Java
     * programming language.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>null</code>
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public String getString(int columnIndex) throws SQLException {
        XData x = currentRow[columnIndex - 1];
        wasLastValueNull = x == null || x.isValueNull();
        return wasLastValueNull ? null : SQLTypeConverter.getString(x
                .getObject());
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>java.sql.Time</code> object
     * in the Java programming language.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>null</code>
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public java.sql.Time getTime(int columnIndex) throws SQLException {
        return getTime(columnIndex, Calendar.getInstance());
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>java.sql.Time</code> object
     * in the Java programming language.
     * 
     * @param columnName
     *            the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>null</code>
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public java.sql.Time getTime(String columnName) throws SQLException {
        return getTime(findColumn(columnName), Calendar.getInstance());
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>java.sql.Time</code> object
     * in the Java programming language. This method uses the given calendar to
     * construct an appropriate millisecond value for the time if the underlying
     * database does not store timezone information.
     * 
     * @param columnName
     *            the SQL name of the column
     * @param cal
     *            the <code>java.util.Calendar</code> object to use in
     *            constructing the time
     * @return the column value as a <code>java.sql.Time</code> object; if the
     *         value is SQL <code>NULL</code>, the value returned is
     *         <code>null</code> in the Java programming language
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public java.sql.Time getTime(String columnName, Calendar cal)
            throws SQLException {
        return getTime(findColumn(columnName), cal);
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>java.sql.Time</code> object
     * in the Java programming language. This method uses the given calendar to
     * construct an appropriate millisecond value for the time if the underlying
     * database does not store timezone information.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @param cal
     *            the <code>java.util.Calendar</code> object to use in
     *            constructing the time
     * @return the column value as a <code>java.sql.Time</code> object; if the
     *         value is SQL <code>NULL</code>, the value returned is
     *         <code>null</code> in the Java programming language
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public java.sql.Time getTime(int columnIndex, Calendar cal)
            throws SQLException {
        XData x = currentRow[columnIndex - 1];
        wasLastValueNull = x == null || x.isValueNull();
        return wasLastValueNull ? null : SQLTypeConverter.getTime(
                x.getObject(), cal);
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>java.sql.Timestamp</code>
     * object.
     * 
     * @param columnName
     *            the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>null</code>
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public java.sql.Timestamp getTimestamp(String columnName)
            throws SQLException {
        return getTimestamp(findColumn(columnName), Calendar.getInstance());
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>java.sql.Timestamp</code>
     * object in the Java programming language.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>null</code>
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public java.sql.Timestamp getTimestamp(int columnIndex) throws SQLException {
        return getTimestamp(columnIndex, Calendar.getInstance());
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>java.sql.Timestamp</code>
     * object in the Java programming language. This method uses the given
     * calendar to construct an appropriate millisecond value for the timestamp
     * if the underlying database does not store timezone information.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @param cal
     *            the <code>java.util.Calendar</code> object to use in
     *            constructing the timestamp
     * @return the column value as a <code>java.sql.Timestamp</code> object;
     *         if the value is SQL <code>NULL</code>, the value returned is
     *         <code>null</code> in the Java programming language
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public java.sql.Timestamp getTimestamp(int columnIndex, Calendar cal)
            throws SQLException {
        XData x = currentRow[columnIndex - 1];
        wasLastValueNull = x == null || x.isValueNull();
        return wasLastValueNull ? null : SQLTypeConverter.getTimestamp(x
                .getObject(), cal);
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>java.sql.Timestamp</code>
     * object in the Java programming language. This method uses the given
     * calendar to construct an appropriate millisecond value for the timestamp
     * if the underlying database does not store timezone information.
     * 
     * @param columnName
     *            the SQL name of the column
     * @param cal
     *            the <code>java.util.Calendar</code> object to use in
     *            constructing the date
     * @return the column value as a <code>java.sql.Timestamp</code> object;
     *         if the value is SQL <code>NULL</code>, the value returned is
     *         <code>null</code> in the Java programming language
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public java.sql.Timestamp getTimestamp(String columnName, Calendar cal)
            throws SQLException {
        return getTimestamp(findColumn(columnName), cal);
    }

    /**
     * Retrieves the type of this <code>ResultSet</code> object. The type is
     * determined by the <code>Statement</code> object that created the result
     * set.
     * 
     * @return <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *         <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or
     *         <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public int getType() throws SQLException {
        return scrollType;
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>java.net.URL</code> object
     * in the Java programming language.
     * 
     * @param columnName
     *            the SQL name of the column
     * @return the column value as a <code>java.net.URL</code> object; if the
     *         value is SQL <code>NULL</code>, the value returned is
     *         <code>null</code> in the Java programming language
     * @exception SQLException
     *                if a database access error occurs or if a URL is malformed
     * @since 1.4
     * 
     */
    public java.net.URL getURL(String columnName) throws SQLException {
        throw new SQLException("getURL is not supported");
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>java.net.URL</code> object
     * in the Java programming language.
     * 
     * @param columnIndex
     *            the index of the column 1 is the first, 2 is the second,...
     * @return the column value as a <code>java.net.URL</code> object; if the
     *         value is SQL <code>NULL</code>, the value returned is
     *         <code>null</code> in the Java programming language
     * @exception SQLException
     *                if a database access error occurs, or if a URL is
     *                malformed
     * @since 1.4
     * 
     */
    public java.net.URL getURL(int columnIndex) throws SQLException {
        XData x = currentRow[columnIndex - 1];
        wasLastValueNull = x == null || x.isValueNull();
        return wasLastValueNull ? null : SQLTypeConverter.getURL(x.getObject());
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as as a stream of two-byte Unicode
     * characters. The first byte is the high byte; the second byte is the low
     * byte.
     * 
     * The value can then be read in chunks from the stream. This method is
     * particularly suitable for retrieving large <code>LONGVARCHAR</code>values.
     * The JDBC driver will do any necessary conversion from the database format
     * into Unicode.
     * 
     * <P>
     * <B>Note:</B> All the data in the returned stream must be read prior to
     * getting the value of any other column. The next call to a getter method
     * implicitly closes the stream. Also, a stream may return <code>0</code>
     * when the method <code>InputStream.available</code> is called, whether
     * there is data available or not.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @return a Java input stream that delivers the database column value as a
     *         stream of two-byte Unicode characters; if the value is SQL
     *         <code>NULL</code>, the value returned is <code>null</code>
     * 
     * @exception SQLException
     *                if a database access error occurs
     * @deprecated use <code>getCharacterStream</code> in place of
     *             <code>getUnicodeStream</code>
     * 
     */
    @Deprecated
    public java.io.InputStream getUnicodeStream(int columnIndex)
            throws SQLException {
        return getAsciiStream(columnIndex);
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a stream of two-byte Unicode
     * characters. The first byte is the high byte; the second byte is the low
     * byte.
     * 
     * The value can then be read in chunks from the stream. This method is
     * particularly suitable for retrieving large <code>LONGVARCHAR</code>
     * values. The JDBC technology-enabled driver will do any necessary
     * conversion from the database format into Unicode.
     * 
     * <P>
     * <B>Note:</B> All the data in the returned stream must be read prior to
     * getting the value of any other column. The next call to a getter method
     * implicitly closes the stream. Also, a stream may return <code>0</code>
     * when the method <code>InputStream.available</code> is called, whether
     * there is data available or not.
     * 
     * @param columnName
     *            the SQL name of the column
     * @return a Java input stream that delivers the database column value as a
     *         stream of two-byte Unicode characters. If the value is SQL
     *         <code>NULL</code>, the value returned is <code>null</code>.
     * @exception SQLException
     *                if a database access error occurs
     * @deprecated use <code>getCharacterStream</code> instead
     * 
     */
    @Deprecated
    public java.io.InputStream getUnicodeStream(String columnName)
            throws SQLException {
        return getUnicodeStream(findColumn(columnName));
    }

    /**
     * Retrieves the first warning reported by calls on this
     * <code>ResultSet</code> object. Subsequent warnings on this
     * <code>ResultSet</code> object will be chained to the
     * <code>SQLWarning</code> object that this method returns.
     * 
     * <P>
     * The warning chain is automatically cleared each time a new row is read.
     * This method may not be called on a <code>ResultSet</code> object that
     * has been closed; doing so will cause an <code>SQLException</code> to be
     * thrown.
     * <P>
     * <B>Note:</B> This warning chain only covers warnings caused by
     * <code>ResultSet</code> methods. Any warning caused by
     * <code>Statement</code> methods (such as reading OUT parameters) will be
     * chained on the <code>Statement</code> object.
     * 
     * @return the first <code>SQLWarning</code> object reported or
     *         <code>null</code> if there are none
     * @exception SQLException
     *                if a database access error occurs or this method is called
     *                on a closed result set
     * 
     */
    public SQLWarning getWarnings() throws SQLException {
        return sqlWarning;
    }

    /**
     * Inserts the contents of the insert row into this <code>ResultSet</code>
     * object and into the database. The cursor must be on the insert row when
     * this method is called.
     * 
     * @exception SQLException
     *                if a database access error occurs, if this method is
     *                called when the cursor is not on the insert row, or if not
     *                all of non-nullable columns in the insert row have been
     *                given a value
     * @since 1.2
     * 
     */
    public void insertRow() throws SQLException {
        throw new SQLException("todo - support?");
    }

    /**
     * Retrieves whether the cursor is after the last row in this
     * <code>ResultSet</code> object.
     * 
     * @return <code>true</code> if the cursor is after the last row;
     *         <code>false</code> if the cursor is at any other position or
     *         the result set contains no rows
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public boolean isAfterLast() throws SQLException {
        return currentRowIndex >= totalRows;
    }

    /**
     * Retrieves whether the cursor is before the first row in this
     * <code>ResultSet</code> object.
     * 
     * @return <code>true</code> if the cursor is before the first row;
     *         <code>false</code> if the cursor is at any other position or
     *         the result set contains no rows
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public boolean isBeforeFirst() throws SQLException {
        return currentRowIndex < 0;
    }

    /**
     * Retrieves whether the cursor is on the first row of this
     * <code>ResultSet</code> object.
     * 
     * @return <code>true</code> if the cursor is on the first row;
     *         <code>false</code> otherwise
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public boolean isFirst() throws SQLException {
        return currentRowIndex == 0;
    }

    /**
     * Retrieves whether the cursor is on the last row of this
     * <code>ResultSet</code> object. Note: Calling the method
     * <code>isLast</code> may be expensive because the JDBC driver might need
     * to fetch ahead one row in order to determine whether the current row is
     * the last row in the result set.
     * 
     * @return <code>true</code> if the cursor is on the last row;
     *         <code>false</code> otherwise
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public boolean isLast() throws SQLException {
        return (currentRowIndex + 1) == totalRows;
    }

    /**
     * Moves the cursor to the last row in this <code>ResultSet</code> object.
     * 
     * @return <code>true</code> if the cursor is on a valid row;
     *         <code>false</code> if there are no rows in the result set
     * @exception SQLException
     *                if a database access error occurs or the result set type
     *                is <code>TYPE_FORWARD_ONLY</code>
     * @since 1.2
     * 
     */
    public boolean last() throws SQLException {
        if (this.scrollType == ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLException(
                    "operation not permitted with scroll type FORWARD ONLY");
        }
        getAllPackets();
        currentRowIndex = totalRows - 1;
        resetCurrentRow();
        return true;
    }

    /**
     * Moves the cursor to the remembered cursor position, usually the current
     * row. This method has no effect if the cursor is not on the insert row.
     * 
     * @exception SQLException
     *                if a database access error occurs or the result set is not
     *                updatable
     * @since 1.2
     * 
     */
    public void moveToCurrentRow() throws SQLException {
        // do nothing
    }

    /**
     * Moves the cursor to the insert row. The current cursor position is
     * remembered while the cursor is positioned on the insert row.
     * 
     * The insert row is a special row associated with an updatable result set.
     * It is essentially a buffer where a new row may be constructed by calling
     * the updater methods prior to inserting the row into the result set.
     * 
     * Only the updater, getter, and <code>insertRow</code> methods may be
     * called when the cursor is on the insert row. All of the columns in a
     * result set must be given a value each time this method is called before
     * calling <code>insertRow</code>. An updater method must be called
     * before a getter method can be called on a column value.
     * 
     * @exception SQLException
     *                if a database access error occurs or the result set is not
     *                updatable
     * @since 1.2
     * 
     */
    public void moveToInsertRow() throws SQLException {
        // todo ?
        throw new SQLException("resultset is not updatable");
    }

    /**
     * Moves the cursor down one row from its current position. A
     * <code>ResultSet</code> cursor is initially positioned before the first
     * row; the first call to the method <code>next</code> makes the first row
     * the current row; the second call makes the second row the current row,
     * and so on.
     * 
     * <P>
     * If an input stream is open for the current row, a call to the method
     * <code>next</code> will implicitly close it. A <code>ResultSet</code>
     * object's warning chain is cleared when a new row is read.
     * 
     * @return <code>true</code> if the new current row is valid;
     *         <code>false</code> if there are no more rows
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public boolean next() throws SQLException {
        if (rows == null || totalRows == 0) {
            return false;
        }
        if ((currentRowIndex + 1) >= totalRows) {
            if (!hasMoreRowsToFetch) {
                // log.debug("already at last packet, done with next()");
                return false;
            }

            int lastTotal = totalRows;// to make bi-directional RS works
            // more packets to come?
            // the first time we should have already called resetRawRows(),
            // thus input stream has been exhausted - see this constructor for
            // the call
            setNextResultSet();
            resetRawRows();
            if (this.scrollType == ResultSet.TYPE_FORWARD_ONLY
                    && totalRows <= 0
                    || this.scrollType != ResultSet.TYPE_FORWARD_ONLY
                    && totalRows == lastTotal) {
                return false;
            }
        }
        currentRowIndex++;
        resetCurrentRow();
        return true;
    }

    /**
     * Moves the cursor to the previous row in this <code>ResultSet</code>
     * object.
     * 
     * @return <code>true</code> if the cursor is on a valid row;
     *         <code>false</code> if it is off the result set
     * @exception SQLException
     *                if a database access error occurs or the result set type
     *                is <code>TYPE_FORWARD_ONLY</code>
     * @since 1.2
     * 
     */
    public boolean previous() throws SQLException {
        if (this.scrollType == ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLException(
                    "operation not permitted with scroll type FORWARD ONLY",
                    null, ErrorCodes.METHOD_INVALID_OPERATION);
        }
        if (this.currentRowIndex < 1) {
            throw new SQLException(
                    "already at the first row - cannot move cursor to previous row",
                    null, ErrorCodes.METHOD_INVALID_OPERATION);
        }
        currentRowIndex--;
        resetCurrentRow();
        return true;
    }

    /**
     * Refreshes the current row with its most recent value in the database.
     * This method cannot be called when the cursor is on the insert row.
     * 
     * <P>
     * The <code>refreshRow</code> method provides a way for an application to
     * explicitly tell the JDBC driver to refetch a row(s) from the database. An
     * application may want to call <code>refreshRow</code> when caching or
     * prefetching is being done by the JDBC driver to fetch the latest value of
     * a row from the database. The JDBC driver may actually refresh multiple
     * rows at once if the fetch size is greater than one.
     * 
     * <P>
     * All values are refetched subject to the transaction isolation level and
     * cursor sensitivity. If <code>refreshRow</code> is called after calling
     * an updater method, but before calling the method <code>updateRow</code>,
     * then the updates made to the row are lost. Calling the method
     * <code>refreshRow</code> frequently will likely slow performance.
     * 
     * @exception SQLException
     *                if a database access error occurs or if this method is
     *                called when the cursor is on the insert row
     * @since 1.2
     * 
     */
    public void refreshRow() throws SQLException {
        // no effect
    }

    /**
     * Moves the cursor a relative number of rows, either positive or negative.
     * Attempting to move beyond the first/last row in the result set positions
     * the cursor before/after the the first/last row. Calling
     * <code>relative(0)</code> is valid, but does not change the cursor
     * position.
     * 
     * <p>
     * Note: Calling the method <code>relative(1)</code> is identical to
     * calling the method <code>next()</code> and calling the method
     * <code>relative(-1)</code> is identical to calling the method
     * <code>previous()</code>.
     * 
     * @param rows
     *            an <code>int</code> specifying the number of rows to move
     *            from the current row; a positive number moves the cursor
     *            forward; a negative number moves the cursor backward
     * @return <code>true</code> if the cursor is on a row; <code>false</code>
     *         otherwise
     * @exception SQLException
     *                if a database access error occurs, there is no current
     *                row, or the result set type is
     *                <code>TYPE_FORWARD_ONLY</code>
     * @since 1.2
     * 
     */
    public boolean relative(int rows) throws SQLException {
        if (this.scrollType == ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLException(
                    "relative() not supported with scroll type FORWARD ONLY");
        }
        if (rows == 0) {
            return true;
        }
        currentRowIndex += rows;
        if (rows > 0) {
            currentRowIndex = currentRowIndex >= totalRows ? totalRows - 1
                    : currentRowIndex;
        } else {
            currentRowIndex = currentRowIndex < 0 ? 0 : currentRowIndex;
        }
        resetCurrentRow();

        return true;
    }

    /**
     * Retrieves whether a row has been deleted. A deleted row may leave a
     * visible "hole" in a result set. This method can be used to detect holes
     * in a result set. The value returned depends on whether or not this
     * <code>ResultSet</code> object can detect deletions.
     * 
     * @return <code>true</code> if a row was deleted and deletions are
     *         detected; <code>false</code> otherwise
     * @exception SQLException
     *                if a database access error occurs
     * 
     * @see DatabaseMetaData#deletesAreDetected
     * @since 1.2
     * 
     */
    public boolean rowDeleted() throws SQLException {
        return false;// todo
    }

    /**
     * Retrieves whether the current row has had an insertion. The value
     * returned depends on whether or not this <code>ResultSet</code> object
     * can detect visible inserts.
     * 
     * @return <code>true</code> if a row has had an insertion and insertions
     *         are detected; <code>false</code> otherwise
     * @exception SQLException
     *                if a database access error occurs
     * 
     * @see DatabaseMetaData#insertsAreDetected
     * @since 1.2
     * 
     */
    public boolean rowInserted() throws SQLException {
        return false;// todo
    }

    /**
     * Retrieves whether the current row has been updated. The value returned
     * depends on whether or not the result set can detect updates.
     * 
     * @return <code>true</code> if both (1) the row has been visibly updated
     *         by the owner or another and (2) updates are detected
     * @exception SQLException
     *                if a database access error occurs
     * @see DatabaseMetaData#updatesAreDetected
     * @since 1.2
     * 
     */
    public boolean rowUpdated() throws SQLException {
        return false;// todo
    }

    /**
     * Gives a hint as to the direction in which the rows in this
     * <code>ResultSet</code> object will be processed. The initial value is
     * determined by the <code>Statement</code> object that produced this
     * <code>ResultSet</code> object. The fetch direction may be changed at
     * any time.
     * 
     * @param direction
     *            an <code>int</code> specifying the suggested fetch
     *            direction; one of <code>ResultSet.FETCH_FORWARD</code>,
     *            <code>ResultSet.FETCH_REVERSE</code>, or
     *            <code>ResultSet.FETCH_UNKNOWN</code>
     * @exception SQLException
     *                if a database access error occurs or the result set type
     *                is <code>TYPE_FORWARD_ONLY</code> and the fetch
     *                direction is not <code>FETCH_FORWARD</code>
     * @since 1.2
     * @see Statement#setFetchDirection
     * @see #getFetchDirection
     * 
     */
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != ResultSet.FETCH_FORWARD) {
            throw new SQLException("fetch direction not supported", null,
                    ErrorCodes.METHOD_NOT_SUPPORTED);
        }
        this.fetchDirection = direction;
    }

    /**
     * Gives the JDBC driver a hint as to the number of rows that should be
     * fetched from the database when more rows are needed for this
     * <code>ResultSet</code> object. If the fetch size specified is zero, the
     * JDBC driver ignores the value and is free to make its own best guess as
     * to what the fetch size should be. The default value is set by the
     * <code>Statement</code> object that created the result set. The fetch
     * size may be changed at any time.
     * 
     * @param rows
     *            the number of rows to fetch
     * @exception SQLException
     *                if a database access error occurs or the condition
     *                <code>0 <= rows <= this.getMaxRows()</code> is not
     *                satisfied
     * @since 1.2
     * @see #getFetchSize
     * 
     */
    public void setFetchSize(int rows) throws SQLException {
        if (rows <= 0) {
            throw new SQLException("Fetch size must not be negative", null,
                    ErrorCodes.METHOD_INVALID_OPERATION);
        }
        this.fetchSize = rows;
    }

    /**
     * Updates the designated column with a <code>java.sql.Array</code> value.
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnName
     *            the name of the column
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.4
     * 
     */
    public void updateArray(String columnName, java.sql.Array x)
            throws SQLException {
        updateArray(findColumn(columnName), x);
    }

    /**
     * Updates the designated column with a <code>java.sql.Array</code> value.
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.4
     * 
     */
    public void updateArray(int columnIndex, java.sql.Array x)
            throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with an ascii stream value. The updater
     * methods are used to update column values in the current row or the insert
     * row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @param length
     *            the length of the stream
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateAsciiStream(int columnIndex, java.io.InputStream x,
            int length) throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with an ascii stream value. The updater
     * methods are used to update column values in the current row or the insert
     * row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     * 
     * @param columnName
     *            the name of the column
     * @param x
     *            the new column value
     * @param length
     *            the length of the stream
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateAsciiStream(String columnName, java.io.InputStream x,
            int length) throws SQLException {
        updateAsciiStream(findColumn(columnName), x, length);
    }

    /**
     * Updates the designated column with a <code>java.math.BigDecimal</code>
     * value. The updater methods are used to update column values in the
     * current row or the insert row. The updater methods do not update the
     * underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateBigDecimal(int columnIndex, BigDecimal x)
            throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with a <code>java.sql.BigDecimal</code>
     * value. The updater methods are used to update column values in the
     * current row or the insert row. The updater methods do not update the
     * underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     * 
     * @param columnName
     *            the name of the column
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateBigDecimal(String columnName, BigDecimal x)
            throws SQLException {
        updateBigDecimal(findColumn(columnName), x);
    }

    /**
     * Updates the designated column with a binary stream value. The updater
     * methods are used to update column values in the current row or the insert
     * row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     * 
     * @param columnName
     *            the name of the column
     * @param x
     *            the new column value
     * @param length
     *            the length of the stream
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateBinaryStream(String columnName, java.io.InputStream x,
            int length) throws SQLException {
        updateBinaryStream(findColumn(columnName), x, length);
    }

    /**
     * Updates the designated column with a binary stream value. The updater
     * methods are used to update column values in the current row or the insert
     * row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @param length
     *            the length of the stream
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateBinaryStream(int columnIndex, java.io.InputStream x,
            int length) throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with a <code>java.sql.Blob</code> value.
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnName
     *            the name of the column
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.4
     * 
     */
    public void updateBlob(String columnName, java.sql.Blob x)
            throws SQLException {
        updateBlob(findColumn(columnName), x);
    }

    /**
     * Updates the designated column with a <code>java.sql.Blob</code> value.
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.4
     * 
     */
    public void updateBlob(int columnIndex, java.sql.Blob x)
            throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with a <code>boolean</code> value. The
     * updater methods are used to update column values in the current row or
     * the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnName
     *            the name of the column
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateBoolean(String columnName, boolean x) throws SQLException {
        updateBoolean(findColumn(columnName), x);
    }

    /**
     * Updates the designated column with a <code>boolean</code> value. The
     * updater methods are used to update column values in the current row or
     * the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with a <code>byte</code> value. The
     * updater methods are used to update column values in the current row or
     * the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with a <code>byte</code> value. The
     * updater methods are used to update column values in the current row or
     * the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnName
     *            the name of the column
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateByte(String columnName, byte x) throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with a <code>byte</code> array value. The
     * updater methods are used to update column values in the current row or
     * the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with a byte array value.
     * 
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnName
     *            the name of the column
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateBytes(String columnName, byte[] x) throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with a character stream value. The updater
     * methods are used to update column values in the current row or the insert
     * row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @param length
     *            the length of the stream
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateCharacterStream(int columnIndex, java.io.Reader x,
            int length) throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with a character stream value. The updater
     * methods are used to update column values in the current row or the insert
     * row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     * 
     * @param columnName
     *            the name of the column
     * @param reader
     *            the <code>java.io.Reader</code> object containing the new
     *            column value
     * @param length
     *            the length of the stream
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateCharacterStream(String columnName, java.io.Reader reader,
            int length) throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with a <code>java.sql.Clob</code> value.
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.4
     * 
     */
    public void updateClob(int columnIndex, java.sql.Clob x)
            throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with a <code>java.sql.Clob</code> value.
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnName
     *            the name of the column
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.4
     * 
     */
    public void updateClob(String columnName, java.sql.Clob x)
            throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with a <code>java.sql.Date</code> value.
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateDate(int columnIndex, java.sql.Date x)
            throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with a <code>java.sql.Date</code> value.
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnName
     *            the name of the column
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateDate(String columnName, java.sql.Date x)
            throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with a <code>double</code> value. The
     * updater methods are used to update column values in the current row or
     * the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnName
     *            the name of the column
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateDouble(String columnName, double x) throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with a <code>double</code> value. The
     * updater methods are used to update column values in the current row or
     * the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with a <code>float	</code> value. The
     * updater methods are used to update column values in the current row or
     * the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnName
     *            the name of the column
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateFloat(String columnName, float x) throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with a <code>float</code> value. The
     * updater methods are used to update column values in the current row or
     * the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with an <code>int</code> value. The
     * updater methods are used to update column values in the current row or
     * the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with an <code>int</code> value. The
     * updater methods are used to update column values in the current row or
     * the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnName
     *            the name of the column
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateInt(String columnName, int x) throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with a <code>long</code> value. The
     * updater methods are used to update column values in the current row or
     * the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnName
     *            the name of the column
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateLong(String columnName, long x) throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with a <code>long</code> value. The
     * updater methods are used to update column values in the current row or
     * the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with a <code>null</code> value. The
     * updater methods are used to update column values in the current row or
     * the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnName
     *            the name of the column
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateNull(String columnName) throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Gives a nullable column a null value.
     * 
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with an <code>Object</code> value. The
     * updater methods are used to update column values in the current row or
     * the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with an <code>Object</code> value. The
     * updater methods are used to update column values in the current row or
     * the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnName
     *            the name of the column
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateObject(String columnName, Object x) throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with an <code>Object</code> value. The
     * updater methods are used to update column values in the current row or
     * the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnName
     *            the name of the column
     * @param x
     *            the new column value
     * @param scale
     *            for <code>java.sql.Types.DECIMAL</code> or
     *            <code>java.sql.Types.NUMERIC</code> types, this is the
     *            number of digits after the decimal point. For all other types
     *            this value will be ignored.
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateObject(String columnName, Object x, int scale)
            throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with an <code>Object</code> value. The
     * updater methods are used to update column values in the current row or
     * the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @param scale
     *            for <code>java.sql.Types.DECIMA</code> or
     *            <code>java.sql.Types.NUMERIC</code> types, this is the
     *            number of digits after the decimal point. For all other types
     *            this value will be ignored.
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateObject(int columnIndex, Object x, int scale)
            throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with a <code>java.sql.Ref</code> value.
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnName
     *            the name of the column
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.4
     * 
     */
    public void updateRef(String columnName, java.sql.Ref x)
            throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with a <code>java.sql.Ref</code> value.
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.4
     * 
     */
    public void updateRef(int columnIndex, java.sql.Ref x) throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the underlying database with the new contents of the current row
     * of this <code>ResultSet</code> object. This method cannot be called
     * when the cursor is on the insert row.
     * 
     * @exception SQLException
     *                if a database access error occurs or if this method is
     *                called when the cursor is on the insert row
     * @since 1.2
     * 
     */
    public void updateRow() throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with a <code>short</code> value. The
     * updater methods are used to update column values in the current row or
     * the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnName
     *            the name of the column
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateShort(String columnName, short x) throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with a <code>short</code> value. The
     * updater methods are used to update column values in the current row or
     * the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with a <code>String</code> value. The
     * updater methods are used to update column values in the current row or
     * the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnName
     *            the name of the column
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateString(String columnName, String x) throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with a <code>String</code> value. The
     * updater methods are used to update column values in the current row or
     * the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with a <code>java.sql.Time</code> value.
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateTime(int columnIndex, java.sql.Time x)
            throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with a <code>java.sql.Time</code> value.
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     * 
     * @param columnName
     *            the name of the column
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateTime(String columnName, java.sql.Time x)
            throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code>
     * value. The updater methods are used to update column values in the
     * current row or the insert row. The updater methods do not update the
     * underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     * 
     * @param columnName
     *            the name of the column
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateTimestamp(String columnName, java.sql.Timestamp x)
            throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code>
     * value. The updater methods are used to update column values in the
     * current row or the insert row. The updater methods do not update the
     * underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public void updateTimestamp(int columnIndex, java.sql.Timestamp x)
            throws SQLException {
        throw new SQLException("method not supported");
    }

    /**
     * Reports whether the last column read had a value of SQL <code>NULL</code>.
     * Note that you must first call one of the getter methods on a column to
     * try to read its value and then call the method <code>wasNull</code> to
     * see if the value read was SQL <code>NULL</code>.
     * 
     * @return <code>true</code> if the last column value read was SQL
     *         <code>NULL</code> and <code>false</code> otherwise
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public boolean wasNull() throws SQLException {
        return this.wasLastValueNull;
    }

    // newArrayIdx starts from 0
    /**
     * 
     * @param newArrayIdx 
     * @throws java.sql.SQLException 
     */
    private void resetCurrentRow(int newArrayIdx) throws SQLException {
        currentRowIndex = newArrayIdx;
        resetCurrentRow();
    }

    // set the cursor to currentRowIndex
    /**
     * 
     * @throws java.sql.SQLException 
     */
    private void resetCurrentRow() throws SQLException {
        if (rows == null) {
            throw new SQLException("There are no rows to reset");
        }
        this.currentRow = rows.get(currentRowIndex);
        clearWarnings();
    }

    /**
     * 
     * @throws java.sql.SQLException 
     */
    protected void resetRawRows() throws SQLException {
        try {
            responseMessage.resetRows();
            this.hasMoreRowsToFetch = !responseMessage.isLastPacket();

            if (this.scrollType == ResultSet.TYPE_FORWARD_ONLY) {
                this.rows = null;
                this.totalRows = 0;
                this.currentRowIndex = -1;// array/list starts at 0

                this.rows = responseMessage.getRows();
                if (rows != null) {
                    this.totalRows = rows.size();
                }
            } else if (responseMessage.getRows() != null) {
                if (rows == null) {
                    rows = new ArrayList<XData[]>();
                }
                this.rows.addAll(responseMessage.getRows());
                this.totalRows = rows.size();
            }
        } catch (Throwable t) {
            throw new SQLException("Error reading next rows - "
                    + t.getMessage());
        }
    }

    /**
     * 
     * @throws java.sql.SQLException 
     */
    private void getAllPackets() throws SQLException {
        int lastTotal = 0;
        while (hasMoreRowsToFetch) {
            lastTotal = totalRows;
            resetRawRows();
            if (totalRows == lastTotal) {
                logger.debug("setting next resultset for getAllPackets");
                setNextResultSet();
            }
        }
    }

    // get the next set of packets for the query
    /**
     * 
     * @throws java.sql.SQLException 
     */
    protected void setNextResultSet() throws SQLException {
        hasMoreRowsToFetch = false;
    }

    /**
     * 
     * @throws java.sql.SQLException 
     * @return 
     */
    public int getHoldability() throws SQLException {
        // TODO Added for 1.6 compatibility
        return 0;
    }

    /**
     * 
     * @param columnIndex 
     * @throws java.sql.SQLException 
     * @return 
     */
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        // TODO Added for 1.6 compatibility
        return null;
    }

    /**
     * 
     * @param columnLabel 
     * @throws java.sql.SQLException 
     * @return 
     */
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        // TODO Added for 1.6 compatibility
        return null;
    }

    // public NClob getNClob(int columnIndex) throws SQLException
    // {
    // // TODO Added for 1.6 compatibility
    // return null;
    // }
    //
    // public NClob getNClob(String columnLabel) throws SQLException
    // {
    // // TODO Added for 1.6 compatibility
    // return null;
    // }

    /**
     * 
     * @param columnIndex 
     * @throws java.sql.SQLException 
     * @return 
     */
    public String getNString(int columnIndex) throws SQLException {
        // TODO Added for 1.6 compatibility
        return null;
    }

    /**
     * 
     * @param columnLabel 
     * @throws java.sql.SQLException 
     * @return 
     */
    public String getNString(String columnLabel) throws SQLException {
        // TODO Added for 1.6 compatibility
        return null;
    }

    // public RowId getRowId(int columnIndex) throws SQLException
    // {
    // // TODO Added for 1.6 compatibility
    // return null;
    // }
    //
    // public RowId getRowId(String columnLabel) throws SQLException
    // {
    // // TODO Added for 1.6 compatibility
    // return null;
    // }
    //
    // public SQLXML getSQLXML(int columnIndex) throws SQLException
    // {
    // // TODO Added for 1.6 compatibility
    // return null;
    // }
    //
    // public SQLXML getSQLXML(String columnLabel) throws SQLException
    // {
    // // TODO Added for 1.6 compatibility
    // return null;
    // }

    /**
     * 
     * @throws java.sql.SQLException 
     * @return 
     */
    public boolean isClosed() throws SQLException {
        // TODO Added for 1.6 compatibility
        return false;
    }

    /**
     * 
     * @param columnIndex 
     * @param x 
     * @throws java.sql.SQLException 
     */
    public void updateAsciiStream(int columnIndex, InputStream x)
            throws SQLException {
        // TODO Added for 1.6 compatibility
    }

    /**
     * 
     * @param columnLabel 
     * @param x 
     * @throws java.sql.SQLException 
     */
    public void updateAsciiStream(String columnLabel, InputStream x)
            throws SQLException {
        // TODO Added for 1.6 compatibility
    }

    /**
     * 
     * @param columnIndex 
     * @param x 
     * @param length 
     * @throws java.sql.SQLException 
     */
    public void updateAsciiStream(int columnIndex, InputStream x, long length)
            throws SQLException {
        // TODO Added for 1.6 compatibility
    }

    /**
     * 
     * @param columnLabel 
     * @param x 
     * @param length 
     * @throws java.sql.SQLException 
     */
    public void updateAsciiStream(String columnLabel, InputStream x, long length)
            throws SQLException {
        // TODO Added for 1.6 compatibility
    }

    /**
     * 
     * @param columnIndex 
     * @param x 
     * @throws java.sql.SQLException 
     */
    public void updateBinaryStream(int columnIndex, InputStream x)
            throws SQLException {
        // TODO Added for 1.6 compatibility
    }

    /**
     * 
     * @param columnLabel 
     * @param x 
     * @throws java.sql.SQLException 
     */
    public void updateBinaryStream(String columnLabel, InputStream x)
            throws SQLException {
        // TODO Added for 1.6 compatibility
    }

    /**
     * 
     * @param columnIndex 
     * @param x 
     * @param length 
     * @throws java.sql.SQLException 
     */
    public void updateBinaryStream(int columnIndex, InputStream x, long length)
            throws SQLException {
        // TODO Added for 1.6 compatibility
    }

    /**
     * 
     * @param columnLabel 
     * @param x 
     * @param length 
     * @throws java.sql.SQLException 
     */
    public void updateBinaryStream(String columnLabel, InputStream x,
            long length) throws SQLException {
        // TODO Added for 1.6 compatibility
    }

    /**
     * 
     * @param columnIndex 
     * @param inputStream 
     * @throws java.sql.SQLException 
     */
    public void updateBlob(int columnIndex, InputStream inputStream)
            throws SQLException {
        // TODO Added for 1.6 compatibility
    }

    /**
     * 
     * @param columnLabel 
     * @param inputStream 
     * @throws java.sql.SQLException 
     */
    public void updateBlob(String columnLabel, InputStream inputStream)
            throws SQLException {
        // TODO Added for 1.6 compatibility
    }

    /**
     * 
     * @param columnIndex 
     * @param inputStream 
     * @param length 
     * @throws java.sql.SQLException 
     */
    public void updateBlob(int columnIndex, InputStream inputStream, long length)
            throws SQLException {
        // TODO Added for 1.6 compatibility
    }

    /**
     * 
     * @param columnLabel 
     * @param inputStream 
     * @param length 
     * @throws java.sql.SQLException 
     */
    public void updateBlob(String columnLabel, InputStream inputStream,
            long length) throws SQLException {
        // TODO Added for 1.6 compatibility
    }

    /**
     * 
     * @param columnIndex 
     * @param x 
     * @throws java.sql.SQLException 
     */
    public void updateCharacterStream(int columnIndex, Reader x)
            throws SQLException {
        // TODO Added for 1.6 compatibility
    }

    /**
     * 
     * @param columnLabel 
     * @param reader 
     * @throws java.sql.SQLException 
     */
    public void updateCharacterStream(String columnLabel, Reader reader)
            throws SQLException {
        // TODO Added for 1.6 compatibility
    }

    /**
     * 
     * @param columnIndex 
     * @param x 
     * @param length 
     * @throws java.sql.SQLException 
     */
    public void updateCharacterStream(int columnIndex, Reader x, long length)
            throws SQLException {
        // TODO Added for 1.6 compatibility
    }

    /**
     * 
     * @param columnLabel 
     * @param reader 
     * @param length 
     * @throws java.sql.SQLException 
     */
    public void updateCharacterStream(String columnLabel, Reader reader,
            long length) throws SQLException {
        // TODO Added for 1.6 compatibility
    }

    /**
     * 
     * @param columnIndex 
     * @param reader 
     * @throws java.sql.SQLException 
     */
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        // TODO Added for 1.6 compatibility
    }

    /**
     * 
     * @param columnLabel 
     * @param reader 
     * @throws java.sql.SQLException 
     */
    public void updateClob(String columnLabel, Reader reader)
            throws SQLException {
        // TODO Added for 1.6 compatibility
    }

    /**
     * 
     * @param columnIndex 
     * @param reader 
     * @param length 
     * @throws java.sql.SQLException 
     */
    public void updateClob(int columnIndex, Reader reader, long length)
            throws SQLException {
        // TODO Added for 1.6 compatibility
    }

    /**
     * 
     * @param columnLabel 
     * @param reader 
     * @param length 
     * @throws java.sql.SQLException 
     */
    public void updateClob(String columnLabel, Reader reader, long length)
            throws SQLException {
        // TODO Added for 1.6 compatibility
    }

    /**
     * 
     * @param columnIndex 
     * @param x 
     * @throws java.sql.SQLException 
     */
    public void updateNCharacterStream(int columnIndex, Reader x)
            throws SQLException {
        // TODO Added for 1.6 compatibility
    }

    /**
     * 
     * @param columnLabel 
     * @param reader 
     * @throws java.sql.SQLException 
     */
    public void updateNCharacterStream(String columnLabel, Reader reader)
            throws SQLException {
        // TODO Added for 1.6 compatibility
    }

    /**
     * 
     * @param columnIndex 
     * @param x 
     * @param length 
     * @throws java.sql.SQLException 
     */
    public void updateNCharacterStream(int columnIndex, Reader x, long length)
            throws SQLException {
        // TODO Added for 1.6 compatibility
    }

    /**
     * 
     * @param columnLabel 
     * @param reader 
     * @param length 
     * @throws java.sql.SQLException 
     */
    public void updateNCharacterStream(String columnLabel, Reader reader,
            long length) throws SQLException {
        // TODO Added for 1.6 compatibility
    }

    // public void updateNClob(int columnIndex, NClob nClob) throws SQLException
    // {
    // // TODO Added for 1.6 compatibility
    // }
    //
    // public void updateNClob(String columnLabel, NClob nClob) throws
    // SQLException
    // {
    // // TODO Added for 1.6 compatibility
    // }

    /**
     * 
     * @param columnIndex 
     * @param reader 
     * @throws java.sql.SQLException 
     */
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    /**
     * 
     * @param columnLabel 
     * @param reader 
     * @throws java.sql.SQLException 
     */
    public void updateNClob(String columnLabel, Reader reader)
            throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    /**
     * 
     * @param columnIndex 
     * @param reader 
     * @param length 
     * @throws java.sql.SQLException 
     */
    public void updateNClob(int columnIndex, Reader reader, long length)
            throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    /**
     * 
     * @param columnLabel 
     * @param reader 
     * @param length 
     * @throws java.sql.SQLException 
     */
    public void updateNClob(String columnLabel, Reader reader, long length)
            throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    /**
     * 
     * @param columnIndex 
     * @param nString 
     * @throws java.sql.SQLException 
     */
    public void updateNString(int columnIndex, String nString)
            throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    /**
     * 
     * @param columnLabel 
     * @param nString 
     * @throws java.sql.SQLException 
     */
    public void updateNString(String columnLabel, String nString)
            throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    // public void updateRowId(int columnIndex, RowId x) throws SQLException
    // {
    // // TODO Added for 1.6 compatibility
    //        
    // }
    //
    // public void updateRowId(String columnLabel, RowId x) throws SQLException
    // {
    // // TODO Added for 1.6 compatibility
    //        
    // }
    //
    // public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws
    // SQLException
    // {
    // // TODO Added for 1.6 compatibility
    //        
    // }
    //
    // public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws
    // SQLException
    // {
    // // TODO Added for 1.6 compatibility
    //        
    // }

    /**
     * 
     * @return 
     * @param iface 
     * @throws java.sql.SQLException 
     */
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        // TODO Added for 1.6 compatibility
        return false;
    }

    /**
     * 
     * @return 
     * @param iface 
     * @throws java.sql.SQLException 
     */
    public <T> T unwrap(Class<T> iface) throws SQLException {
        // TODO Added for 1.6 compatibility
        return null;
    }

	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateNClob(String columnLabel, NClob nClob)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public NClob getNClob(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}
}
