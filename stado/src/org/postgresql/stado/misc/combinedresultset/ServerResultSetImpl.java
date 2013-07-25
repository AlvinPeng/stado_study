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
package org.postgresql.stado.misc.combinedresultset;

import java.io.InputStream;
import java.io.Reader;
import java.sql.*;
import java.util.*;

import org.postgresql.stado.common.XDBResultSetMetaData;
import org.postgresql.stado.common.util.SQLTypeConverter;


/**
 * 
 */
public class ServerResultSetImpl implements java.sql.ResultSet, ServerResultSet {
    // private static final XLogger logger =
    // XLogger.getLogger(ServerResultSetImpl.class);
    /**
     * How to order rows
     */
    private List<SortCriteria> sortedOrderList;

    /**
     * Result set containing current record
     */
    private ResultSetPosManager currentResultSetManager;

    /**
     * Result Set metadata implementation
     */
    private XDBResultSetMetaData rsImpl;

    /**
     * List of ResultSetPosManagers
     */
    private List<ResultSetPosManager> resultSetManagerList = new ArrayList<ResultSetPosManager>();

    /**
     * Here ResultSetPosManagers are sorted
     */
    private TreeSet<ResultSetPosManager> aTreeSet;

    private LinkedList<ResultSetPosManager> aLinkedList;

    private Collection<String> finalCoordTempTableList;

    private Collection<String> finalNodeTempTableList;

    private long limit = -1;

    /** track how many rows we are doing */
    private long currentRow = 0;

    public ResultSetMetaData getMetaData() {
        return rsImpl;
    }

    public ServerResultSetImpl(Collection<? extends ResultSet> resultSetList,
            List<SortCriteria> sortedOrderList, boolean isDistinct, long limit,
            long offset, XDBResultSetMetaData metaData) throws SQLException {
        ArrayList<SortCriteria> hiddenColumnList = new ArrayList<SortCriteria>();

        this.limit = limit;

        // Check if the resultSetList is not NULL
        if (resultSetList == null) {
            throw new NullPointerException("The resultset list is null");
        }
        // Check if the sortedOrderList is not NULL
        if (sortedOrderList == null) {
            this.sortedOrderList = new ArrayList<SortCriteria>();
        } else {
            this.sortedOrderList = sortedOrderList;

            // check for hidden columns
            for (SortCriteria aSortItem : sortedOrderList) {
                if (!aSortItem.isIncludeInResult()) {
                    hiddenColumnList.add(aSortItem);
                }
            }
        }

        // distinctModifier force ResultSet is NOT equal to other ResultSet
        // if the server resultset is not distinct
        int distinctModifier = 0;
        for (ResultSet rs : resultSetList) {
            ResultSetPosManager resultSetPosMan = new ResultSetPosManager(rs,
                    this.sortedOrderList, distinctModifier);
            resultSetManagerList.add(resultSetPosMan);
            if (!isDistinct) {
                distinctModifier++;
            }
        }

        if (this.sortedOrderList == null || this.sortedOrderList.isEmpty()) {
            aLinkedList = new LinkedList<ResultSetPosManager>();
            // Init linked list
            for (ResultSetPosManager rsPosMan : resultSetManagerList) {
                if (rsPosMan.next()) {
                    aLinkedList.addFirst(rsPosMan);
                }
            }
        } else {
            aTreeSet = new TreeSet<ResultSetPosManager>();
            // Init tree set
            for (ResultSetPosManager rsPosMan : resultSetManagerList) {
                boolean wasAdded = false;
                while (!wasAdded) {
                    if (rsPosMan.next()) {
                        // We have next record, try to sort it
                        // if add() returned false it means we have duplicate
                        // record
                        // and should skip it (do next loop)
                        wasAdded = aTreeSet.add(rsPosMan);
                    } else {
                        // Result set is exhausted, forget about it
                        break;
                    }
                }
            }
        }

        // See if offset is set, then we need to skip some rows.
        while (offset > 0) {
            do_next();
            offset--;
        }

        rsImpl = metaData;
    }

    public boolean next() throws SQLException {

        if (currentRow == limit) {
            // We reached the maximum limit, just clean up
            close();
            currentResultSetManager = null;
            aTreeSet = null;
            aLinkedList = null;
            return false;
        }

        currentRow++;

        return do_next();
    }

    /**
     * main next() functionality, broken out because of OFFSET
     */
    private boolean do_next() {
        // Advance currentResultSetManager to new record and add it to
        // the TreeSet to have it sorted
        // Note; currentResultSetManager == null when next() called first time
        if (currentResultSetManager != null) {
            if (aTreeSet != null) {
                boolean wasAdded = false;
                while (!wasAdded) {
                    if (currentResultSetManager.next()) {
                        // We have next record, try to sort it
                        // if add() returned false it means we have duplicate
                        // record
                        // and should skip it (do next loop)
                        wasAdded = aTreeSet.add(currentResultSetManager);
                    } else {
                        // Result set is exhausted, forget about it
                        break;
                    }
                }
            } else if (aLinkedList != null) {
                if (currentResultSetManager.next()) {
                    aLinkedList.addFirst(currentResultSetManager);
                }
            }
        }

        // Chhosing next currentResultSetManager
        if (aTreeSet != null) {
            if (aTreeSet.isEmpty()) {
                // No result set left. Reporting end of the Result set
                currentResultSetManager = null;
            } else {
                currentResultSetManager = aTreeSet.last();
                aTreeSet.remove(currentResultSetManager);
            }
        } else if (aLinkedList != null) {
            if (aLinkedList.isEmpty()) {
                // No result set left. Reporting end of the Result set
                currentResultSetManager = null;
            } else {
                currentResultSetManager = aLinkedList.removeLast();
            }
        }
        return currentResultSetManager != null;

    }

    public byte[] getBytes(int position) throws SQLException {
        return SQLTypeConverter.getBytes(currentResultSetManager
                .getObject(position));
    }

    public String getString(int position) throws SQLException {
        return SQLTypeConverter.getString(currentResultSetManager
                .getObject(position));
    }

    public int getInt(int position) throws SQLException {
        return SQLTypeConverter.getInt(currentResultSetManager
                .getObject(position));
    }

    public int getInt(String columnName) throws SQLException {
        return getInt(findColumn(columnName));
    }

    public long getLong(int position) throws SQLException {
        return SQLTypeConverter.getLong(currentResultSetManager
                .getObject(position));
    }

    public long getLong(String columnName) throws SQLException {
        return getLong(findColumn(columnName));
    }

    /**
     * This is for cleaning up temp tables when we are done with the ResultSet
     */
    public void setFinalCoordTempTableList(Collection<String> tempTableList) {
        finalCoordTempTableList = tempTableList;
    }

    public Collection<String> getFinalCoordTempTableList() {
        return finalCoordTempTableList;
    }

    /**
     * This is for cleaning up temp tables when we are done with the ResultSet
     */
    public void setFinalNodeTempTableList(Collection<String> tempTableList) {
        finalNodeTempTableList = tempTableList;
    }

    public Collection<String> getFinalNodeTempTableList() {
        return finalNodeTempTableList;
    }

    public void addToFinalNodeTempTableList(Collection<String> tempList) {
        if (finalNodeTempTableList == null) {
            finalNodeTempTableList = new ArrayList<String>(tempList);
        } else {
            finalNodeTempTableList.addAll(tempList);
        }
    }

    /**
     * This will close the resulset
     */
    public void close() throws SQLException {
        for (ResultSetPosManager rsPosMan : resultSetManagerList) {
            rsPosMan.close();
        }
    }

    public void deleteRow() throws SQLException {
        throw new SQLException("Operation is not supported");
    }

    public int findColumn(String strColumn) throws SQLException {
        int colCount = rsImpl.getColumnCount();
        for (int i = 1; i <= colCount; i++) {
            if (strColumn.equalsIgnoreCase(rsImpl.getColumnName(i))) {
                return i;
            }
        }
        throw new SQLException("Column name is not found: " + strColumn);
    }

    public boolean absolute(int row) throws SQLException {
        throw new SQLException("method not supported");
    }

    public void afterLast() throws SQLException {
        throw new SQLException("method not supported");
    }

    public void beforeFirst() throws SQLException {
        throw new SQLException("method not supported");
    }

    public void cancelRowUpdates() throws SQLException {
        throw new SQLException("method not supported");
    }

    public void clearWarnings() throws SQLException {
        throw new SQLException("method not supported");
    }

    public boolean first() throws SQLException {
        throw new SQLException("method not supported");
    }

    public java.sql.Array getArray(int position) throws SQLException {
        return SQLTypeConverter.getArray(currentResultSetManager
                .getObject(position));
    }

    public java.sql.Array getArray(String columnName) throws SQLException {
        return getArray(findColumn(columnName));
    }

    public java.io.InputStream getAsciiStream(String columnName)
            throws SQLException {
        return getAsciiStream(findColumn(columnName));
    }

    public java.io.InputStream getAsciiStream(int position) throws SQLException {
        return SQLTypeConverter.getAsciiStream(currentResultSetManager
                .getObject(position));
    }

    public java.math.BigDecimal getBigDecimal(String columnName)
            throws SQLException {
        return getBigDecimal(findColumn(columnName));
    }

    public java.math.BigDecimal getBigDecimal(int position) throws SQLException {
        return SQLTypeConverter.getBigDecimal(currentResultSetManager
                .getObject(position));
    }

    public java.math.BigDecimal getBigDecimal(int position, int scale)
            throws SQLException {
        return getBigDecimal(position);
    }

    public java.math.BigDecimal getBigDecimal(String columnName, int scale)
            throws SQLException {
        return getBigDecimal(findColumn(columnName));
    }

    public java.io.InputStream getBinaryStream(int columnIndex)
            throws SQLException {
        return SQLTypeConverter.getBinaryStream(currentResultSetManager
                .getObject(columnIndex));
    }

    public java.io.InputStream getBinaryStream(String columnName)
            throws SQLException {
        return getBinaryStream(findColumn(columnName));
    }

    public java.sql.Blob getBlob(int columnIndex) throws SQLException {
        return SQLTypeConverter.getBlob(currentResultSetManager
                .getObject(columnIndex));
    }

    public java.sql.Blob getBlob(String columnName) throws SQLException {
        return getBlob(findColumn(columnName));
    }

    public boolean getBoolean(int columnIndex) throws SQLException {
        return SQLTypeConverter.getBoolean(currentResultSetManager
                .getObject(columnIndex));
    }

    public boolean getBoolean(String columnName) throws SQLException {
        return getBoolean(findColumn(columnName));
    }

    public byte getByte(int columnIndex) throws SQLException {
        return SQLTypeConverter.getByte(currentResultSetManager
                .getObject(columnIndex));
    }

    public byte getByte(String columnName) throws SQLException {
        return getByte(findColumn(columnName));
    }

    public byte[] getBytes(String columnName) throws SQLException {
        return getBytes(findColumn(columnName));
    }

    public java.io.Reader getCharacterStream(int columnIndex)
            throws SQLException {
        return SQLTypeConverter.getCharacterStream(currentResultSetManager
                .getObject(columnIndex));
    }

    public java.io.Reader getCharacterStream(String columnName)
            throws SQLException {
        return getCharacterStream(findColumn(columnName));
    }

    public java.sql.Clob getClob(int columnIndex) throws SQLException {
        return SQLTypeConverter.getClob(currentResultSetManager
                .getObject(columnIndex));
    }

    public java.sql.Clob getClob(String columnName) throws SQLException {
        return getClob(findColumn(columnName));
    }

    public int getConcurrency() throws SQLException {
        throw new SQLException("method not supported");
    }

    public String getCursorName() throws SQLException {
        throw new SQLException("method not supported");
    }

    public java.sql.Date getDate(int columnIndex) throws SQLException {
        return getDate(columnIndex, Calendar.getInstance());
    }

    public java.sql.Date getDate(String columnName) throws SQLException {
        return getDate(findColumn(columnName), Calendar.getInstance());
    }

    public java.sql.Date getDate(int columnIndex, Calendar cal)
            throws SQLException {
        return SQLTypeConverter.getDate(currentResultSetManager
                .getObject(columnIndex), cal);
    }

    public java.sql.Date getDate(String columnName, Calendar cal)
            throws SQLException {
        return getDate(findColumn(columnName), cal);
    }

    public double getDouble(int columnIndex) throws SQLException {
        return SQLTypeConverter.getDouble(currentResultSetManager
                .getObject(columnIndex));
    }

    public double getDouble(String columnName) throws SQLException {
        return getDouble(findColumn(columnName));
    }

    public int getFetchDirection() throws SQLException {
        throw new SQLException("method not supported");
    }

    public int getFetchSize() throws SQLException {
        throw new SQLException("method not supported");
    }

    public float getFloat(int columnIndex) throws SQLException {
        return SQLTypeConverter.getFloat(currentResultSetManager
                .getObject(columnIndex));
    }

    public float getFloat(String columnName) throws SQLException {
        return getFloat(findColumn(columnName));
    }

    public Object getObject(int columnIndex) throws SQLException {
        return currentResultSetManager.getObject(columnIndex);
    }

    public Object getObject(String columnName) throws SQLException {
        return getObject(findColumn(columnName));
    }

    public Object getObject(int columnIndex, java.util.Map<String, Class<?>> map)
            throws SQLException {
        return SQLTypeConverter.getObject(currentResultSetManager
                .getObject(columnIndex), map);
    }

    public Object getObject(String columnName,
            java.util.Map<String, Class<?>> map) throws SQLException {
        return getObject(findColumn(columnName), map);
    }

    public java.sql.Ref getRef(int columnIndex) throws SQLException {
        return SQLTypeConverter.getRef(currentResultSetManager
                .getObject(columnIndex));
    }

    public java.sql.Ref getRef(String columnName) throws SQLException {
        return getRef(findColumn(columnName));
    }

    public int getRow() throws SQLException {
        return 0;
    }

    public short getShort(String columnName) throws SQLException {
        return getShort(findColumn(columnName));
    }

    public short getShort(int columnIndex) throws SQLException {
        return SQLTypeConverter.getShort(currentResultSetManager
                .getObject(columnIndex));
    }

    public java.sql.Statement getStatement() throws SQLException {
        return null;
    }

    public String getString(String columnName) throws SQLException {
        return getString(findColumn(columnName));
    }

    public java.sql.Time getTime(String columnName) throws SQLException {
        return getTime(findColumn(columnName));
    }

    public java.sql.Time getTime(int columnIndex) throws SQLException {
        return getTime(columnIndex, Calendar.getInstance());
    }

    public java.sql.Time getTime(String columnName, Calendar cal)
            throws SQLException {
        return getTime(findColumn(columnName), cal);
    }

    public java.sql.Time getTime(int columnIndex, Calendar cal)
            throws SQLException {
        return SQLTypeConverter.getTime(currentResultSetManager
                .getObject(columnIndex), cal);
    }

    public java.sql.Timestamp getTimestamp(String columnName)
            throws SQLException {
        return getTimestamp(findColumn(columnName), Calendar.getInstance());
    }

    public java.sql.Timestamp getTimestamp(int columnIndex) throws SQLException {
        return getTimestamp(columnIndex, Calendar.getInstance());
    }

    public java.sql.Timestamp getTimestamp(String columnName, Calendar cal)
            throws SQLException {
        return getTimestamp(findColumn(columnName), cal);
    }

    public java.sql.Timestamp getTimestamp(int columnIndex, Calendar cal)
            throws SQLException {
        return SQLTypeConverter.getTimestamp(currentResultSetManager
                .getObject(columnIndex), cal);
    }

    public int getType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    public java.net.URL getURL(int columnIndex) throws SQLException {
        return SQLTypeConverter.getURL(currentResultSetManager
                .getObject(columnIndex));
    }

    public java.net.URL getURL(String columnName) throws SQLException {
        return getURL(findColumn(columnName));
    }

    public java.io.InputStream getUnicodeStream(String columnName)
            throws SQLException {
        return getUnicodeStream(findColumn(columnName));
    }

    public java.io.InputStream getUnicodeStream(int columnIndex)
            throws SQLException {
        return SQLTypeConverter.getAsciiStream(currentResultSetManager
                .getObject(columnIndex));
    }

    public java.sql.SQLWarning getWarnings() throws SQLException {
        return null;
    }

    public void insertRow() throws SQLException {
        throw new SQLException("method not supported");
    }

    public boolean isAfterLast() throws SQLException {
        throw new SQLException("method not supported");
    }

    public boolean isBeforeFirst() throws SQLException {
        throw new SQLException("method not supported");
    }

    public boolean isFirst() throws SQLException {
        throw new SQLException("method not supported");
    }

    public boolean isLast() throws SQLException {
        throw new SQLException("method not supported");
    }

    public boolean last() throws SQLException {
        throw new SQLException("method not supported");
    }

    public void moveToCurrentRow() throws SQLException {
        throw new SQLException("method not supported");
    }

    public void moveToInsertRow() throws SQLException {
        throw new SQLException("method not supported");
    }

    public boolean previous() throws SQLException {
        throw new SQLException("method not supported");
    }

    public void refreshRow() throws SQLException {
        throw new SQLException("method not supported");
    }

    public boolean relative(int rows) throws SQLException {
        throw new SQLException("method not supported");
    }

    public boolean rowDeleted() throws SQLException {
        throw new SQLException("method not supported");
    }

    public boolean rowInserted() throws SQLException {
        throw new SQLException("method not supported");
    }

    public boolean rowUpdated() throws SQLException {
        throw new SQLException("method not supported");
    }

    public void setFetchDirection(int direction) throws SQLException {
        throw new SQLException("method not supported");
    }

    public void setFetchSize(int rows) throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateArray(String columnName, java.sql.Array x)
            throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateArray(int columnIndex, java.sql.Array x)
            throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateAsciiStream(String columnName, java.io.InputStream x,
            int length) throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateAsciiStream(int columnIndex, java.io.InputStream x,
            int length) throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateBigDecimal(String columnName, java.math.BigDecimal x)
            throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateBigDecimal(int columnIndex, java.math.BigDecimal x)
            throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateBinaryStream(int columnIndex, java.io.InputStream x,
            int length) throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateBinaryStream(String columnName, java.io.InputStream x,
            int length) throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateBlob(int columnIndex, java.sql.Blob x)
            throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateBlob(String columnName, java.sql.Blob x)
            throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateBoolean(String columnName, boolean x) throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateByte(String columnName, byte x) throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateBytes(String columnName, byte[] x) throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateCharacterStream(int columnIndex, java.io.Reader x,
            int length) throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateCharacterStream(String columnName, java.io.Reader reader,
            int length) throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateClob(String columnName, java.sql.Clob x)
            throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateClob(int columnIndex, java.sql.Clob x)
            throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateDate(int columnIndex, java.sql.Date x)
            throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateDate(String columnName, java.sql.Date x)
            throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateDouble(String columnName, double x) throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateFloat(String columnName, float x) throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateInt(String columnName, int x) throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateLong(String columnName, long x) throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateNull(String columnName) throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateObject(String columnName, Object x) throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateObject(int columnIndex, Object x, int scale)
            throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateObject(String columnName, Object x, int scale)
            throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateRef(int columnIndex, java.sql.Ref x) throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateRef(String columnName, java.sql.Ref x)
            throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateRow() throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateShort(String columnName, short x) throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateString(String columnName, String x) throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateTime(String columnName, java.sql.Time x)
            throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateTime(int columnIndex, java.sql.Time x)
            throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateTimestamp(String columnName, java.sql.Timestamp x)
            throws SQLException {
        throw new SQLException("method not supported");
    }

    public void updateTimestamp(int columnIndex, java.sql.Timestamp x)
            throws SQLException {
        throw new SQLException("method not supported");
    }

    public boolean wasNull() throws SQLException {
        return currentResultSetManager.lastValueWasNull();
    }

    public int getHoldability() throws SQLException {
        // TODO Added for 1.6 compatibility
        return 0;
    }

    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        // TODO Added for 1.6 compatibility
        return null;
    }

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

    public String getNString(int columnIndex) throws SQLException {
        // TODO Added for 1.6 compatibility
        return null;
    }

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
    //
    public boolean isClosed() throws SQLException {
        // TODO Added for 1.6 compatibility
        return false;
    }

    public void updateAsciiStream(int columnIndex, InputStream x)
            throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    public void updateAsciiStream(String columnLabel, InputStream x)
            throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    public void updateAsciiStream(int columnIndex, InputStream x, long length)
            throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    public void updateAsciiStream(String columnLabel, InputStream x, long length)
            throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    public void updateBinaryStream(int columnIndex, InputStream x)
            throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    public void updateBinaryStream(String columnLabel, InputStream x)
            throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    public void updateBinaryStream(int columnIndex, InputStream x, long length)
            throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    public void updateBinaryStream(String columnLabel, InputStream x,
            long length) throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    public void updateBlob(int columnIndex, InputStream inputStream)
            throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    public void updateBlob(String columnLabel, InputStream inputStream)
            throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    public void updateBlob(int columnIndex, InputStream inputStream, long length)
            throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    public void updateBlob(String columnLabel, InputStream inputStream,
            long length) throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    public void updateCharacterStream(int columnIndex, Reader x)
            throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    public void updateCharacterStream(String columnLabel, Reader reader)
            throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    public void updateCharacterStream(int columnIndex, Reader x, long length)
            throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    public void updateCharacterStream(String columnLabel, Reader reader,
            long length) throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    public void updateClob(String columnLabel, Reader reader)
            throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    public void updateClob(int columnIndex, Reader reader, long length)
            throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    public void updateClob(String columnLabel, Reader reader, long length)
            throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    public void updateNCharacterStream(int columnIndex, Reader x)
            throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    public void updateNCharacterStream(String columnLabel, Reader reader)
            throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    public void updateNCharacterStream(int columnIndex, Reader x, long length)
            throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    public void updateNCharacterStream(String columnLabel, Reader reader,
            long length) throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    // public void updateNClob(int columnIndex, NClob nClob) throws SQLException
    // {
    // // TODO Added for 1.6 compatibility
    //        
    // }
    //
    // public void updateNClob(String columnLabel, NClob nClob) throws
    // SQLException
    // {
    // // TODO Added for 1.6 compatibility
    //        
    // }

    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    public void updateNClob(String columnLabel, Reader reader)
            throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    public void updateNClob(int columnIndex, Reader reader, long length)
            throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    public void updateNClob(String columnLabel, Reader reader, long length)
            throws SQLException {
        // TODO Added for 1.6 compatibility

    }

    public void updateNString(int columnIndex, String nString)
            throws SQLException {
        // TODO Added for 1.6 compatibility

    }

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

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        // TODO Added for 1.6 compatibility
        return false;
    }

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
