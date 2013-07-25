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
 * XDBResultSetMetaData.java
 *
 *  
 */

package org.postgresql.stado.common;

import java.sql.*;

import org.postgresql.stado.engine.io.DataTypes;


/**
 * 
 *  
 */
public class XDBResultSetMetaData implements java.sql.ResultSetMetaData {

    private ColumnMetaData[] metaInfo = null;

    /** Creates a new instance of XDBResultSetMetaData */
    private XDBResultSetMetaData() {
    }

    /**
     * must pass in the full column info
     * @param metaInfo 
     */
    public XDBResultSetMetaData(ColumnMetaData[] metaInfo) {
        this.metaInfo = metaInfo;
    }

    /**
     * Gets the designated column's table's catalog name.
     * 
     * @param column
     *            the first column is 1, the second is 2, ...
     * @return the name of the catalog for the table in which the given column
     *         appears or "" if not applicable
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    /**
     * <p>
     * Returns the fully-qualified name of the Java class whose instances are
     * manufactured if the method <code>ResultSet.getObject</code> is called
     * to retrieve a value from the column. <code>ResultSet.getObject</code>
     * may return a subclass of the class returned by this method.
     * 
     * @param column
     *            the first column is 1, the second is 2, ...
     * @return the fully-qualified name of the class in the Java programming
     *         language that would be used by the method
     *         <code>ResultSet.getObject</code> to retrieve the value in the
     *         specified column. This is the class name used for custom mapping.
     * @exception SQLException
     *                if a database access error occurs
     * @since 1.2
     * 
     */
    public String getColumnClassName(int column) throws SQLException {
        // XData ???
        return Object.class.getName();
    }

    /**
     * Returns the number of columns in this <code>ResultSet</code> object.
     * 
     * @return the number of columns
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public int getColumnCount() throws SQLException {
        return this.metaInfo.length;
    }

    /**
     * Indicates the designated column's normal maximum width in characters.
     * 
     * @param column
     *            the first column is 1, the second is 2, ...
     * @return the normal maximum number of characters allowed as the width of
     *         the designated column
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public int getColumnDisplaySize(int column) throws SQLException {
        return metaInfo[column - 1].maxLength;
    }

    /**
     * Gets the designated column's suggested title for use in printouts and
     * displays.
     * 
     * @param column
     *            the first column is 1, the second is 2, ...
     * @return the suggested column title
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public String getColumnLabel(int column) throws SQLException {
        return metaInfo[column - 1].alias;
    }

    /**
     * Get the designated column's name.
     * 
     * @param column
     *            the first column is 1, the second is 2, ...
     * @return column name
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public String getColumnName(int column) throws SQLException {
        return metaInfo[column - 1].name;
    }

    /**
     * Retrieves the designated column's SQL type.
     * 
     * @param column
     *            the first column is 1, the second is 2, ...
     * @return SQL type from java.sql.Types
     * @exception SQLException
     *                if a database access error occurs
     * @see Types
     * 
     */
    public int getColumnType(int column) throws SQLException {
        return metaInfo[column - 1].javaSqlType;
    }

    /**
     * Retrieves the designated column's database-specific type name.
     * 
     * @param column
     *            the first column is 1, the second is 2, ...
     * @return type name used by the database. If the column type is a
     *         user-defined type, then a fully-qualified type name is returned.
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public String getColumnTypeName(int column) throws SQLException {
        return DataTypes.getJavaTypeDesc(getColumnType(column));
    }

    /**
     * Get the designated column's number of decimal digits.
     * 
     * @param column
     *            the first column is 1, the second is 2, ...
     * @return precision
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public int getPrecision(int column) throws SQLException {
        return metaInfo[column - 1].precision;
    }

    /**
     * Gets the designated column's number of digits to right of the decimal
     * point.
     * 
     * @param column
     *            the first column is 1, the second is 2, ...
     * @return scale
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public int getScale(int column) throws SQLException {
        return metaInfo[column - 1].scale;
    }

    /**
     * Get the designated column's table's schema.
     * 
     * @param column
     *            the first column is 1, the second is 2, ...
     * @return schema name or "" if not applicable
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public String getSchemaName(int column) throws SQLException {
        return "";// n/a
    }

    /**
     * Gets the designated column's table name.
     * 
     * @param column
     *            the first column is 1, the second is 2, ...
     * @return table name or "" if not applicable
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public String getTableName(int column) throws SQLException {
        return metaInfo[column - 1].tableName;
    }

    /**
     * Indicates whether the designated column is automatically numbered, thus
     * read-only.
     * 
     * @param column
     *            the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public boolean isAutoIncrement(int column) throws SQLException {
        return metaInfo[column - 1].isAutoIncrement;
    }

    /**
     * Indicates whether a column's case matters.
     * 
     * @param column
     *            the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public boolean isCaseSensitive(int column) throws SQLException {
        return metaInfo[column - 1].isCaseSensitive;
    }

    /**
     * Indicates whether the designated column is a cash value.
     * 
     * @param column
     *            the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public boolean isCurrency(int column) throws SQLException {
        return metaInfo[column - 1].isCurrency;
    }

    /**
     * Indicates whether a write on the designated column will definitely
     * succeed.
     * 
     * @param column
     *            the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return !metaInfo[column - 1].isReadOnly;
    }

    /**
     * Indicates the nullability of values in the designated column.
     * 
     * @param column
     *            the first column is 1, the second is 2, ...
     * @return the nullability status of the given column; one of
     *         <code>columnNoNulls</code>, <code>columnNullable</code> or
     *         <code>columnNullableUnknown</code>
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public int isNullable(int column) throws SQLException {
        return metaInfo[column - 1].nullable;
    }

    /**
     * Indicates whether the designated column is definitely not writable.
     * 
     * @param column
     *            the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public boolean isReadOnly(int column) throws SQLException {
        return metaInfo[column - 1].isReadOnly;
    }

    /**
     * Indicates whether the designated column can be used in a where clause.
     * 
     * @param column
     *            the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public boolean isSearchable(int column) throws SQLException {
        return metaInfo[column - 1].isSearchable;
    }

    /**
     * Indicates whether values in the designated column are signed numbers.
     * 
     * @param column
     *            the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public boolean isSigned(int column) throws SQLException {
        return metaInfo[column - 1].isSigned;
    }

    /**
     * Indicates whether it is possible for a write on the designated column to
     * succeed.
     * 
     * @param column
     *            the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException
     *                if a database access error occurs
     * 
     */
    public boolean isWritable(int column) throws SQLException {
        return metaInfo[column - 1].isWritable;
    }

    /**
     * 
     * @param iface 
     * @throws java.sql.SQLException 
     * @return 
     */
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        // TODO Added for 1.6 compatibility
        return false;
    }

    /**
     * 
     * @param iface 
     * @throws java.sql.SQLException 
     * @return 
     */
    public <T> T unwrap(Class<T> iface) throws SQLException {
        // TODO Added for 1.6 compatibility
        return null;
    }

}
