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
 * ColumnMetaData.java
 *
 *  
 */

package org.postgresql.stado.common;

/**
 * 
 *  
 */
public class ColumnMetaData {

    public static final short IS_NULLABLE = 0x01;

    public static final short IS_WRITABLE = 0x02;

    public static final short IS_CURRENCY = 0x04;

    public static final short IS_SIGNED_NUM = 0x08;

    public static final short IS_CASE_SENSITIVE = 0x10;

    public static final short IS_AUTO_INCREMENT = 0x20;// i.e. serial..

    public static final short IS_READ_ONLY = 0x40;

    public static final short IS_SEARCHABLE = 0x80;

    public static final short IS_PRIMARY_KEY = 0x100;

    public static final short IS_FOREIGN_KEY = 0x200;

    public String name = null;// underlying name

    public String alias = null;

    public int javaSqlType = -111111;// unknown??

    public int maxLength = 0;

    public int precision = 0;

    public int scale = 0;

    public String tableName = null;

    // derived from flags
    public int nullable = java.sql.ResultSetMetaData.columnNullableUnknown;

    public boolean isAutoIncrement = false;

    public boolean isCaseSensitive = false;

    public boolean isCurrency = false;

    public boolean isSigned = false;

    public boolean isWritable = true;

    public boolean isReadOnly = false;

    public boolean isSearchable = true;

    public boolean isPrimaryKey = false;

    public boolean isForeignKey = false;

    /**
     * name - the actual database column name precision - number of decimal
     * digits scale - number of digits right of decimal point
     * 
     * @param name
     * @param alias
     * @param length
     * @param xdbType
     * @param precision
     * @param scale
     * @param tableName
     * @param flags
     * @param checkFlags
     */
    public ColumnMetaData(String name, String alias, int length, int xdbType,
            int precision, int scale, String tableName, short flags,
            boolean checkFlags) {
        this.name = name != null ? name : alias;
        this.alias = alias != null ? alias : name;

        this.maxLength = length;
        this.javaSqlType = xdbType;
        this.precision = precision;
        this.tableName = tableName;

        if (checkFlags) {// otherwise use the defaults?
            boolean isNullable = (flags & IS_NULLABLE) == 1;
            if (isNullable) {
                this.nullable = java.sql.ResultSetMetaData.columnNullable;
            } else {
                this.nullable = java.sql.ResultSetMetaData.columnNoNulls;
            }

            isWritable = (flags & IS_WRITABLE) == 1;
            isCurrency = (flags & IS_CURRENCY) == 1;
            isSigned = (flags & IS_SIGNED_NUM) == 1;
            isCaseSensitive = (flags & IS_CASE_SENSITIVE) == 1;
            isAutoIncrement = (flags & IS_AUTO_INCREMENT) == 1;// if true, the
            // should also
            // be read-only
            isReadOnly = (flags & IS_READ_ONLY) == 1;
            isSearchable = (flags & IS_SEARCHABLE) == 1;
            isPrimaryKey = (flags & IS_PRIMARY_KEY) == 1;
            isForeignKey = (flags & IS_FOREIGN_KEY) == 1;
        }
    }
}
