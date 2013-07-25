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
 * DataTypes.java
 *
 *  
 */

package org.postgresql.stado.engine.io;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;

import org.postgresql.stado.common.util.ParseCmdLine;

/**
 * List of the current java sql data types. This is similarly to java.sql.Types,
 * but is unique to XDB protocol. Thus, shouldn't use these numbers for anything
 * else
 * 
 *  
 */
public class DataTypes {
    // don't use logger here, for build process
    // private static final XLogger logger = XLogger.getLogger(DataTypes.class);
    
    /**
     * This method is introduced to get the proper description of TIME and TIMESTAMP datatype.
     * For other types it makes call to getJavaTypeDesc(int sqlType).
     * FB# 7753
     * AM: The method is obviously incomplete, and reference to FB# 7753 is not
     * longer available. Unless someone defines requirements for the data type
     * formatting fallback to getJavaTypeDesc(sqlType);
     * @param sqlType 
     * @param length 
     * @param precision
     * @param scale
     * @param withTimeZone 
     */
    public static String getJavaTypeDesc(int sqlType, int length, int precision, int scale, boolean withTimeZone) {
/*        
        String typeString = "";
        HashMap<String, String> map = new HashMap<String,String>();
        map.put("length", "" + length);
        map.put("precision", "" + precision);
        map.put("scale", "" + scale);
        
        switch (sqlType) {
            case Types.TIME:
                return ParseCmdLine.substitute(typeString, map);
            case Types.TIMESTAMP:
                return ParseCmdLine.substitute(typeString, map);
            default:
                return getJavaTypeDesc(sqlType);
        }
*/
        return getJavaTypeDesc(sqlType);
    }

    public static String getJavaTypeDesc(int sqlType) {                
        switch (sqlType) {
        case Types.BIT:
            return "BIT";
        case Types.TINYINT:
            return "TINYINT";
        case Types.SMALLINT:
            return "SMALLINT";
        case Types.INTEGER:
            return "INTEGER";
        case Types.BIGINT:
            return "BIGINT";
        case Types.FLOAT:
            return "FLOAT";
        case Types.REAL:
            return "REAL";
        case Types.DOUBLE:
            return "DOUBLE PRECISION";
        case Types.NUMERIC:
            return "NUMERIC";
        case Types.DECIMAL:
            return "DECIMAL";
        case Types.CHAR:
            return "CHAR";
        case Types.VARCHAR:
            return "VARCHAR";
        case Types.LONGVARCHAR:
            return "LONGVARCHAR";
        case Types.DATE:
            return "DATE";
        case Types.TIME:
            return "TIME";
        case Types.TIMESTAMP:
            return "TIMESTAMP";
        case Types.BINARY:
            return "BINARY";
        case Types.VARBINARY:
            return "VARBINARY";
        case Types.LONGVARBINARY:
            return "LONGVARBINARY";
        case Types.NULL:
            return "NULL";
        case Types.OTHER:
            return "OTHER";
        case Types.JAVA_OBJECT:
            return "JAVA_OBJECT";
        case Types.DISTINCT:
            return "DISTINCT";
        case Types.STRUCT:
            return "STRUCT";
        case Types.ARRAY:
            return "ARRAY";
        case Types.BLOB:
            return "BLOB";
        case Types.CLOB:
            return "CLOB";
        case Types.REF:
            return "REF";
        case Types.DATALINK:
            return "DATALINK";
        case Types.BOOLEAN:
            return "BOOLEAN";
        default:
            // logger.warn("undefined sql type: " + javaSqlType);
            return null;
        }
    }

    public static int getJavaType(String typeName) {
        typeName = typeName.trim().toUpperCase();
        if ("BIT".equals(typeName)) {
            return Types.BIT;
        } else if ("TINYINT".equals(typeName)) {
            return Types.TINYINT;
        } else if ("SMALLINT".equals(typeName)) {
            return Types.SMALLINT;
        } else if ("INTEGER".equals(typeName)) {
            return Types.INTEGER;
        } else if ("BIGINT".equals(typeName)) {
            return Types.BIGINT;
        } else if ("FLOAT".equals(typeName)) {
            return Types.FLOAT;
        } else if ("REAL".equals(typeName)) {
            return Types.REAL;
        } else if ("DOUBLE PRECISION".equals(typeName)) {
            return Types.DOUBLE;
        } else if ("NUMERIC".equals(typeName)) {
            return Types.NUMERIC;
        } else if ("DECIMAL".equals(typeName)) {
            return Types.DECIMAL;
        } else if ("CHAR".equals(typeName)) {
            return Types.CHAR;
        } else if ("VARCHAR".equals(typeName)) {
            return Types.VARCHAR;
        } else if ("LONGVARCHAR".equals(typeName)) {
            return Types.LONGVARCHAR;
        } else if ("DATE".equals(typeName)) {
            return Types.DATE;
        } else if ("TIME".equals(typeName)) {
            return Types.TIME;
        } else if ("TIMESTAMP".equals(typeName)) {
            return Types.TIMESTAMP;
        } else if ("BINARY".equals(typeName)) {
            return Types.BINARY;
        } else if ("VARBINARY".equals(typeName)) {
            return Types.VARBINARY;
        } else if ("LONGVARBINARY".equals(typeName)) {
            return Types.LONGVARBINARY;
        } else if ("NULL".equals(typeName)) {
            return Types.NULL;
        } else if ("OTHER".equals(typeName)) {
            return Types.OTHER;
        } else if ("JAVA_OBJECT".equals(typeName)) {
            return Types.JAVA_OBJECT;
        } else if ("DISTINCT".equals(typeName)) {
            return Types.DISTINCT;
        } else if ("STRUCT".equals(typeName)) {
            return Types.STRUCT;
        } else if ("ARRAY".equals(typeName)) {
            return Types.ARRAY;
        } else if ("BLOB".equals(typeName)) {
            return Types.BLOB;
        } else if ("CLOB".equals(typeName)) {
            return Types.CLOB;
        } else if ("REF".equals(typeName)) {
            return Types.REF;
        } else if ("DATALINK".equals(typeName)) {
            return Types.DATALINK;
        } else if ("BOOLEAN".equals(typeName)) {
            return Types.BOOLEAN;
        } else {
            // logger.warn("undefined sql type: " + typeName);
            return Types.NULL;
        }
    }

    /** is this a numeric data type? */
    public static boolean isNumeric(int javaType) {
        return javaType == Types.BIT || javaType == Types.TINYINT
                || javaType == Types.SMALLINT || javaType == Types.INTEGER
                || javaType == Types.BIGINT || javaType == Types.FLOAT
                || javaType == Types.REAL || javaType == Types.DOUBLE
                || javaType == Types.NUMERIC || javaType == Types.DECIMAL;
    }

    public static boolean isString(int javaType) {
        return javaType == Types.CHAR || javaType == Types.VARCHAR
                || javaType == Types.LONGVARCHAR;
    }

    public static boolean isBinary(int javaType) {
        return javaType == Types.BINARY || javaType == Types.BIT
                || javaType == Types.VARBINARY
                || javaType == Types.LONGVARBINARY || javaType == Types.BLOB;
    }

    public static void setParameter(PreparedStatement ps, int index,
            String value, int sqlType) throws SQLException {
        if (value == null) {
            ps.setNull(index, sqlType);
            return;
        }
        switch (sqlType) {
        case Types.BIGINT:
            ps.setLong(index, Long.parseLong(value));
            break;
        case Types.INTEGER:
        case Types.SMALLINT:
        case Types.TINYINT:
            ps.setInt(index, Integer.parseInt(value));
            break;
        case Types.DECIMAL:
        case Types.DOUBLE:
        case Types.FLOAT:
        case Types.NUMERIC:
        case Types.REAL:
            ps.setDouble(index, Double.parseDouble(value));
            break;
        case Types.DATE:
        case Types.TIME:
        case Types.TIMESTAMP:
        default:
            ps.setString(index, value);
            break;
        }
    }
}
