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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Ref;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

import org.postgresql.stado.engine.datatypes.XBlob;
import org.postgresql.stado.engine.datatypes.XClob;


/**
 * 
 * 
 */
public class SQLTypeConverter {
    public static Array getArray(Object value) throws SQLException {
        throw new SQLException("Data type is not supported");
    }

    public static InputStream getAsciiStream(Object value) throws SQLException {
        if (value == null) {
            return null;
        } else if (value instanceof Clob) {
            return ((Clob) value).getAsciiStream();
        } else {
            return new XClob(value.toString()).getAsciiStream();
        }
    }

    public static InputStream getBinaryStream(Object value) throws SQLException {
        if (value == null) {
            return null;
        } else if (value instanceof Blob) {
            return ((Blob) value).getBinaryStream();
        } else {
            return new XBlob(getBytes(value)).getBinaryStream();
        }
    }

    public static BigDecimal getBigDecimal(Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        } else if (value instanceof Number) {
            return new BigDecimal(((Number) value).doubleValue());
        } else if (value instanceof Boolean) {
            return ((Boolean) value).booleanValue() ? new BigDecimal(1)
                    : new BigDecimal(0);
        } else {
            return new BigDecimal(value.toString().trim());
        }
    }

    public static Blob getBlob(Object value) throws SQLException {
        if (value == null) {
            return null;
        } else if (value instanceof Blob) {
            return (Blob) value;
        } else {
            return new XBlob(getBytes(value));
        }
    }

    public static boolean getBoolean(Object value) throws SQLException {
        if (value == null) {
            return false;
        }
        if (value instanceof Boolean) {
            return ((Boolean) value).booleanValue();
        } else if (value instanceof Number) {
            return ((Number) value).byteValue() != 0;
        } else {
            return Boolean.valueOf(value.toString().trim()).booleanValue();
        }
    }

    public static byte getByte(Object value) throws SQLException {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number) {
            return ((Number) value).byteValue();
        } else if (value instanceof Boolean) {
            return ((Boolean) value).booleanValue() ? (byte) 1 : (byte) 0;
        } else {
            return Byte.parseByte(value.toString().trim());
        }
    }

    public static byte[] getBytes(Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[]) {
            return (byte[]) value;
        } else {
            // Try and parse hexadecimal string
            String strVal = value.toString().trim();
            if (strVal.length() % 2 != 0) {
                strVal = "0" + strVal;
            }
            byte[] out = new byte[strVal.length() / 2];
            try {
                for (int i = 0; i < out.length; i++) {
                    out[i] = (byte) Integer.parseInt(strVal.substring(2 * i,
                            2 * i + 2), 16);
                }
                return out;
            } catch (NumberFormatException nfe) {
                // Invalid hexadecimal
            }
            // Invalid hexadecimal
            return value.toString().trim().getBytes();
        }
    }

    public static Reader getCharacterStream(Object value) throws SQLException {
        if (value == null) {
            return null;
        } else if (value instanceof Clob) {
            return ((Clob) value).getCharacterStream();
        } else {
            return new XClob(value.toString()).getCharacterStream();
        }
    }

    public static Clob getClob(Object value) throws SQLException {
        if (value == null) {
            return null;
        } else if (value instanceof Clob) {
            return (Clob) value;
        } else {
            return new XClob(value.toString());
        }
    }

    public static java.sql.Date getDate(Object value, Calendar calendar)
            throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof java.util.Date) {
            return new java.sql.Date(((java.util.Date) value).getTime());
        } else if (value instanceof Number) {
            return new java.sql.Date(((Number) value).longValue());
        } else {
            try {
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                return new java.sql.Date(df.parse(value.toString().trim())
                        .getTime());
            } catch (Throwable t) {
                return new java.sql.Date(Long
                        .parseLong(value.toString().trim()));
            }
        }
    }

    public static double getDouble(Object value) throws SQLException {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        } else if (value instanceof Boolean) {
            return ((Boolean) value).booleanValue() ? 1 : 0;
        } else {
            return Double.parseDouble(value.toString().trim());
        }
    }

    public static int getInt(Object value) throws SQLException {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        } else if (value instanceof Boolean) {
            return ((Boolean) value).booleanValue() ? 1 : 0;
        } else {
            return Integer.parseInt(value.toString().trim());
        }
    }

    public static float getFloat(Object value) throws SQLException {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        } else if (value instanceof Boolean) {
            return ((Boolean) value).booleanValue() ? 1 : 0;
        } else {
            return Float.parseFloat(value.toString().trim());
        }
    }

    public static long getLong(Object value) throws SQLException {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        } else if (value instanceof Boolean) {
            return ((Boolean) value).booleanValue() ? 1 : 0;
        } else {
            return Long.parseLong(value.toString().trim());
        }
    }

    public static Object getObject(Object value, Map map) throws SQLException {
        return value;
    }

    public static Ref getRef(Object value) throws SQLException {
        throw new SQLException("Data type is not supported");
    }

    public static short getShort(Object value) throws SQLException {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number) {
            return ((Number) value).shortValue();
        } else if (value instanceof Boolean) {
            return ((Boolean) value).booleanValue() ? (short) 1 : (short) 0;
        } else {
            return Short.parseShort(value.toString().trim());
        }
    }

    public static String getString(Object value) throws SQLException {
        if (value == null) {
            return null;
        } else if (value instanceof byte[]) {
            return getHexString((byte[]) value);
        } else {
            return value.toString();
        }
    }

    public static java.sql.Time getTime(Object value, Calendar calendar)
            throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof java.util.Date) {
            return new java.sql.Time(((java.util.Date) value).getTime());
        } else if (value instanceof Number) {
            return new java.sql.Time(((Number) value).longValue());
        } else {
            try {
                SimpleDateFormat tf = new SimpleDateFormat("HH:mm:ss");
                return new java.sql.Time(tf.parse(value.toString().trim())
                        .getTime());
            } catch (Throwable t) {
                return new java.sql.Time(Long
                        .parseLong(value.toString().trim()));
            }
        }
    }

    public static java.sql.Timestamp getTimestamp(Object value,
            Calendar calendar) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof java.util.Date) {
            return new java.sql.Timestamp(((java.util.Date) value).getTime());
        } else if (value instanceof Number) {
            return new java.sql.Timestamp(((Number) value).longValue());
        } else {
            try {
                SimpleDateFormat tsf = new SimpleDateFormat(
                        "yyyy-MM-dd HH:mm:ss");
                return new java.sql.Timestamp(tsf
                        .parse(value.toString().trim()).getTime());
            } catch (Throwable t) {
                return new java.sql.Timestamp(Long.parseLong(value.toString()
                        .trim()));
            }
        }
    }

    public static URL getURL(Object value) throws SQLException {
        throw new SQLException("Data type is not supported");
    }

    public static String getHexString(byte[] bytes) {
        StringBuffer hexadecimal = new StringBuffer(bytes.length * 4);
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] >= 32 && bytes[i] < 128) {
                hexadecimal.append((char) bytes[i]);
            } else if (bytes[i] == '\n') {
                hexadecimal.append("\\n");
            } else if (bytes[i] == '\r') {
                hexadecimal.append("\\r");
            } else if (bytes[i] == '\t') {
                hexadecimal.append("\\t");
            } else if (bytes[i] == '\b') {
                hexadecimal.append("\\b");
            } else if (bytes[i] == '\f') {
                hexadecimal.append("\\f");
            } else {
                // non-printable characters
                hexadecimal.append("\\x");
                int high = (bytes[i] >> 4) & 0xf;
                hexadecimal.append(high < 10 ? (char) ('0' + high)
                        : (char) ('a' + high - 10));
                int low = bytes[i] & 0xf;
                hexadecimal.append(low < 10 ? (char) ('0' + low)
                        : (char) ('a' + low - 10));
            }
        }
        return hexadecimal.toString();
    }
}
