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
 * FloatType.java
 *
 *  
 */

package org.postgresql.stado.engine.datatypes;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.sql.Types;

/**
 * 
 *  
 */
public class FloatType implements XData {
    private int javaType = Types.FLOAT;

    private BigDecimal mantissa = null;

    private Integer exponent = null;

    private static final BigInteger TEN = new BigInteger(new byte[] { 10 });

    /** Creates a new instance of FloatType */
    public FloatType(String str) {
        if (str != null) {
            int idx = str.indexOf("E");
            if (idx < 0) {
                idx = str.indexOf("e");
            }
            try {
                if (idx < 0) {
                    mantissa = new BigDecimal(str);
                    exponent = null;
                } else {
                    mantissa = new BigDecimal(str.substring(0, idx));
                    String expStr = str.substring(idx + 1);
                    if (expStr.charAt(0) == '+') {
                        expStr = expStr.substring(1);
                    }
                    exponent = new Integer(expStr);
                }
            } catch (NumberFormatException e) {
                mantissa = null;
                exponent = null;
            }
        }
    }

    public int getJavaType() {
        return javaType;
    }

    /** get the current data type value as string */
    @Override
    public String toString() {
        Object obj = null;
        try {
            obj = getObject();
        } catch (SQLException e) {
            // Ignore and leaving NULL
        }
        return obj == null ? null : obj.toString();
    }

    /** is this data value null? */
    public boolean isValueNull() {
        return mantissa == null;
    }

    /** indicate whether the current data value is null */
    public void setNull(boolean isNull) {
        if (isNull) {
            mantissa = null;
            exponent = null;
        }
    }

    public void setJavaType(int type) {
        javaType = type;
    }

    /** returns the java object implemented */
    public Object getObject() throws SQLException {
        if (mantissa == null) {
            return null;
        }
        if (exponent == null) {
            return mantissa;
        }
        BigInteger unscaled = mantissa.unscaledValue();
        int newScale = mantissa.scale() - exponent.intValue();
        if (newScale < 0) {
            unscaled = unscaled.multiply(TEN.pow(-newScale));
            newScale = 0;
        }
        return new BigDecimal(unscaled, newScale);
    }

    public void setObject(Object o) throws SQLException {
    }

    /** convert the value to string with the specified charset encoding */
    public String toString(String encoding)
            throws java.io.UnsupportedEncodingException {
        if (isValueNull()) {
            return null;
        }
        return new String(toString().getBytes(encoding));
    }

}
