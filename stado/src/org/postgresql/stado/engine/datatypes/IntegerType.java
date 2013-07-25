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
 * IntegerType.java
 *
 *  
 */

package org.postgresql.stado.engine.datatypes;

import java.sql.SQLException;
import java.sql.Types;

/**
 * 
 *  
 */
public class IntegerType implements XData {
    public Integer val = null;

    private boolean isNull = true;

    /** Creates a new instance of IntegerType */
    public IntegerType(String str) {
        if (str != null) {
            val = new Integer(str);
            isNull = false;
        }
    }

    public IntegerType(int i) {
        val = new Integer(i);
        isNull = false;
    }

    public int getJavaType() {
        return Types.INTEGER;
    }

    /** get the current data type value as string */
    @Override
    public String toString() {
        if (val == null) {
            return null;
        }
        return val.toString();
    }

    /** is this data value null? */
    public boolean isValueNull() {
        return isNull;
    }

    /** indicate whether the current data value is null */
    public void setNull(boolean isNull) {
        this.isNull = isNull;
    }

    public void setJavaType(int type) {
    }

    /** returns the java */
    public Object getObject() {
        return val;
    }

    public void setObject(Object o) throws SQLException {
    }

    /** convert the value to string with the specified charset encoding */
    public String toString(String encoding)
            throws java.io.UnsupportedEncodingException {
        if (val == null) {
            return null;
        }
        return new String(val.toString().getBytes(encoding));
    }

}
