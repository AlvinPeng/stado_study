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
 * ByteArrayType.java
 *
 *  
 */

package org.postgresql.stado.engine.datatypes;

import java.sql.SQLException;

/**
 * handles BINARY, VARBINARY, LONGVARBINARY
 * 
 *  
 */
public class ByteArrayType implements org.postgresql.stado.engine.datatypes.XData {
    public byte[] val = {};

    public int javaType = java.sql.Types.BINARY;

    public boolean isNull = true;

    /** Creates a new instance of ByteArrayType */
    private ByteArrayType() {
    }

    public ByteArrayType(byte[] bs) {
        this.val = bs;
        if (bs != null) {
            isNull = false;
        }
    }

    public int getJavaType() {
        return this.javaType;
    }

    /** returns the java object implemented */
    public Object getObject() throws SQLException {
        return this.val;
    }

    /** is this data value null? */
    public boolean isValueNull() {
        return isNull;
    }

    public void setJavaType(int type) {
        this.javaType = type;
    }

    /** indicate whether the current data value is null */
    public void setNull(boolean isNull) {
        this.isNull = isNull;
    }

    public void setObject(Object o) throws SQLException {
    }

    @Override
    public String toString() {
        if (val == null) {
            return null;
        }
        return new String(val);
    }

    /** convert the value to string with the specified charset encoding */
    public String toString(String encoding)
            throws java.io.UnsupportedEncodingException {
        if (val == null) {
            return null;
        }
        return new String(val, 0, val.length, encoding);// todo
    }
}
