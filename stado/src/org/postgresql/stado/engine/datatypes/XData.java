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
 * XData.java
 *
 *  
 */

package org.postgresql.stado.engine.datatypes;

import java.sql.SQLException;

/**
 * All XDB datatype should implement this
 * 
 *  
 */
public interface XData {

    public int getJavaType();

    public void setJavaType(int type);// useful for some classes

    /** is this data value null? */
    public boolean isValueNull();

    /** indicate whether the current data value is null */
    public void setNull(boolean isNull);

    /**
     * get the current data type value as string - default encoding or use the
     * last known type
     */
    public String toString();

    /** convert the value to string with the specified charset encoding */
    public String toString(String encoding)
            throws java.io.UnsupportedEncodingException;

    /** returns the java object implemented */
    public Object getObject() throws SQLException;

    public void setObject(Object o) throws SQLException;

}
