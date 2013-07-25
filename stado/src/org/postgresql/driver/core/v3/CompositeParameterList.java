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
package org.postgresql.driver.core.v3;

import org.postgresql.driver.core.*;
import org.postgresql.driver.util.GT;
import org.postgresql.driver.util.PSQLException;
import org.postgresql.driver.util.PSQLState;

import java.sql.SQLException;
import java.io.InputStream;

/**
 * Parameter list for V3 query strings that contain multiple statements.
 * We delegate to one SimpleParameterList per statement, and translate
 * parameter indexes as needed.
 *
 * @author Oliver Jowett (oliver@opencloud.com)
 */
class CompositeParameterList implements V3ParameterList {
    CompositeParameterList(SimpleParameterList[] subparams, int[] offsets) {
        this.subparams = subparams;
        this.offsets = offsets;
        this.total = offsets[offsets.length - 1] + subparams[offsets.length - 1].getInParameterCount();
    }

    private final int findSubParam(int index) throws SQLException {
        if (index < 1 || index > total)
            throw new PSQLException(GT.tr("The column index is out of range: {0}, number of columns: {1}.", new Object[]{new Integer(index), new Integer(total)}), PSQLState.INVALID_PARAMETER_VALUE );

        for (int i = offsets.length - 1; i >= 0; --i)
            if (offsets[i] < index)
                return i;

        throw new IllegalArgumentException("I am confused; can't find a subparam for index " + index);
    }
    public void registerOutParameter(int index, int sqlType)
    {
        
    }
    public int getDirection(int i)
    {return 0;}
    public int getParameterCount()
    {
        return total;
    }
    public int getInParameterCount() {
        return total;
    }
    public int getOutParameterCount()
    {
        return 0;
    }

    public int[] getTypeOIDs() {
        int oids[] = new int[total];
        for (int i=0; i<offsets.length; i++) {
            int subOids[] = subparams[i].getTypeOIDs();
            System.arraycopy(subOids, 0, oids, offsets[i], subOids.length);
        }
        return oids;
    }

    public void setIntParameter(int index, int value) throws SQLException {
        int sub = findSubParam(index);
        subparams[sub].setIntParameter(index - offsets[sub], value);
    }

    public void setLiteralParameter(int index, String value, int oid) throws SQLException {
        int sub = findSubParam(index);
        subparams[sub].setStringParameter(index - offsets[sub], value, oid);
    }

    public void setStringParameter(int index, String value, int oid) throws SQLException {
        int sub = findSubParam(index);
        subparams[sub].setStringParameter(index - offsets[sub], value, oid);
    }

    public void setBytea(int index, byte[] data, int offset, int length) throws SQLException {
        int sub = findSubParam(index);
        subparams[sub].setBytea(index - offsets[sub], data, offset, length);
    }

    public void setBytea(int index, InputStream stream, int length) throws SQLException {
        int sub = findSubParam(index);
        subparams[sub].setBytea(index - offsets[sub], stream, length);
    }

    public void setNull(int index, int oid) throws SQLException {
        int sub = findSubParam(index);
        subparams[sub].setNull(index - offsets[sub], oid);
    }

    public String toString(int index) {
        try
        {
            int sub = findSubParam(index);
            return subparams[sub].toString(index - offsets[sub]);
        }
        catch (SQLException e)
        {
            throw new IllegalStateException(e.getMessage());
        }
    }

    public ParameterList copy() {
        SimpleParameterList[] copySub = new SimpleParameterList[subparams.length];
        for (int sub = 0; sub < subparams.length; ++sub)
            copySub[sub] = (SimpleParameterList)subparams[sub].copy();

        return new CompositeParameterList(copySub, offsets);
    }

    public void clear() {
        for (int sub = 0; sub < subparams.length; ++sub)
        {
            subparams[sub].clear();
        }
    }

    public SimpleParameterList[] getSubparams() {
        return subparams;
    }

    public void checkAllParametersSet() throws SQLException {
        for (int sub = 0; sub < subparams.length; ++sub)
            subparams[sub].checkAllParametersSet();
    }

    public void convertFunctionOutParameters()
    {
        for (int sub = 0; sub < subparams.length; ++sub)
            subparams[sub].convertFunctionOutParameters();
    }

    private final int total;
    private final SimpleParameterList[] subparams;
    private final int[] offsets;
}
