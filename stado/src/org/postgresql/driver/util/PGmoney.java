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
package org.postgresql.driver.util;


import java.io.Serializable;
import java.sql.SQLException;

/**
 * This implements a class that handles the PostgreSQL money and cash types
 */
public class PGmoney extends PGobject implements Serializable, Cloneable
{
    /*
     * The value of the field
     */
    public double val;

    /*
     * @param value of field
     */
    public PGmoney(double value)
    {
        this();
        val = value;
    }

    public PGmoney(String value) throws SQLException
    {
        this();
        setValue(value);
    }

    /*
     * Required by the driver
     */
    public PGmoney()
    {
        setType("money");
    }

    public void setValue(String s) throws SQLException
    {
        try
        {
            String s1;
            boolean negative;

            negative = (s.charAt(0) == '(') ;

            // Remove any () (for negative) & currency symbol
            s1 = PGtokenizer.removePara(s).substring(1);

            // Strip out any , in currency
            int pos = s1.indexOf(',');
            while (pos != -1)
            {
                s1 = s1.substring(0, pos) + s1.substring(pos + 1);
                pos = s1.indexOf(',');
            }

            val = Double.valueOf(s1).doubleValue();
            val = negative ? -val : val;

        }
        catch (NumberFormatException e)
        {
            throw new PSQLException(GT.tr("Conversion of money failed."), PSQLState.NUMERIC_CONSTANT_OUT_OF_RANGE, e);
        }
    }

    public boolean equals(Object obj)
    {
        if (obj instanceof PGmoney)
        {
            PGmoney p = (PGmoney)obj;
            return val == p.val;
        }
        return false;
    }

    public String getValue()
    {
        if (val < 0)
        {
            return "-$" + ( -val);
        }
        else
        {
            return "$" + val;
        }
    }
}
