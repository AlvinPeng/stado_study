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
package org.postgresql.driver.core.types;

import java.math.BigDecimal;
import java.sql.Types;

import org.postgresql.driver.util.GT;
import org.postgresql.driver.util.PSQLException;
import org.postgresql.driver.util.PSQLState;

/**
 * @author davec
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class PGString implements PGType
{

    String val;
    protected PGString( String x )
    {
        val = x;
    }
    /* (non-Javadoc)
     * @see org.postgresql.types.PGType#castToServerType(int)
     */
    public static PGType castToServerType(String val, int targetType) throws PSQLException
    {
        try
        {
            switch (targetType )
            {
	            case Types.BIT:
	            {
	                if ( val.equalsIgnoreCase("true") || val.equalsIgnoreCase("1") || val.equalsIgnoreCase("t"))
	                    return new PGBoolean( Boolean.TRUE );
	                if ( val.equalsIgnoreCase("false") || val.equalsIgnoreCase("0") || val.equalsIgnoreCase("f"))
	                    return new PGBoolean( Boolean.FALSE);
	            }
	            
	            return new PGBoolean( Boolean.FALSE);
	            
	            case Types.VARCHAR:
	            case Types.LONGVARCHAR:                
	                return new PGString(val);
	            case Types.BIGINT:
	                return new PGLong( new Long(Long.parseLong( val )));
	            case Types.INTEGER:
	                return new PGInteger( new Integer(Integer.parseInt( val )));
	            case Types.TINYINT:
	                return new PGShort( new Short( Short.parseShort( val )));
	            case Types.FLOAT:
	            case Types.DOUBLE:
	                return new PGDouble( new Double(Double.parseDouble( val )));
	            case Types.REAL:
	                return new PGFloat( new Float( Float.parseFloat( val )));
	            case Types.NUMERIC:
	            case Types.DECIMAL:
	                return new PGBigDecimal( new BigDecimal( val));
	            default:
	                return new PGUnknown( val );
	            
	            }
        }
        catch( Exception ex )
        {
            throw new PSQLException(GT.tr("Cannot convert an instance of {0} to type {1}", new Object[]{val.getClass().getName(),"Types.OTHER"}), PSQLState.INVALID_PARAMETER_TYPE, ex);
        }
    }
    public String toString()
    {
        return val;
    }

}
