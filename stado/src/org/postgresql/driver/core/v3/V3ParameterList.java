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

import java.sql.SQLException;

import org.postgresql.driver.core.ParameterList;

/**
 * Common interface for all V3 parameter list implementations.
 * 
 * @author Oliver Jowett (oliver@opencloud.com)
 */
interface V3ParameterList extends ParameterList {
    /**
     * Ensure that all parameters in this list have been
     * assigned values. Return silently if all is well, otherwise
     * throw an appropriate exception.
     *
     * @throws SQLException if not all parameters are set.
     */
    void checkAllParametersSet() throws SQLException;

    /**
     * Convert any function output parameters to the correct type (void)
     * and set an ignorable value for it.
     */
    void convertFunctionOutParameters();

    /**
     * Return a list of the SimpleParameterList objects that
     * make up this parameter list. If this object is already a
     * SimpleParameterList, returns null (avoids an extra array
     * construction in the common case).
     *
     * @return an array of single-statement parameter lists, or
     *   <code>null</code> if this object is already a single-statement
     *   parameter list.
     */
    SimpleParameterList[] getSubparams();
    
   
    
}
