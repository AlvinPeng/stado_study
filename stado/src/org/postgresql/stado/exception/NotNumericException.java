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
package org.postgresql.stado.exception;

import org.postgresql.stado.optimizer.SqlExpression;

/**
 * 
 */
public class NotNumericException extends XDBServerException {
    /**
     * 
     */
    private static final long serialVersionUID = -3477300270612630357L;

    /**
     * 
     * @param aSqlExpression 
     */
    public NotNumericException(SqlExpression aSqlExpression) {
        super(ErrorMessageRepository.EXPRESSION_NOT_NUMERIC
                + aSqlExpression.rebuildString(), 0,
                ErrorMessageRepository.EXPRESSION_NOT_NUMERIC_CODE);

    }

    /**
     * 
     * @param aParameter 
     * @param function 
     */
    public NotNumericException(SqlExpression aParameter, SqlExpression function) {

        super(ErrorMessageRepository.EXPRESSION_NOT_ALPHANUMERIC + " ( "
                + aParameter.rebuildString() + ", " + function.rebuildString()
                + " )", 0,
                ErrorMessageRepository.EXPRESSION_NOT_ALPHANUMERIC_CODE);

    }

}
