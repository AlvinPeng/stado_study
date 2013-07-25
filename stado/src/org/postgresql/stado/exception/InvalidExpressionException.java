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
import org.postgresql.stado.parser.ExpressionType;

/**
 * 
 * 
 */
public class InvalidExpressionException extends XDBServerException {
    /**
     * 
     */
    private static final long serialVersionUID = 3461795006207253787L;

    /**
     * 
     * @param left 
     * @param right 
     * @param Operator 
     */
    public InvalidExpressionException(SqlExpression left, SqlExpression right,
            String Operator) {
        super(ErrorMessageRepository.INVALID_EXPRESSION + " ( "
                + left.rebuildString() + ", " + Operator + " "
                + right.rebuildString() + " )", 0,
                ErrorMessageRepository.INVALID_EXPRESSION_CODE);
    }

    /**
     * 
     * @param leftType 
     * @param operator 
     * @param rightType 
     */
    public InvalidExpressionException(ExpressionType leftType, String operator,
            ExpressionType rightType) {
        super(ErrorMessageRepository.INVALID_EXPRESSION + "( "
                + leftType.getTypeString() + " , " + operator + "  "
                + rightType.getTypeString(), 0,
                ErrorMessageRepository.INVALID_EXPRESSION_CODE);
    }
}
