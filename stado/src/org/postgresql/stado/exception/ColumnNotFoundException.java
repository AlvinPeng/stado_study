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
 * PM 
 */
public class ColumnNotFoundException extends XDBServerException {
    /**
     * 
     */
    private static final long serialVersionUID = 5041676296080719907L;

    String columnName, tableName;

    SqlExpression aAttributeColumnExpression;

    public ColumnNotFoundException(SqlExpression attributeColumnExpression) {
        super(ErrorMessageRepository.UNKOWN_COLUMN_NAME,
                XDBServerException.SEVERITY_LOW,
                ErrorMessageRepository.UNKOWN_COLUMN_NAME_CODE);
        aAttributeColumnExpression = attributeColumnExpression;
        columnName = aAttributeColumnExpression.getColumn().columnName;
        tableName = aAttributeColumnExpression.getColumn().getTableAlias();
    }

    /**
     * 
     * @param columnName 
     * @param tableName 
     */
    public ColumnNotFoundException(String columnName, String tableName) {
        super(ErrorMessageRepository.UNKOWN_COLUMN_NAME,
                XDBServerException.SEVERITY_LOW,
                ErrorMessageRepository.UNKOWN_COLUMN_NAME_CODE);
        this.columnName = columnName;
        this.tableName = tableName;
    }

    /**
     * 
     * @return 
     */
    @Override
    public String getMessage() {
        return super.getMessage() + "(" + columnName + ")";
    }

    /**
     * 
     * @return 
     */
    public SqlExpression getColumnExpression() {
        return aAttributeColumnExpression;
    }

}
