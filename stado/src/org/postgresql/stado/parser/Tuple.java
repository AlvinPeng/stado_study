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
package org.postgresql.stado.parser;

import java.sql.Types;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.optimizer.SqlExpression;


/**
 * This class is used to by Insert Table Command, where we create a tuple to
 * insert This should be useful when we support bulk inserts
 */
public class Tuple {
    private XDBSessionContext client;

    private List<SysColumn> columns;

    /**
     *
     * @return returns the aSysTable to which this particular tuple belongs
     */
    public SysTable getaSysTable() {
        return aSysTable;
    }

    // Information Classes and data
    private SysTable aSysTable;

    private LinkedHashMap<SysColumn, SqlExpression> mapColumnValueList = new LinkedHashMap<SysColumn, SqlExpression>();

    /**
     * Constructor
     *
     * @param tableName
     *            The Table into ehich
     * @param columnNameList
     *            contains the column name list
     * @param valueList
     *            Contains the list of values which will be inserted in the
     *            table specified
     * @throws IllegalArgumentException -
     *             The function checks for all the column names and finds out if
     *             it exists in the metadata table, incase we are not able to
     *             find it we throw an exception
     */
    public Tuple(String tableName, List<SysColumn> columnList,
            List<SqlExpression> valueList, XDBSessionContext client) {
        this.client = client;
        this.columns = columnList;
        aSysTable = client.getSysDatabase().getSysTable(getTableName(tableName));

        for (int i = 0; i < valueList.size(); i++) {
            SqlExpression value = valueList.get(i);
            if (value.getExprType() == SqlExpression.SQLEX_PARAMETER) {
                value.setExprDataType(value.setExpressionResultType(value,
                        columnList.get(i)));
            }
            SysColumn column = columnList.get(i);
            if (value.getExprDataType() == null) {
                value.setExprDataType(new ExpressionType(column));
            }
            mapColumnValueList.put(column, value);
        }
    }

    public String getValue(String columnName) {
        return getValue(aSysTable.getSysColumn(columnName));
    }

    public String getValue(SysColumn column) {
        SqlExpression theResult = getExpression(column);
        if (theResult != null
                && theResult.getExprType() == SqlExpression.SQLEX_CONSTANT
                && theResult.getConstantValue() != null) {
            int aType = column.getColType();
            try {
                if (aType == Types.DATE) {
                    theResult.setConstantValue(SqlExpression.normalizeDate(theResult.getConstantValue()));
                } else if (aType == Types.TIME) {
                    theResult.setConstantValue(SqlExpression.normalizeTime(theResult.getConstantValue()));
                }
            } catch (Exception ex) {
                // fallback
                aType = Types.TIMESTAMP;
            }
            if (aType == Types.TIMESTAMP) {
                theResult.setConstantValue(SqlExpression.normalizeTimeStamp(theResult.getConstantValue()));
            }
        }
        return theResult == null ? null : theResult.rebuildString(client);
    }

    public SqlExpression getExpression(String columnName) {
        return getExpression(aSysTable.getSysColumn(columnName));
    }

    public SqlExpression getExpression(SysColumn column) {
        SqlExpression expr = mapColumnValueList.get(column);
        if (expr == null) {
            try {
                expr = column.getDefaultExpr(client);
                mapColumnValueList.put(column, expr);
            } catch (Exception e) {
                // TODO handle
            }
        }
        return expr;
    }

    public String getTableName(String tableName) {
        return tableName;
    }

    /**
     * Forget all values that was calculated, retain only original values, to
     * force recalculation.
     */
    public void reset() {
        Iterator<SysColumn> it = mapColumnValueList.keySet().iterator();
        while (it.hasNext()) {
            if (!columns.contains(it.next())) {
                it.remove();
            }

        }
    }
}
