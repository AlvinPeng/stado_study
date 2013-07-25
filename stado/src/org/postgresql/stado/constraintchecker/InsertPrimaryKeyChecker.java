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
 * PrimaryKeyChecker.java
 * 
 *  
 */
package org.postgresql.stado.constraintchecker;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.IExecutable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysIndex;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.parser.Parser;
import org.postgresql.stado.parser.SqlSelect;
import org.postgresql.stado.parser.Tuple;


/**
 *  
 */
public class InsertPrimaryKeyChecker extends AbstractConstraintChecker {
    private static final XLogger logger = XLogger
            .getLogger(InsertPrimaryKeyChecker.class);

    private static final String TABLE_ALIAS1 = "t1";

    private static final String TABLE_ALIAS2 = "t2";

    private Tuple tuple;

    /**
     * 
     * @param targetTable 
     * @param tuple 
     * @param client 
     */

    public InsertPrimaryKeyChecker(SysTable targetTable, Tuple tuple,
            XDBSessionContext client) {
        super(targetTable, client);
        this.tuple = tuple;
    }

    /**
     * Validate with temp table: SELECT 1 FROM target, temp WHERE (target.KEY =
     * temp.KEY) SELECT 1 FROM temp t1, temp t2 WHERE (t1.KEY = t2.KEY and
     * t1.rowid < t2.rowid) Validate with tuple: SELECT 1 FROM target WHERE
     * (target.KEY = <tuple.KEY>)
     * 
     * @param idx 
     * @throws java.lang.Exception 
     * @return 
     */
    @Override
    protected Map<IExecutable, ViolationCriteria> prepareConstraint(Object idx)
            throws Exception {
        ViolationCriteria criteria = new ViolationCriteria();
        criteria.violationType = VIOLATE_IF_NOT_EMPTY;
        criteria.message = idx.toString();
        Map<IExecutable, ViolationCriteria> validateSQL = new HashMap<IExecutable, ViolationCriteria>();
        StringBuffer sbSelect = new StringBuffer("select 1 from ");
        sbSelect.append(targetTable.getTableName()).append(" ").append(
                TABLE_ALIAS1);
        if (tuple == null) {
            sbSelect.append(", ").append(tempTable.getTableName()).append(" ")
                    .append(TABLE_ALIAS2);
        }
        sbSelect.append(" WHERE ");
        StringBuffer sbCondition = new StringBuffer();
        for (Iterator it = ((SysIndex) idx).getKeyColumns().iterator(); it
                .hasNext();) {
            SysColumn col = (SysColumn) it.next();
            sbCondition.append(TABLE_ALIAS1).append(".").append(
                    col.getColName()).append("=");
            if (tuple == null) {
                sbCondition.append(TABLE_ALIAS2).append(".").append(
                        col.getColName());
            } else {
                sbCondition.append(tuple.getValue(col));
            }
            sbCondition.append(" and ");
        }
        sbSelect.append(sbCondition);
        sbSelect.setLength(sbSelect.length() - 5);
        Parser parser = new Parser(client);
        parser.parseStatement(sbSelect.toString());
        SqlSelect select = (SqlSelect) parser.getSqlObject();
        select.addSkipPermissionCheck(targetTable.getTableName());
        select.prepare();
        validateSQL.put(select, criteria);
        if (tuple == null) {
            StringBuffer sbSelfSelect = new StringBuffer("select 1 from ");
            sbSelfSelect.append(tempTable.getTableName()).append(" ").append(
                    TABLE_ALIAS1);
            sbSelfSelect.append(", ").append(tempTable.getTableName()).append(
                    " ").append(TABLE_ALIAS2);
            sbSelfSelect.append(" WHERE ").append(sbCondition);
            sbSelfSelect.append(TABLE_ALIAS1).append(".").append(
                    "Engine.XDROWID_NAME").append("<");
            sbSelfSelect.append(TABLE_ALIAS2).append(".").append(
                    "Engine.XDROWID_NAME");
            parser = new Parser(client);
            parser.parseStatement(sbSelfSelect.toString());
            select = (SqlSelect) parser.getSqlObject();
            select.prepare();
            validateSQL.put(select, criteria);
        }
        return validateSQL;

    }


    /**
     * 
     * @param columnsInvolved 
     * @param keysToCheck 
     * @return 
     * @see org.postgresql.stado.ConstraintChecker.AbstractConstraintChecker#scanConstraints(java.util.Collection,
     *      java.util.Collection)
     */

    @Override
    protected Collection<SysColumn> scanConstraints(
            Collection<SysColumn> columnsInvolved,
            Collection keysToCheck) {
        final String method = "scanConstraints";
        logger.entering(method, new Object[] {});
        try {

            Collection<SysColumn> colsToAdd = new HashSet<SysColumn>();
            Collection<SysIndex> keys = targetTable
                    .getAllUniqueAndPrimarySysIndexes();
            for (Iterator<SysIndex> it = keys.iterator(); it.hasNext();) {
                SysIndex anIndex = it.next();
                if (anIndex.isDistributed) {
                    keysToCheck.add(anIndex);
                    for (Iterator<SysColumn> itcol = anIndex.getKeyColumns()
                            .iterator(); itcol.hasNext();) {
                        SysColumn column = itcol.next();
                        if (!columnsInvolved.contains(column)) {
                            colsToAdd.add(column);
                        }
                    }
                }
            }
            return colsToAdd;

        } finally {
            logger.exiting(method);
        }
    }

	@Override
	public boolean isReadOnly() {
		// TODO Auto-generated method stub
		return false;
	}

}
