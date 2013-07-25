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
 * DeleteForeignReferenceChecker.java
 * 
 *  
 */
package org.postgresql.stado.constraintchecker;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.IExecutable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysConstraint;
import org.postgresql.stado.metadata.SysForeignKey;
import org.postgresql.stado.metadata.SysReference;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.optimizer.QueryCondition;
import org.postgresql.stado.parser.Parser;
import org.postgresql.stado.parser.SqlSelect;


/**
 *  
 */
public class DeleteForeignReferenceChecker extends AbstractConstraintChecker {
    private static final XLogger logger = XLogger
            .getLogger(DeleteForeignReferenceChecker.class);

    private static final String TABLE_ALIAS1 = "t1";

    private static final String TABLE_ALIAS2 = "t2";

    private QueryCondition whereClause;

    /**
     * @param targetTable
     * @param client
     */
    public DeleteForeignReferenceChecker(SysTable targetTable,
            QueryCondition whereClause,
            XDBSessionContext client) {
        super(targetTable, client);
        this.whereClause = whereClause;
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
        logger.entering(method, new Object[] { columnsInvolved, keysToCheck });
        try {

            Collection<SysColumn> colsToAdd = new HashSet<SysColumn>();
            Collection sysFks = targetTable.getSysReferences();
            for (Iterator it = sysFks.iterator(); it.hasNext();) {
                SysReference reference = (SysReference) it.next();
                if (reference.getDistributedCheck()) {
                    Collection fks = reference.getForeignKeys();
                    for (Iterator iter = fks.iterator(); iter.hasNext();) {
                        SysForeignKey fk = (SysForeignKey) iter.next();
                        SysColumn col = targetTable.getSysColumn(fk
                                .getRefcolid());
                        if (!columnsInvolved.contains(col)) {
                            colsToAdd.add(col);
                        }
                    }
                    keysToCheck.add(reference);
                }
            }
            return colsToAdd;

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * With temp table: SELECT 1 FROM <temp> t1 JOIN <foreign> t2 ON t1.<key> =
     * t2.<key> Without temp table: SELECT 1 FROM (SELECT <key> FROM <target>
     * [WHERE <condition>]) t1 JOIN <foreign> t2 ON t1.<key> = t2.<key>
     * 
     * @see org.postgresql.stado.constraintchecker.AbstractConstraintChecker#prepareConstraint(java.lang.Object)
     */
    @Override
    protected Map<IExecutable, ViolationCriteria> prepareConstraint(
            Object constraint) throws Exception {
        final String method = "prepareConstraint";
        logger.entering(method, new Object[] {});
        try {

            SysConstraint sysConstraint = ((SysReference) constraint)
                    .getConstraint();
            ViolationCriteria criteria = new ViolationCriteria();
            criteria.violationType = VIOLATE_IF_NOT_EMPTY;
            criteria.message = sysConstraint.toString();
            SysTable foreignTable = sysConstraint.getSysTable();
            StringBuffer sbSelect = new StringBuffer("SELECT 1 FROM ");
            if (tempTable == null) {
                sbSelect.append("(SELECT ");
                Collection fks = ((SysReference) constraint).getForeignKeys();
                for (Iterator iter = fks.iterator(); iter.hasNext();) {
                    SysForeignKey fk = (SysForeignKey) iter.next();
                    SysColumn refCol = targetTable.getSysColumn(fk
                            .getRefcolid());
                    sbSelect.append(refCol.getColName()).append(", ");
                }
                sbSelect.setLength(sbSelect.length() - 2);
                sbSelect.append(" FROM ").append(targetTable.getTableName());
                if (whereClause != null) {
                    sbSelect.append(" WHERE ").append(
                            whereClause.rebuildString());
                }
                sbSelect.append(") ").append(TABLE_ALIAS1);

            } else {
                sbSelect.append(tempTable.getTableName());
                sbSelect.append(" ").append(TABLE_ALIAS1);
            }
            sbSelect.append(" JOIN ").append(foreignTable.getTableName());
            sbSelect.append(" ").append(TABLE_ALIAS2);
            sbSelect.append(" ON ");
            Collection fks = ((SysReference) constraint).getForeignKeys();
            for (Iterator iter = fks.iterator(); iter.hasNext();) {
                SysForeignKey fk = (SysForeignKey) iter.next();
                SysColumn col = foreignTable.getSysColumn(fk.getColid());
                SysColumn refCol = targetTable.getSysColumn(fk.getRefcolid());
                sbSelect.append(TABLE_ALIAS1).append(".").append(
                        refCol.getColName()).append("=");
                sbSelect.append(TABLE_ALIAS2).append(".").append(
                        col.getColName()).append(" AND ");
            }
            sbSelect.setLength(sbSelect.length() - 5);
            Parser parser = new Parser(client);
            parser.parseStatement(sbSelect.toString());
            SqlSelect select = (SqlSelect) parser.getSqlObject();
            select.addSkipPermissionCheck(targetTable.getTableName());
            select.addSkipPermissionCheck(foreignTable.getTableName());
            select.prepare();
            return Collections.singletonMap((IExecutable) select, criteria);

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
