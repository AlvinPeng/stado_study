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
 * InsertForeignKeyChecker.java
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
import org.postgresql.stado.metadata.SysForeignKey;
import org.postgresql.stado.metadata.SysReference;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.parser.Parser;
import org.postgresql.stado.parser.SqlSelect;
import org.postgresql.stado.parser.Tuple;


/**
 *  
 */
public class InsertForeignKeyChecker extends AbstractConstraintChecker {
    private static final XLogger logger = XLogger
            .getLogger(InsertForeignKeyChecker.class);

    private Tuple tuple;

    /**
     * 
     * @param tuple 
     * @param targetTable 
     * @param client 
     */
    public InsertForeignKeyChecker(SysTable targetTable, Tuple tuple,
            XDBSessionContext client) {
        super(targetTable, client);
        this.tuple = tuple;
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
            Collection sysFks = targetTable.getSysFkReferenceList();
            for (Iterator it = sysFks.iterator(); it.hasNext();) {
                SysReference reference = (SysReference) it.next();
                if (reference.getDistributedCheck()) {
                    Collection fks = reference.getForeignKeys();
                    for (Iterator iter = fks.iterator(); iter.hasNext();) {
                        SysForeignKey fk = (SysForeignKey) iter.next();
                        SysColumn col = targetTable.getSysColumn(fk.getColid());
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
     * WIth temp table SELECT 1 FROM <temp> LEFT JOIN <foreign> ON (<temp>.<key_col1> = ,
     * <foreign>.<key_col1>, <temp>.<key_col2> = , <foreign>.<key_col2>, ...,
     * <temp>.<key_colN> = , <foreign>.<key_colN>) WHERE <foreign>.xrowid =
     * null With Tuple (reversed violate condition) SELECT 1 FROM <foreign>
     * WHERE <key_col1> = <tuple_value1> AND <key_col2> = <tuple_value2> AND ...
     * AND <key_colN> = <tuple_valueN>
     * 
     * @see org.postgresql.stado.constraintchecker.AbstractConstraintChecker#prepareConstraint(java.lang.Object)
     * @param constraint 
     * @throws java.lang.Exception 
     * @return 
     */
    @Override
    protected Map<IExecutable, ViolationCriteria> prepareConstraint(
            Object constraint) throws Exception {
        final String method = "prepareConstraint";
        logger.entering(method, new Object[] {});
        try {

            ViolationCriteria criteria = new ViolationCriteria();
            criteria.message = ((SysReference) constraint).getConstraint()
                    .toString();
            StringBuffer sbSelect = new StringBuffer("SELECT 1 FROM ");
            SysTable foreignTable = targetTable.getSysDatabase().getSysTable(
                    ((SysReference) constraint).getRefTableID());
            if (tuple == null) {
                criteria.violationType = VIOLATE_IF_NOT_EMPTY;
                sbSelect.append(tempTable.getTableName()).append(" LEFT JOIN ");
                sbSelect.append(foreignTable.getTableName()).append(" ON (");
                StringBuffer sbColumns = new StringBuffer();
                Collection fks = ((SysReference) constraint).getForeignKeys();
                for (Iterator iter = fks.iterator(); iter.hasNext();) {
                    SysForeignKey fk = (SysForeignKey) iter.next();
                    SysColumn col = targetTable.getSysColumn(fk.getColid());
                    SysColumn refCol = foreignTable.getSysColumn(fk
                            .getRefcolid());
                    sbSelect.append(tempTable.getTableName()).append(".");
                    sbSelect.append(col.getColName()).append("=");
                    sbSelect.append(foreignTable.getTableName()).append(".");
                    sbSelect.append(refCol.getColName()).append(", ");
                }
                sbSelect.append(sbColumns.substring(0, sbColumns.length() - 2));
                sbSelect.append(") WHERE ");
                for (Iterator<SysColumn> it = targetTable.getRowID().iterator(); it
                        .hasNext();) {
                    SysColumn column = it.next();
                    sbSelect.append(foreignTable.getTableName()).append(".")
                            .append(column.getColName())
                            .append(" IS NULL AND ");
                }
                sbSelect.setLength(sbSelect.length() - 5);
            } else {
                criteria.violationType = VIOLATE_IF_EMPTY;
                sbSelect.append(foreignTable.getTableName());
                sbSelect.append(" WHERE ");
                Collection fks = ((SysReference) constraint).getForeignKeys();
                for (Iterator iter = fks.iterator(); iter.hasNext();) {
                    SysForeignKey fk = (SysForeignKey) iter.next();
                    SysColumn col = targetTable.getSysColumn(fk.getColid());
                    SysColumn refCol = foreignTable.getSysColumn(fk
                            .getRefcolid());
                    sbSelect.append(refCol.getColName());
                    String value = tuple.getValue(col);
                    if ("null".equalsIgnoreCase(value)) {
                        sbSelect.append(" IS NULL AND ");
                    } else {
                        sbSelect.append("=").append(value).append(" AND ");
                    }
                }
                sbSelect.setLength(sbSelect.length() - 5);
            }
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
