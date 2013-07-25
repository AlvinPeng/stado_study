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


/**
 * 
 */
public class UpdateForeignKeyChecker extends AbstractConstraintChecker {
    private static final XLogger logger = XLogger
            .getLogger(UpdateForeignKeyChecker.class);

    /**
     * @param targetTable
     * @param client
     */
    public UpdateForeignKeyChecker(SysTable targetTable,
            XDBSessionContext client) {
        super(targetTable, client);
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
                    boolean touched = false;
                    Collection<SysColumn> refKeys = new HashSet<SysColumn>();
                    Collection fks = reference.getForeignKeys();
                    for (Iterator iter = fks.iterator(); iter.hasNext();) {
                        SysForeignKey fk = (SysForeignKey) iter.next();
                        SysColumn col = targetTable.getSysColumn(fk.getColid());
                        if (columnsInvolved.contains(col)) {
                            touched = true;
                        } else {
                            refKeys.add(col);
                        }
                    }
                    if (touched) {
                        keysToCheck.add(reference);
                        colsToAdd.addAll(refKeys);
                    }
                }
            }
            return colsToAdd;

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * SELECT 1 FROM <temp> LEFT JOIN <foreign> ON <temp>.<key>_new =
     * <foreign>.<key> WHERE <foreign>.<rowid> = null
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
            criteria.violationType = VIOLATE_IF_NOT_EMPTY;
            criteria.message = ((SysReference) constraint).getConstraint()
                    .toString();
            SysTable foreignTable = targetTable.getSysDatabase().getSysTable(
                    ((SysReference) constraint).getRefTableID());
            StringBuffer sbSelect = new StringBuffer("SELECT 1 FROM ");
            sbSelect.append(tempTable.getTableName()).append(" LEFT JOIN ");
            sbSelect.append(foreignTable.getTableName()).append(" ON ");
            Collection fks = ((SysReference) constraint).getForeignKeys();
            for (Iterator iter = fks.iterator(); iter.hasNext();) {
                SysForeignKey fk = (SysForeignKey) iter.next();
                SysColumn col = targetTable.getSysColumn(fk.getColid());
                SysColumn refCol = foreignTable.getSysColumn(fk.getRefcolid());
                sbSelect.append(tempTable.getTableName()).append(".");
                sbSelect.append(col.getColName()).append("_new=");
                sbSelect.append(foreignTable.getTableName()).append(".");
                sbSelect.append(refCol.getColName()).append(" AND ");
            }
            sbSelect.setLength(sbSelect.length() - 5);
            sbSelect.append(" WHERE ");
            for (Iterator<SysColumn> it = foreignTable.getRowID().iterator(); it
                    .hasNext();) {
                SysColumn column = it.next();
                sbSelect.append(foreignTable.getTableName()).append(".")
                        .append(column.getColName()).append(" IS NULL AND ");
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
