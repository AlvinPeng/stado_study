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
 * SqlAlterDropConstraint.java
 *
 *
 */

package org.postgresql.stado.parser;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;

import org.postgresql.stado.common.util.ParseCmdLine;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.MetaData;
import org.postgresql.stado.metadata.SyncAlterTableDropConstraint;
import org.postgresql.stado.metadata.SysConstraint;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysIndex;
import org.postgresql.stado.metadata.SysPermission;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.parser.core.syntaxtree.Constraint;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.IdentifierHandler;


/**
 *
 *
 */
public class SqlAlterDropConstraint extends DepthFirstVoidArguVisitor implements
        IXDBSql, IPreparable {
    private static final XLogger logger = XLogger
            .getLogger(SqlAlterAddColumn.class);

    private XDBSessionContext client;

    private SysDatabase database;

    private SqlAlterTable parent;

    private String constraintName;

    private String[] commands = null;

    private SysIndex indexToDrop;

    // if we drop reference
    private int refId; // the id of the references to drop

    private String refTableName = null;

    /**
     * @param table
     * @param client
     */
    public SqlAlterDropConstraint(SqlAlterTable parent, XDBSessionContext client) {
        this.client = client;
        database = client.getSysDatabase();
        this.parent = parent;
    }

    /**
     * Grammar production:
     * f0 -> <CONSTRAINT_>
     * f1 -> Identifier(prn)
     */
    @Override
    public void visit(Constraint n, Object argu) {
        constraintName = (String) n.f1.accept(new IdentifierHandler(), argu);
    }

    /**
     * @return
     */
    public SqlAlterTable getParent() {
        return parent;
    }

    public String getConstraintName() {
        return constraintName;
    }

    /**
     * @return the refId
     */
    public int getRefId() {
        return refId;
    }

    /**
     * @return the refId
     */
    public String getRefTableName() {
        return refTableName;
    }

    public int getIndexIdToDrop() {
        return indexToDrop == null ? -1 : indexToDrop.idxid;
    }

    public LockSpecification<SysTable> getLockSpecs() {
        SysTable referedTable = null;
        if (refTableName != null) {
            referedTable = database.getSysTable(refTableName);
        }
        Collection<SysTable> noTables = Collections.emptySet();
        LockSpecification<SysTable> aLSpec = new LockSpecification<SysTable>(
                referedTable == null ? noTables : Collections
                        .singleton(referedTable), noTables);
        return aLSpec;
    }

    public Collection<DBNode> getNodeList() {
        SysTable referedTable = null;
        if (refTableName != null) {
            referedTable = database.getSysTable(refTableName);
        }
        Collection<DBNode> result;
        if (referedTable == null) {
            result = Collections.emptySet();
        } else {
            result = new ArrayList<DBNode>(referedTable.getNodeList());
        }
        return result;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#getCost()
     */
    public long getCost() {
        return LOW_COST;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IPreparable#isPrepared()
     */
    public boolean isPrepared() {
        return commands != null;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IPreparable#prepare()
     */
    public void prepare() throws Exception {
        final String method = "prepare";
        logger.entering(method, new Object[] {});
        try {

            LinkedList<String> comm = new LinkedList<String>();
            // make sure that the constraint exists on the table
            SysTable table = parent.getTable();
            table.ensurePermission(client.getCurrentUser(),
                    SysPermission.PRIVILEGE_ALTER);
            SysConstraint theConstraint = table.getConstraint(constraintName);
            if (theConstraint == null) {
                throw new XDBServerException("Constraint " + constraintName
                        + " is not found in table " + table.getTableName());
            }
            if (theConstraint.getSysTable() != table) {
                throw new XDBServerException(
                        "Can not drop inherited constraint " + constraintName);
            }
            // index to drop
            indexToDrop = table.getSysIndex(theConstraint.getIdxID());

            if (indexToDrop != null && indexToDrop.issyscreated == 0) {
                indexToDrop = null; // do not drop a non syscreated index
            }

            // We must get sysreferences to drop as well as the tableName
            // refered.
            // No Simple way to do this from the MetaData cache
            // Arrrggghhhh!! do a db lookup
            try {
                ResultSet rs = MetaData
                        .getMetaData()
                        .executeQuery(
                                "SELECT refid, tablename from xsysreferences, xsystables WHERE "
                                        + " xsysreferences.constid="
                                        + theConstraint.getConstID()
                                        + " AND xsysreferences.Reftableid = xsystables.tableid ");

                if (rs.next()) {
                    refId = rs.getInt(1);
                    refTableName = rs.getString(2).trim();
                }
            } catch (SQLException e) {
                throw new XDBServerException(
                        e.getMessage(), e,
                        ErrorMessageRepository.SQL_EXEC_FAILURE_CODE);
            }
            HashMap<String, String> arguments = new HashMap<String, String>();
            arguments.put("table", IdentifierHandler.quote(table.getTableName()));
            arguments.put("constr_name", IdentifierHandler.quote(constraintName));

            if (theConstraint.getIsSoft() == 0) {
                String sql;
                boolean toParent;
                switch (theConstraint.getConstType()) {
                case 'P':
                    sql = Props.XDB_SQLCOMMAND_ALTERTABLE_DROPCONSTRAINT_PRIMARY;
                    toParent = Props.XDB_SQLCOMMAND_ALTERTABLE_DROPCONSTRAINT_PRIMARY_TO_PARENT;
                    break;
                case 'U':
                    sql = Props.XDB_SQLCOMMAND_ALTERTABLE_DROPCONSTRAINT_UNIQUE;
                    toParent = Props.XDB_SQLCOMMAND_ALTERTABLE_DROPCONSTRAINT_UNIQUE_TO_PARENT;
                    break;
                case 'R':
                    sql = Props.XDB_SQLCOMMAND_ALTERTABLE_DROPCONSTRAINT_REFERENCE;
                    toParent = Props.XDB_SQLCOMMAND_ALTERTABLE_DROPCONSTRAINT_REFERENCE_TO_PARENT;
                    break;
                case 'C':
                    sql = Props.XDB_SQLCOMMAND_ALTERTABLE_DROPCONSTRAINT_CHECK;
                    toParent = Props.XDB_SQLCOMMAND_ALTERTABLE_DROPCONSTRAINT_CHECK_TO_PARENT;
                    break;
                default:
                    sql = Props.XDB_SQLCOMMAND_ALTERTABLE_DROPCONSTRAINT;
                    toParent = Props.XDB_SQLCOMMAND_ALTERTABLE_DROPCONSTRAINT_TO_PARENT;
                }

                sql = ParseCmdLine.substitute(sql, arguments);
                if (sql.length() > 0) {
                    if (toParent) {
                        parent.addCommonCommand(sql);
                    } else {
                        comm.add(sql);
                    }
                }
            }

            if (indexToDrop != null
                    && Props.XDB_SQLCOMMAND_ALTERTABLE_DROP_INDEX_AFTER_DROP_CONSTRAINT) {
                // build sql commands for droping the index
                arguments.put("index_list", IdentifierHandler.quote(indexToDrop.idxname));
                String sql = ParseCmdLine.substitute(
                        Props.XDB_SQLCOMMAND_DROP_INDEX, arguments);
                comm.add(sql);
            }
            commands = comm.toArray(new String[comm.size()]);

        } finally {
            logger.exiting(method);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IExecutable#execute(org.postgresql.stado.Engine.Engine)
     */
    public ExecutionResult execute(Engine engine) throws Exception {
        final String method = "execute";
        logger.entering(method, new Object[] {});
        try {

            engine.executeDDLOnMultipleNodes(commands, parent.getNodeList(),
                    new SyncAlterTableDropConstraint(this), client);
            return null;

        } finally {
            logger.exiting(method);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#needCoordinatorConnection()
     */
    public boolean needCoordinatorConnection() {
        return false;
    }

	@Override
	public boolean isReadOnly() {
		// TODO Auto-generated method stub
		return false;
	}

}
