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

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IExecutable;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.scheduler.ILockCost;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.parser.core.syntaxtree.AddDef;
import org.postgresql.stado.parser.core.syntaxtree.AlterDefOperation;
import org.postgresql.stado.parser.core.syntaxtree.AlterTable;
import org.postgresql.stado.parser.core.syntaxtree.DropDef;
import org.postgresql.stado.parser.core.syntaxtree.Inherit;
import org.postgresql.stado.parser.core.syntaxtree.NodeChoice;
import org.postgresql.stado.parser.core.syntaxtree.NodeSequence;
import org.postgresql.stado.parser.core.syntaxtree.OwnerDef;
import org.postgresql.stado.parser.core.syntaxtree.RenameDef;
import org.postgresql.stado.parser.core.syntaxtree.SetTablespace;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.IdentifierHandler;
import org.postgresql.stado.parser.handler.QueryTreeTracker;
import org.postgresql.stado.parser.handler.TableNameHandler;


/**
 *
 * PM
 */

public class SqlAlterTable extends DepthFirstVoidArguVisitor implements IXDBSql,
        IPreparable {
    private static final XLogger logger = XLogger
            .getLogger(SqlAlterTable.class);

    private XDBSessionContext client;

    private SysDatabase database;

    private String tableName;

    private SysTable table;

    private Command commandToExecute;

    // We support only single alter definition, but many
    // other
    // DBMS vendors support list of them, so putting List here for future
    // compatibility
    private List<Object> alterDefs = new LinkedList<Object>();

    private String commonCommand;

    private boolean prepared = false;

    public SqlAlterTable(XDBSessionContext client) {
        this.client = client;
        database = client.getSysDatabase();
        commandToExecute = new Command(Command.CREATE, this,
                new QueryTreeTracker(), client);
    }

    /**
     * Grammar production: f0 -> <TABLE_> f1 -> TableName(prn) f2 ->
     * AlterTableActon(prn) f3 -> ( "," AlterTableActon(prn) )*
     */
    @Override
    public void visit(AlterTable n, Object argu) {
        n.f0.accept(this, argu);

        TableNameHandler aTableNameHandler = new TableNameHandler(client);
        n.f1.accept(aTableNameHandler, argu);
        tableName = aTableNameHandler.getTableName();
        n.f2.accept(this, argu);
        n.f3.accept(this, argu);
    }

    /**
     * Grammar production:
     * f0 -> <SET_>
     * f1 -> <TABLESPACE_>
     * f2 -> Identifier(prn)
     */
    @Override
    public void visit(SetTablespace n, Object argu) {
        SqlAlterSetTablespace theAlterSetTablespace = new SqlAlterSetTablespace(
                this, client);
        alterDefs.add(theAlterSetTablespace);
        n.accept(theAlterSetTablespace, argu);
    }

    /**
     * Grammar production:
     * f0 -> <OWNER_TO_>
     * f1 -> ( <PUBLIC_> | Identifier(prn) )
     */
    @Override
    public void visit(OwnerDef n, Object argu) {
        SqlAlterOwner theAlterOwner = new SqlAlterOwner(this, client);
        alterDefs.add(theAlterOwner);
        n.accept(theAlterOwner, argu);
    }

    /**
     * Grammar production:
     * f0 -> <ADD_>
     * f1 -> ( [ <COLUMN_> ] ColumnDeclare(prn) [ <FIRST_> | <AFTER_> Identifier(prn) ] | [ Constraint(prn) ] ( PrimaryKeyDef(prn) | ForeignKeyDef(prn) | CheckDef(prn) ) )
     */
    @Override
    public void visit(AddDef n, Object argu) {
        switch (n.f1.which) {
        case 0:
            SqlAlterAddColumn addColumn = new SqlAlterAddColumn(this, client);
            alterDefs.add(addColumn);
            n.f1.accept(addColumn, argu);
            break;
        case 1:
            NodeSequence seq = (NodeSequence) n.f1.choice;
            NodeChoice choice = (NodeChoice) seq.nodes.get(1);
            switch (choice.which) {
            case 0:
                SqlAlterAddPrimary addPrimary = new SqlAlterAddPrimary(this,
                        client);
                alterDefs.add(addPrimary);
                n.f1.accept(addPrimary, argu);
                break;
            case 1:
                SqlAlterAddForeignKey addForeign = new SqlAlterAddForeignKey(
                        this, client);
                alterDefs.add(addForeign);
                n.f1.accept(addForeign, argu);
                break;
            case 2:
                SqlAlterAddCheck addCheck = new SqlAlterAddCheck(this, client);
                alterDefs.add(addCheck);
                n.f1.accept(addCheck, argu);
                break;
            }
            break;
        }
    }

    /**
     * Grammar production:
     * f0 -> <DROP_>
     * f1 -> ( [ <COLUMN_> ] Identifier(prn) | Constraint(prn) | <PRIMARYKEY_> )
     */
    @Override
    public void visit(DropDef n, Object argu) {
        switch (n.f1.which) {
        case 0:
            SqlAlterDropColumn dropColumn = new SqlAlterDropColumn(this, client);
            alterDefs.add(dropColumn);
            n.f1.accept(dropColumn, argu);
            break;
        case 1:
            SqlAlterDropConstraint dropConstraint = new SqlAlterDropConstraint(
                    this, client);
            alterDefs.add(dropConstraint);
            n.f1.accept(dropConstraint, argu);
            break;
        case 2:
            SqlAlterDropPrimarykey dropPrimary = new SqlAlterDropPrimarykey(
                    this, client);
            alterDefs.add(dropPrimary);
            n.f1.accept(dropPrimary, argu);
            break;
        }
    }

    /**
     * Grammar production:
     * f0 -> [ <NO_> ]
     * f1 -> <INHERIT_>
     * f2 -> TableName(prn)
     */
    @Override
    public void visit(Inherit n, Object argu) {
        SqlAlterInherit inherit = new SqlAlterInherit(this, client);
        alterDefs.add(inherit);
        n.accept(inherit, argu);
    }

    public void addCommonCommand(String commonCommand) {
        if (this.commonCommand == null) {
            this.commonCommand = "ALTER TABLE " + IdentifierHandler.quote(tableName)
                + " " + commonCommand;
        } else {
            this.commonCommand = this.commonCommand + ", " + commonCommand;
        }
    }

    /**
     * Grammar production:
     * f0 -> Identifier(prn)
     * f1 -> ( AlterDefOperationType(prn) | AlterDefOperationSet(prn) | DropDefaultNotNull(prn) )
     */
    @Override
    public void visit(AlterDefOperation n, Object argu) {
        SqlAlterModifyColumn aSqlAlterModifyColumn = new SqlAlterModifyColumn(
                this, (String) n.f0.accept(new IdentifierHandler(), argu), client);
        n.f1.accept(aSqlAlterModifyColumn, null);
        alterDefs.add(aSqlAlterModifyColumn);
    }

    /**
     * Grammar production:
     * f0 -> <RENAME_>
     * f1 -> [ <COLUMN_> ]
     * f2 -> Identifier(prn)
     * f3 -> <TO_>
     * f4 -> Identifier(prn)
     */
    @Override
    public void visit(RenameDef n, Object argu) {
        Object _ret = null;
        SqlAlterRenameColumn aSqlAlterRenameColumn = new SqlAlterRenameColumn(
                this, client);
        n.accept(aSqlAlterRenameColumn, argu);
        alterDefs.add(aSqlAlterRenameColumn);
    }

    public Collection<DBNode> getNodeList() {
        HashSet<DBNode> nodes = new HashSet<DBNode>();
        nodes.addAll(table.getNodeList());
        for (Object alterDef : alterDefs) {
            if (alterDef instanceof IXDBSql) {
                nodes.addAll(((IXDBSql) alterDef).getNodeList());
            }
        }
        return nodes;
    }

    public String getTableName() {
        return tableName;
    }

    public SysTable getTable() {
        if (table == null) {
            table = database.getSysTable(tableName);
        }
        return table;
    }

    /**
     * This will return the cost of executing this statement in time , milli
     * seconds
     */
    public long getCost() {
        return LOW_COST;
    }

    /**
     * This return the Lock Specification for the system
     *
     * @param theMData
     * @return
     */
    public LockSpecification<SysTable> getLockSpecs() {
        Vector<SysTable> readObjects = new Vector<SysTable>();
        Vector<SysTable> writeObjects = new Vector<SysTable>();
        writeObjects.add(table);
        LockSpecification<SysTable> aLspec = new LockSpecification<SysTable>(
                readObjects, writeObjects);
        for (Object alterDef : alterDefs) {
            if (alterDef instanceof ILockCost) {
                aLspec.addAll(((ILockCost) alterDef).getLockSpecs());
            }
        }
        return aLspec;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IPreparable#isPrepared()
     */
    public boolean isPrepared() {
        return prepared;
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

            prepared = true;
            for (Object alterDef : alterDefs) {
                if (alterDef instanceof IPreparable) {
                    ((IPreparable) alterDef).prepare();
                }
            }

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

            if (commonCommand != null) {
                engine.executeOnMultipleNodes(commonCommand, getNodeList(),
                        client);
            }
            for (Object alterDef : alterDefs) {
                if (alterDef instanceof IExecutable) {
                    ((IExecutable) alterDef).execute(engine);
                }
            }
            return ExecutionResult
                    .createSuccessResult(ExecutionResult.COMMAND_ALTER_TABLE);

        } finally {
            logger.exiting(method);
        }
    }

    public Command getCommandToExecute() {
        return commandToExecute;
    }

    public SysDatabase getDatabase() {
        return database;
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
