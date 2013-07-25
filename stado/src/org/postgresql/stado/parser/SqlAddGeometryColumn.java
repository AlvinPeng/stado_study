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

import java.sql.ResultSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.postgresql.stado.common.util.ParseCmdLine;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IExecutable;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.SyncAlterTableAddColumn;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysPermission;
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
import org.postgresql.stado.parser.core.syntaxtree.SelectAddGeometryColumn;
import org.postgresql.stado.parser.core.syntaxtree.SetTablespace;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.DataTypeHandler;
import org.postgresql.stado.parser.handler.IdentifierHandler;
import org.postgresql.stado.parser.handler.QueryTreeTracker;
import org.postgresql.stado.parser.handler.SQLExpressionHandler;
import org.postgresql.stado.parser.handler.TableNameHandler;

public class SqlAddGeometryColumn extends DepthFirstVoidArguVisitor implements IXDBSql,
        IPreparable {
    private static final XLogger logger = XLogger
            .getLogger(SqlAddGeometryColumn.class);

    private XDBSessionContext client;

    private SysDatabase database;

    private String tableName;

    private SqlCreateTableColumn aSqlCreateTableColumn;

    private SysTable table;

    private Command commandToExecute;

    // We support only single alter definition, but many
    // other
    // DBMS vendors support list of them, so putting List here for future
    // compatibility
    private List<Object> alterDefs = new LinkedList<Object>();

    private String commonCommand;
    
    private boolean prepared = false;

    public SqlAddGeometryColumn(XDBSessionContext client) {
        this.client = client;
        database = client.getSysDatabase();
        commandToExecute = new Command(Command.CREATE, this,
                new QueryTreeTracker(), client);
    }

    /**
     * Grammar production: 
     * f0  -> <SELECT_> 
     * f1  -> <ADDGEOMETRYCOLUMN_> 
     * f2  -> "("
	 * f3  -> SQLArgument(prn)
     * f4  -> ","
	 * f5  -> SQLArgument(prn)
     * f6  -> ","
	 * f7  -> SQLArgument(prn)
     * f8  -> ","
	 * f9  -> SQLArgument(prn)
     * f10 -> ","
	 * f11 -> SQLArgument(prn)
	 * f12 -> [ "," SQLArgument(prn) ]
	 * f13 -> [ "," SQLArgument(prn) ]
     * f14 -> ")" 
     */
    @Override
    public void visit(SelectAddGeometryColumn n, Object argu) {
        String columnName = null;

        
    	SQLExpressionHandler aSqlExpressionHandler = new SQLExpressionHandler(
                commandToExecute);
        
        n.f3.accept(aSqlExpressionHandler, argu);
        String f3  = aSqlExpressionHandler.aroot.getExprString();
        f3 = f3.replaceAll("'", "");

        n.f5.accept(aSqlExpressionHandler, argu);
        String f5 = aSqlExpressionHandler.aroot.getExprString();
        f5 = f5.replaceAll("'", "");
        
        n.f7.accept(aSqlExpressionHandler, argu);
        String f7 = aSqlExpressionHandler.aroot.getExprString();
        f7 = f7.replaceAll("'", "");

        n.f9.accept(aSqlExpressionHandler, argu);
        String f9 = aSqlExpressionHandler.aroot.getExprString();
        f9 = f9.replaceAll("'", "");

        if (n.f12.present()) {
        	if (n.f13.present()) {
	        	tableName = f7;
	        	columnName = f9;        		
        	} else {
	        	tableName = f5;
	        	columnName = f7;
        	}
        	
        } else {
        	tableName = f3;
        	columnName = f5;
        }
        
        commonCommand = argu.toString();

        aSqlCreateTableColumn = new SqlCreateTableColumn(columnName, 
        		new DataTypeHandler(ExpressionType.GEOMETRY_TYPE, 1024, 0, 0), true, null);
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

    public SqlCreateTableColumn getColDef() {
        return aSqlCreateTableColumn;
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
        ArrayList<String> comm = new ArrayList<String>();
        logger.entering(method, new Object[] {});
        try {

            SysTable table = getTable();
            table.ensurePermission(client.getCurrentUser(),
                    SysPermission.PRIVILEGE_ALTER);
            if (table.getSysColumn(aSqlCreateTableColumn.columnName) != null) {
                throw new XDBServerException(
                        "The Table Already Has A Column Named "
                                + aSqlCreateTableColumn.columnName);
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
        ResultSet ret = null;
        try {

            if (commonCommand != null) {
            	Map<Integer, ResultSet> results = engine.executeDDLFunctionOnMultipleNodes(commonCommand, getNodeList(),
                        new SyncAlterTableAddColumn(this), client);
            	
                for (ResultSet rs : results.values()) {
                    	ret = rs;
                }


            }

            return ExecutionResult.createResultSetResult(
                    ExecutionResult.COMMAND_SELECT, ret);

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
