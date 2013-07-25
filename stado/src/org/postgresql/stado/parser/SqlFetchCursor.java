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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Vector;

import org.postgresql.stado.common.ActivityLog;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.constraintchecker.IConstraintChecker;
import org.postgresql.stado.engine.DeclaredCursor;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutableRequest;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IExecutable;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.MultinodeExecutor;
import org.postgresql.stado.engine.NodeProducerThread;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.engine.io.DataTypes;
import org.postgresql.stado.engine.loader.Loader;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.partitions.PartitionMap;
import org.postgresql.stado.metadata.partitions.ReplicatedPartitionMap;
import org.postgresql.stado.metadata.scheduler.ILockCost;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.misc.RSHelper;
import org.postgresql.stado.optimizer.QueryTree;
import org.postgresql.stado.optimizer.SqlExpression;
import org.postgresql.stado.parser.core.syntaxtree.Deallocate;
import org.postgresql.stado.parser.core.syntaxtree.DeclareCursor;
import org.postgresql.stado.parser.core.syntaxtree.FetchCursor;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.IdentifierHandler;
import org.postgresql.stado.parser.handler.QueryTreeHandler;
import org.postgresql.stado.parser.handler.QueryTreeTracker;
import org.postgresql.stado.queryproc.QueryProcessor;

public class SqlFetchCursor extends DepthFirstVoidArguVisitor implements IXDBSql,
        IPreparable {

    private static final XLogger logger = XLogger.getLogger(SqlFetchCursor.class);

    private XDBSessionContext client;

    private Command commandToExecute;

    public QueryTree aQueryTree = null;

    private QueryProcessor qProcessor;
    
    private DeclaredCursor cursor = null;

    private Object finalFetch;

    private Object finalDeclare;

    public SqlFetchCursor(XDBSessionContext client) {
        this.client = client;
        commandToExecute = new Command(Command.SELECT, this,
                new QueryTreeTracker(), client);
        aQueryTree = new QueryTree();


    }

    // ******************************
    // BEGIN GRAMMAR
    // ******************************

    /**
     * Grammar production:
     * f0 -> <FETCH_>
     * f1 -> <INT_LITERAL>
     * f2 -> <FROM_>
     * f3 -> Identifier(prn)
     */
    @Override
    public void visit(FetchCursor n, Object argu) {
		String cursorName = (String) n.f3.accept(new IdentifierHandler(), argu);
		cursor = client.getCursor(cursorName);
		aQueryTree = cursor.getCursorTree().copy();
        
		aQueryTree.setFetchCursor(true);
		
		aQueryTree.setFetchCount(new Integer(n.f1.tokenImage).intValue());
		
		aQueryTree.setCursorName(cursorName);
    }

    /**
     * This will return the cost of executing this statement in time , milli
     * seconds
     */

    public long getCost() {
        return ILockCost.LOW_COST;
    }

    /**
     * This return the Lock Specification for the system
     *
     * @param theMData
     * @return
     */
    public LockSpecification<SysTable> getLockSpecs() {
        Collection<SysTable> empty = Collections.emptyList();
        LockSpecification<SysTable> aLspec = new LockSpecification<SysTable>(
                empty, empty);
        return aLspec;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Parser.IXDBSql#getNodeList()
     */
    public Collection<DBNode> getNodeList() {
        Collection<DBNode> empty = Collections.emptyList();
        return new ArrayList<DBNode>(empty);
    }

    public ExecutionResult execute(Engine engine) throws Exception {
        final String method = "execute";
        logger.entering(method, new Object[] { engine });

        if (!client.isInTransaction()) {
            throw new XDBServerException(
                    ErrorMessageRepository.DECLARE_CURSOR_TRANS,
                    0,
                    ErrorMessageRepository.DECLARE_CURSOR_TRANS_CODE);        	
        }

        try {

            if (!isPrepared()) {
                prepare();
            }

            if (cursor.getMaterializedTable() != null && !cursor.isMaterialized()) {
            	createMaterializedTable(cursor.getMaterializedTable(), engine);
            	
                if (materializeCursor(engine) > 0) {                	
                	try {
	                    Connection oConn = client.getAndSetCoordinatorConnection();
	                    cursor.setOpenStatement(oConn.createStatement());
	                    cursor.getOpenStatement().execute("BEGIN");
	                    cursor.getOpenStatement().execute(finalDeclare.toString());
	                	cursor.setMaterialized(true);
                    } catch (SQLException se) {
                        logger.catching(se);
                        cursor.getOpenStatement().getConnection().rollback();
                        return ExecutionResult.createErrorResult(se);
                    }
                }
            }

            try {
                ResultSet rs;
                rs = cursor.getOpenStatement().executeQuery(finalFetch.toString());
                
                return ExecutionResult.createResultSetResult(
                    ExecutionResult.COMMAND_SELECT, rs);
            } catch (SQLException se) {
                logger.catching(se);
                cursor.getOpenStatement().getConnection().rollback();
                return ExecutionResult.createErrorResult(se);
            }

        } finally {
            if (Props.XDB_ENABLE_ACTIVITY_LOG) {
                ActivityLog.endRequest(client.getStatementId());
            }
            logger.exiting(method);
        }
    }

    public boolean isPrepared() {
        return qProcessor != null && qProcessor.isPrepared();
    }

    public void prepare() throws Exception {
        final String method = "prepare";
        logger.entering(method);
        try {
        	
        	if (!cursor.isMaterialized()) {
	            PartitionMap partitionMap = new ReplicatedPartitionMap();
	            Collection<Integer> nodes = new ArrayList<Integer>(1);
	            nodes.add(1);
	            partitionMap.generateDistribution(nodes);
	            
	            
	            Collection<SysColumn> sysColumns = new LinkedHashSet<SysColumn>();
	            int colID = 1;
	            for (SqlCreateTableColumn srcCol : aQueryTree.getColumnDefinitions()) {
	                SysColumn column = new SysColumn(null, colID, colID,
	                		srcCol.columnName, srcCol.getColumnType(),
	                        srcCol.getColumnLength(), srcCol.getColumnScale(),
	                        srcCol.getColumnPrecision(), true, false,
	                        srcCol.rebuildString(), 0, null);
	                sysColumns.add(column);
	                colID++;
	            }
	            
	            SysTable mTable = client.getSysDatabase().createTempSysTable(
	            		"C_" + cursor.getName() + "_",
	                    SysTable.PTYPE_ONE,
	                    partitionMap,
	                    null,
	                    null,
	                    null,
	                    sysColumns);
	            
	            // Its not really temporary since we need to materialize it for 
	            // future fetches, but it is unlogged since the data is disposable
	            mTable.setTableTemporary(false); 
	            mTable.setTableUnlogged(true);
	            cursor.setMaterializedTable(mTable);
	            
	            qProcessor = new QueryProcessor(client, cursor.getCursorTree());
	            qProcessor.prepare(false);
        	}
            prepareFinalFetch(cursor.getMaterializedTable());
            
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
    
    protected void createMaterializedTable(SysTable materializedTable, Engine engine) {
        if (Props.XDB_COMMIT_AFTER_CREATE_TEMP_TABLE
                || !materializedTable.isTemporary()) {
            MultinodeExecutor anExecutor = client.getMultinodeExecutor(materializedTable.getNodeList());
            anExecutor.executeCommand(materializedTable.getTableDef(false),
            		materializedTable.getNodeList(), true);
        } else {
            engine.executeOnMultipleNodes(materializedTable.getTableDef(false),
            		materializedTable.getNodeList(), client);
        }
    }

    protected long materializeCursor(Engine engine) throws Exception {
        final String method = "materializeCursor";
        logger.entering(method, new Object[] {});
        try {
            long rowCount = 0;
            

                        ResultSet rs = qProcessor.execute(engine).getResultSet();
                        try {
                            if (Props.XDB_USE_LOAD_FOR_STEP) {
                                Loader loader = new Loader(rs, null);
                                loader.setLocalTableInfo(cursor.getMaterializedTable(), client,
                                        cursor.getColumnList(), false);
                                try {
                                    loader.prepareLoad();
                                    boolean success = false;
                                    try {
                                        NodeProducerThread.CATEGORY_NODEQUERYTIME.debug("Run Writers");
                                        loader.runWriters();
                                        success = true;
                                    } finally {
                                        NodeProducerThread.CATEGORY_NODEQUERYTIME.debug("Finish load");
                                        loader.finishLoad(success);
                                    }
                                    return loader.getRowCount();
                                } catch (Exception e) {
                                    logger.catching(e);
                                    XDBServerException ex = new XDBServerException(
                                            "Can not send data to Nodes", e);
                                    logger.throwing(ex);
                                    throw ex;
                                }
                            } else {
                                StringBuffer sbInsert = new StringBuffer(
                                "INSERT INTO ");
                                sbInsert.append(IdentifierHandler.quote(cursor.getMaterializedTable().getTableName())).append(
                                " (");
                                for (SysColumn sysCol : cursor.getMaterializedTable().getColumns()) {
                                    String colName = sysCol.getColName();
                                    sbInsert.append(IdentifierHandler.quote(colName)).append(", ");
                                }
                                sbInsert.setLength(sbInsert.length() - 2);
                                sbInsert.append(") VALUES (");
                                while (rs.next()) {
                                    StringBuffer sbValues = new StringBuffer();

                                    sbValues.setLength(sbValues.length() - 2);

                                    rowCount++;
                                }
                            }
                        } finally {
                            rs.close();
                        }
                        
            return rowCount;
        } finally {
            logger.exiting(method);
        }
    }

    private void prepareFinalFetch(SysTable mTable) throws Exception {
        final String method = "prepareFinalFetch";
        logger.entering(method, new Object[] {});
        try {

        	if (!cursor.isMaterialized()) {
	            StringBuffer sbCommand = new StringBuffer("DECLARE ");
	            sbCommand.append(cursor.getName() + " CURSOR FOR SELECT ");
	            for (SysColumn column : mTable.getColumns()) {
	                    String colName = column.getColName();
	                    sbCommand.append(IdentifierHandler.quote(colName)).append(", ");
	            }
	            sbCommand.setLength(sbCommand.length() - 2);
	            sbCommand.append(" FROM ").append(IdentifierHandler.quote(mTable.getTableName()));
	            finalDeclare = sbCommand;
        	}
        	
        	StringBuffer sbCommand = new StringBuffer("FETCH ");
            sbCommand.append(aQueryTree.getFetchCount());
            sbCommand.append(" FROM " + cursor.getName());
            finalFetch = sbCommand;

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
