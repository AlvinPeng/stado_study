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
 * SqlModifyTable.java
 *
 *
 */
package org.postgresql.stado.parser;

import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.constraintchecker.IConstraintChecker;
import org.postgresql.stado.engine.BatchInsertGroup;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IExecutable;
import org.postgresql.stado.engine.IParametrizedSql;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.MultinodeExecutor;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysForeignKey;
import org.postgresql.stado.metadata.SysReference;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.scheduler.ILockCost;
import org.postgresql.stado.metadata.scheduler.Lock;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.metadata.scheduler.LockType;
import org.postgresql.stado.parser.core.visitor.DepthFirstRetArguVisitor;
import org.postgresql.stado.parser.handler.IdentifierHandler;

/**
 * Move here common code for a lot of Sql...Table classes
 *
 *
 */
public abstract class SqlModifyTable extends DepthFirstRetArguVisitor implements
        IPreparable, IXDBSql, IParametrizedSql {
    private static final XLogger logger = XLogger.getLogger(SqlModifyTable.class);

    protected static final String NODE_ID = "XDB__NODE__ID";

    protected XDBSessionContext client;

    protected SysDatabase database;

    protected Command commandToExecute;

    // Valid after statement is parsed
    protected String tableName;

    protected Collection<DBNode> nodeList;

    private SysTable targetTable;

    private long cost = 0;

    private BatchInsertGroup group = null;

    private long result;

    private boolean batchMode = false;

    /** whether or not we need to use a final temp table for results,
     * or if we update table directly.
     */
    private boolean usesFinalTempTable = true;

    public SqlModifyTable(XDBSessionContext client) {
        this.client = client;
        database = client.getSysDatabase();
    }

    public String getTableName() {
        return tableName;
    }

    /**
     * Get from MetaData SysTable to be modified
     *
     * @return SysTable
     * @throws XDBServerException
     *                 if table not found
     */
    public SysTable getTargetTable() throws XDBServerException {
        if (targetTable == null) {
            targetTable = database.getSysTable(getTableName());
        }
        return targetTable;
    }

    protected LockSpecification<SysTable> lockSpecs = null;

    public Collection<SysTable> getReadTables() {
        HashSet<SysTable> readObjects = new HashSet<SysTable>();
        if (dataSource instanceof ILockCost) {
            for (Lock<SysTable> lockTable : ((ILockCost) dataSource).getLockSpecs().getCombinedVector()) {
                readObjects.add(lockTable.getManagedObject());
            }
        }
        SysTable delTable = getTargetTable();
        Vector vRefFkList = delTable.getSysFkReferenceList();
        Vector vRefRefList = delTable.getSysReferences();

        for (Iterator it = vRefFkList.iterator(); it.hasNext();) {
            SysReference aReferringTab = (SysReference) it.next();
            SysTable refTable = database.getSysTable(aReferringTab.getRefTableID());
            readObjects.add(refTable);
        }

        for (Iterator it = vRefRefList.iterator(); it.hasNext();) {
            SysReference aReferringTab = (SysReference) it.next();
            Vector vFkKeys = aReferringTab.getForeignKeys();
            SysForeignKey aFkey = (SysForeignKey) vFkKeys.elementAt(0);
            SysTable refTable = aFkey.getReferringSysColumn(database).getSysTable();
            readObjects.add(refTable);
        }
        return readObjects;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#getLockSpecs(java.lang.Object)
     */
    public LockSpecification<SysTable> getLockSpecs() {
        Collection<SysTable> empty = Collections.emptyList();
        if (lockSpecs == null) {
            lockSpecs = new LockSpecification<SysTable>(getReadTables(), empty);
            lockSpecs.add(getTargetTable(), LockType.get(
                    LockType.LOCK_SHARE_WRITE_INT, false));
        }
        return lockSpecs;
    }

    protected abstract Collection<DBNode> getExecutionNodes();

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Parser.IXDBSql#getNodeList()
     */
    public Collection<DBNode> getNodeList() {
        if (nodeList == null) {
            nodeList = new HashSet<DBNode>();
            nodeList.addAll(getExecutionNodes());
            SysTable delTable = getTargetTable();
            Vector vRefFkList = delTable.getSysFkReferenceList();
            Vector vRefRefList = delTable.getSysReferences();
            for (Iterator it = vRefFkList.iterator(); it.hasNext();) {
                SysReference aReferringTab = (SysReference) it.next();
                if (aReferringTab.getConstraint().getIsSoft() == 1) {
                    SysTable refTable = database.getSysTable(aReferringTab.getRefTableID());
                    nodeList.addAll(refTable.getNodeList());
                }
            }

            for (Iterator it = vRefRefList.iterator(); it.hasNext();) {
                SysReference aReferringTab = (SysReference) it.next();
                if (aReferringTab.getConstraint().getIsSoft() == 1) {
                    Vector vFkKeys = aReferringTab.getForeignKeys();
                    SysForeignKey aFkey = (SysForeignKey) vFkKeys.elementAt(0);
                    SysTable refTable = aFkey.getReferringSysColumn(database).getSysTable();
                    nodeList.addAll(refTable.getNodeList());
                }
            }
        }
        return nodeList;
    }

    protected abstract Collection<SysColumn> getColumnsInvolved();

    protected abstract IConstraintChecker getPKChecker(SysTable targetTable,
            Collection<SysColumn> columnsInvolved);

    protected abstract IConstraintChecker getFKChecker(SysTable targetTable,
            Collection<SysColumn> columnsInvolved);

    protected abstract IConstraintChecker getFRChecker(SysTable targetTable,
            Collection<SysColumn> columnsInvolved);

    protected SysTable createTempTableMetadata(SysTable targetTable,
            Collection<SysColumn> columnsInvolved) throws Exception {
        return database.createTempSysTable(tableName + "_",
                targetTable.getPartitionScheme(),
                targetTable.getPartitionMap(),
                targetTable.getPartitionColumn(),
                targetTable.getSerialHandler(), targetTable.getRowIDHandler(),
                columnsInvolved);
    }

    /**
     * Create and parse a query to fill temp table, or create a Tuple, what is
     * appropriate
     *
     *
     * @return IExecutable, QueryTree, QueryPlan, Tuple or any other data
     *         container fillTable must handle it.
     * @throws Exception
     *                 if any error occured
     */
    protected abstract Object prepareDataSource(SysTable targetTable,
            SysTable tempTable) throws Exception;

    /**
     * Create SQL statement(s) to apply data from temp table to targetTable
     *
     * @throws Exception
     *                 if any error occured
     */
    protected abstract Object prepareFinalStatements(SysTable targetTable,
            SysTable tempTable) throws Exception;

    /**
     *
     * @param source
     * @param tempTable
     * @param engine
     * @throws Exception
     */
    protected abstract long fillTable(Object source, SysTable tempTable,
            Engine engine) throws Exception;

    protected int internalExecute(Object finalStatements, Engine engine)
            throws Exception {
        int result;
        if (finalStatements instanceof String) {
            result = engine.executeOnMultipleNodes((String) finalStatements,
                    getExecutionNodes(), client);
        } else if (finalStatements instanceof Map) {
            result = engine.executeOnMultipleNodes((Map) finalStatements,
                    client);
        } else {
            XDBServerException ex = new XDBServerException(
                    "Invalid statements: " + finalStatements);
            logger.throwing(ex);
            throw ex;
        }
        return result;
    }

    protected boolean groupExecute(Object finalStatements, Engine engine)
            throws Exception {
        if (finalStatements instanceof String) {
            return engine.addToBatchOnNodes((String) finalStatements,
                    getNodeList(), client);
        } else if (finalStatements instanceof Map) {
            return engine.addToBatchOnNodes((Map) finalStatements, client);
        } else {
            XDBServerException ex = new XDBServerException(
                    "Invalid statements: " + finalStatements);
            logger.throwing(ex);
            throw ex;
        }
    }

    private boolean prepared;

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IPreparable#isPrepared()
     */
    public boolean isPrepared() {
        return prepared;
    }

    /**
     * Clean up the SqlModifyTable object enabling to prepare and execute the
     * same statement.
     */
    public void reset() {
        prepared = false;
    }

    private SysTable tempTable;

    protected SysTable getTempTable() {
        return tempTable;
    }

    private Object dataSource;

    private Object finalStatements;

    protected Object getFinalStatements() {
        return finalStatements;
    }

    protected Collection<IConstraintChecker> validators;

    protected abstract short getPrivilege();

    protected void createTempTable(SysTable tempTable, Engine engine) {
        if (Props.XDB_COMMIT_AFTER_CREATE_TEMP_TABLE
                || !tempTable.isTemporary()) {
            MultinodeExecutor anExecutor = client.getMultinodeExecutor(tempTable.getNodeList());
            anExecutor.executeCommand(tempTable.getTableDef(false),
                    tempTable.getNodeList(), true);
        } else {
            engine.executeOnMultipleNodes(tempTable.getTableDef(false),
                    tempTable.getNodeList(), client);
        }
    }

    protected void dropTempTable(SysTable tempTable, Engine engine) {
        engine.executeOnMultipleNodes("DROP TABLE "
                + IdentifierHandler.quote(tempTable.getTableName()),
                tempTable.getNodeList(), client);
        // If drop temp table is failed metadata will have lost SysTable in
        // memory
        // Maybe we should do additional cleanup after all ?
        database.dropSysTable(tempTable.getTableName());
    }

    protected boolean isBatcheable() {
        return tempTable == null;
    }

    public void setBatchMode() {
        batchMode = true;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IPreparable#prepare()
     */
    public void prepare() throws Exception {
        final String method = "prepare";
        logger.entering(method);
        try {

            validators = new ArrayList<IConstraintChecker>();
            getTargetTable().ensurePermission(client.getCurrentUser(),
                    getPrivilege());
            IConstraintChecker checker = getPKChecker(targetTable,
                    getColumnsInvolved());
            if (checker != null && !checker.isEmpty()) {
                validators.add(checker);
            }
            checker = getFKChecker(targetTable, getColumnsInvolved());
            if (checker != null && !checker.isEmpty()) {
                validators.add(checker);
            }
            checker = getFRChecker(targetTable, getColumnsInvolved());
            if (checker != null && !checker.isEmpty()) {
                validators.add(checker);
            }
            tempTable = createTempTableMetadata(getTargetTable(),
                    getColumnsInvolved());
            dataSource = prepareDataSource(targetTable, tempTable);
            for (IConstraintChecker validator : validators) {
                validator.setTempTable(tempTable);
                validator.prepare();
            }
            if (batchMode) {
                if (isBatcheable()) {
                    group = client.getInsertGroup(this);
                } else {
                    client.closeInsertGroup(this);
                }
            }
            prepared = true;

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
        logger.entering(method, new Object[] { engine });
        try {

            if (!isPrepared()) {
                prepare();
            }
            if (tempTable != null && usesFinalTempTable) {
                createTempTable(tempTable, engine);
            }
            try {
                if (group != null) {
                    if (!group.executed()) {
                        group.setExecuted();
                        List<SqlModifyTable> members = new LinkedList<SqlModifyTable>();
                        // execute the batch
                        for (SqlModifyTable modifyTable : group.getMembers()) {
                            members.add(modifyTable);
                            modifyTable.finalStatements = prepareFinalStatements(
                                    modifyTable.targetTable,
                                    modifyTable.tempTable);
                            if (modifyTable.groupExecute(
                                    modifyTable.finalStatements, engine)) {
                                int[] results = engine.executeBatchOnNodes(
                                        client, false);
                                int i = 0;
                                for (SqlModifyTable member : members) {
                                    if (targetTable.isLookup()) {
                                        member.result = results[i++]
                                                / targetTable.getNodeList().size();
                                    } else {
                                        member.result = results[i++];
                                    }
                                }
                                members = new LinkedList<SqlModifyTable>();
                            }
                        }
                        if (members.size() > 0) {
                            int[] results = engine.executeBatchOnNodes(client,
                                    false);
                            int i = 0;
                            for (SqlModifyTable member : members) {
                                if (targetTable.isLookup()) {
                                    member.result = results[i++]
                                            / targetTable.getNodeList().size();
                                } else {
                                    member.result = results[i++];
                                }
                            }
                        }
                    }
                } else {
                    finalStatements = prepareFinalStatements(targetTable,
                            tempTable);
                    result = fillTable(dataSource, tempTable, engine);
                    // For non-optimized path, we need to do internal execute.
                    // For optimized path, we also know we can skip validators
                    if (usesFinalTempTable) {
                        for (Object element : validators) {
                            ((IExecutable) element).execute(engine);
                        }
                        result = internalExecute(finalStatements, engine);
                    }
                    if (targetTable.isLookup()) {
                        result /= targetTable.getNodeList().size();
                    }
                }
                return ExecutionResult.createRowCountResult(getResultType(),
                        Long.valueOf(result).intValue());
            } finally {
                try {
                    if (tempTable != null && usesFinalTempTable) {
                        dropTempTable(tempTable, engine);
                    }
                } catch (Exception ex) {
                    // It is safe to ignore error message if drop table failed,
                    // and this exception can hide original error if there was
                    // problems
                }
            }

        } finally {
            logger.exiting(method);
        }
    }

    public abstract int getResultType();

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#getCost()
     */
    public long getCost() {
        final String method = "getCost";
        logger.entering(method, new Object[] {});
        try {

            if (cost == 0) {
                try {
                    if (!isPrepared()) {
                        prepare();
                    }
                    if (dataSource instanceof ILockCost) {
                        // I assume fill tamp table and move to target table
                        // operations have same cost
                        cost += 2 * ((ILockCost) dataSource).getCost();
                    }
                    for (Object validator : validators) {
                        if (validator instanceof ILockCost) {
                            cost += ((ILockCost) validator).getCost();
                        }
                    }

                } catch (Exception ex) {
                    logger.catching(ex);
                }
            }
            return cost;

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Executes current batch on Nodes. Method is shared between all
     * SqlModifyTable descendant to fill their temp tables
     *
     * @param engine
     * @throws XDBServerException
     */
    protected void executeCurrentBatch(Engine engine) throws XDBServerException {
        int[] results = engine.executeBatchOnNodes(client, true);
        for (int element : results) {
            if (element == Statement.EXECUTE_FAILED) {
                XDBServerException ex = new XDBServerException(
                        "Failed to populate temp table");
                logger.throwing(ex);
                throw ex;
            }
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#needCoordinatorConnection()
     */
    public boolean needCoordinatorConnection() {
        return true;
    }

    public boolean isReadOnly() {
        return false;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.engine.IParametrizedSql#getParamCount()
     */
    public int getParamCount() throws XDBServerException {
        return commandToExecute.getParamCount();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.engine.IParametrizedSql#setParamValue(int,
     *      java.lang.String)
     */
    public void setParamValue(int index, String value)
            throws ArrayIndexOutOfBoundsException, XDBServerException {
        reset();
        commandToExecute.getParameter(index + 1).setParamValue(value);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.engine.IParametrizedSql#setParamValues(java.lang.String[])
     */
    public void setParamValues(String[] values)
            throws ArrayIndexOutOfBoundsException, XDBServerException {
        reset();
        for (int i = 0; i < values.length; i++) {
            commandToExecute.getParameter(i + 1).setParamValue(values[i]);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.engine.IParametrizedSql#getParamDataType(int)
     */
    public int getParamDataType(int index)
            throws ArrayIndexOutOfBoundsException, XDBServerException {
        ExpressionType type = commandToExecute.getParameter(index + 1).getExprDataType();
        return type == null ? Types.NULL : type.type;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.engine.IParametrizedSql#getParamDataTypes()
     */
    public int[] getParamDataTypes() throws ArrayIndexOutOfBoundsException,
            XDBServerException {
        int[] result = new int[commandToExecute.getParamCount()];
        for (int i = 0; i < commandToExecute.getParamCount(); i++) {
            ExpressionType type = commandToExecute.getParameter(i + 1).getExprDataType();
            result[i] = type == null ? Types.NULL : type.type;
        }
        return result;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.engine.IParametrizedSql#getParamValue(int)
     */

    public String getParamValue(int index)
            throws ArrayIndexOutOfBoundsException, XDBServerException {
        return commandToExecute.getParameter(index + 1).getParamValue();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.engine.IParametrizedSql#getParamValues()
     */
    public String[] getParamValues() throws ArrayIndexOutOfBoundsException,
            XDBServerException {
        String[] result = new String[commandToExecute.getParamCount()];
        for (int i = 0; i < commandToExecute.getParamCount(); i++) {
            result[i] = commandToExecute.getParameter(i + 1).getParamValue();
        }
        return null;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.engine.IParametrizedSql#setParamDataType(int, int)
     */
    public void setParamDataType(int index, int dataType)
            throws ArrayIndexOutOfBoundsException, XDBServerException {
        ExpressionType type = commandToExecute.getParameter(index + 1).getExprDataType();
        if (type == null) {
            type = new ExpressionType();
            commandToExecute.getParameter(index + 1).setExprDataType(type);
        }
        type.type = dataType;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.engine.IParametrizedSql#setParamDataTypes(int[])
     */
    public void setParamDataTypes(int[] dataTypes)
            throws ArrayIndexOutOfBoundsException, XDBServerException {
        for (int i = 0; i < dataTypes.length; i++) {
            ExpressionType type = commandToExecute.getParameter(i + 1).getExprDataType();
            if (type == null) {
                type = new ExpressionType();
                commandToExecute.getParameter(i + 1).setExprDataType(type);
            }
            type.type = dataTypes[i];
        }
    }

    /**
     * @param value - whether or not a final temp table is used
     */
    protected void setUsesFinalTempTable (boolean value) {
        usesFinalTempTable = value;
    }
}
