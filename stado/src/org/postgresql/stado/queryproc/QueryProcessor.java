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
/**
 * QueryProcessor.java
 *
 */
package org.postgresql.stado.queryproc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.postgresql.stado.common.ColumnMetaData;
import org.postgresql.stado.common.ResultSetImpl;
import org.postgresql.stado.common.XDBResultSetMetaData;
import org.postgresql.stado.common.util.ParseCmdLine;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.MultinodeExecutor;
import org.postgresql.stado.engine.NodeProducerThread;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.engine.loader.Loader;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBSecurityException;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.MetaData;
import org.postgresql.stado.metadata.NodeDBConnectionInfo;
import org.postgresql.stado.metadata.SyncCreateTable;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysLogin;
import org.postgresql.stado.metadata.SysPermission;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.SysTablespace;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.misc.RSHelper;
import org.postgresql.stado.misc.Timer;
import org.postgresql.stado.misc.combinedresultset.ServerResultSetImpl;
import org.postgresql.stado.optimizer.AttributeColumn;
import org.postgresql.stado.optimizer.Optimizer;
import org.postgresql.stado.optimizer.QueryCondition;
import org.postgresql.stado.optimizer.QueryTree;
import org.postgresql.stado.optimizer.RelationNode;
import org.postgresql.stado.optimizer.SqlExpression;
import org.postgresql.stado.parser.ExpressionType;
import org.postgresql.stado.parser.IXDBSql;
import org.postgresql.stado.parser.handler.IdentifierHandler;
import org.postgresql.stado.planner.ExecutionPlan;
import org.postgresql.stado.planner.ExecutionStep;
import org.postgresql.stado.planner.QueryPlan;
import org.postgresql.stado.planner.StepDetail;



/*******************************************************************************
 * Notes:
 *
 * There is the possibility of achieving a small improvement in performance with
 * UNION. Right now the UNION query is combined into a temp table at the end, in
 * case it itself is used as part of a subquery. Instead, if we know it is the
 * final query, we could perform the UNION as part of returning the results to
 * the user, and avoid the extra step of creating a single result table.
 ******************************************************************************/

/**
 * Handles processing of SELECT queries
 */
public class QueryProcessor implements IPreparable, IXDBSql {
    private static final XLogger logger = XLogger
    .getLogger(QueryProcessor.class);


    private Timer DataDownTimer;

    private Timer QueryTimer;

    //
    private String finalSelectClause;

    private MultinodeExecutor aMultinodeExecutor;

    private ServerResultSetImpl finalResultSet;

    // UNION'ed ResultSets
    private List<ServerResultSetImpl> unionGroup1;

    // UNION ALL'ed ResultSets
    private List<ServerResultSetImpl> unionGroup2;

    private XDBSessionContext client;

    /**
     * Executes ExecutionPlan
     *
     * @param anExecutionPlan
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    private void executeQueryExecPlan(ExecutionPlan anExecutionPlan)
            throws SQLException {
        final String method = "executeQueryExecPlan";
        logger.entering(method);

        try {
            if (anExecutionPlan.unionPlanList.size() > 0) {
                executeUnionQuery(anExecutionPlan);
            }

            // Check to see if we have any scalar subqueries to execute first
            for (ExecutionPlan scalarPlan : anExecutionPlan.scalarPlanList) {
                executeScalarQuery(scalarPlan);
            }

            for (ExecutionPlan relationPlan : anExecutionPlan.relationPlanList) {
                executeQueryExecPlan(relationPlan);
            }

            // ok, now we can start processing the steps of our query
            for (ExecutionStep anExecStep : anExecutionPlan.stepList) {
                // See if we need to execute any correlated subplans first
                if (anExecStep.correlatedSubPlan != null) {
                    logger.debug("Handle anExecStep.correlatedSubPlan");
                    executeQueryExecPlan(anExecStep.correlatedSubPlan);
                    logger.debug("Handle anExecStep.correlatedSubPlan - Done");
                }

                QueryPlan.CATEGORY_QUERYFLOW.info(anExecStep);

                // Process any uncorrelated subqueries in the plan first
                if (anExecStep.uncorrelatedSubPlanList != null) {
                    for (ExecutionPlan uncorSubPlan : anExecStep.uncorrelatedSubPlanList) {
                        executeQueryExecPlan(uncorSubPlan);
                    }
                }

                // We are ready to execute the steps in the current plan.

                // Note that we first need to do any substitutions of scalar
                // subqueries, if any exist.
                for (ExecutionPlan scalarPlan : anExecutionPlan.scalarPlanList) {
                    if (anExecStep.aStepDetail != null
                            && anExecStep.aStepDetail.queryString != null) {
                        anExecStep.aStepDetail.queryString = scalarReplace(
                                anExecStep.aStepDetail.queryString,
                                scalarPlan.scalarPlaceholderNo,
                                scalarPlan.scalarResult);
                    }

                    if (anExecStep.coordStepDetail != null
                            && anExecStep.coordStepDetail.queryString != null) {
                        anExecStep.coordStepDetail.queryString = scalarReplace(
                                anExecStep.coordStepDetail.queryString,
                                scalarPlan.scalarPlaceholderNo,
                                scalarPlan.scalarResult);
                    }
                }

                if (anExecStep.outerSubPlan != null) {
                    executeQueryExecPlan(anExecStep.outerSubPlan);
                }

                executeQueryStep(anExecStep.aStepDetail,
                        anExecStep.coordStepDetail, anExecStep.nodeUsageTable,
                        anExecStep.producerCount, anExecStep.consumerCount,
                        anExecStep.isExtraStep, anExecStep.destNodeList,
                        anExecStep.isFinalStep);

                // check to see at the end of this step if the subsequent step
                // is
                // correlated, in which case we need to send data to proper
                // nodes.
                if (anExecStep.correlatedSendDownStep != null) {
                    executeQueryStep(
                            anExecStep.correlatedSendDownStep.aStepDetail,
                            anExecStep.correlatedSendDownStep.coordStepDetail,
                            anExecStep.correlatedSendDownStep.nodeUsageTable,
                            anExecStep.correlatedSendDownStep.producerCount,
                            anExecStep.correlatedSendDownStep.consumerCount,
                            anExecStep.correlatedSendDownStep.isExtraStep,
                            anExecStep.correlatedSendDownStep.destNodeList,
                            anExecStep.isFinalStep);

                    if (anExecStep.correlatedSendDownStep2 != null) {
                        executeQueryStep(
                                anExecStep.correlatedSendDownStep2.aStepDetail,
                                anExecStep.correlatedSendDownStep2.coordStepDetail,
                                anExecStep.correlatedSendDownStep2.nodeUsageTable,
                                anExecStep.correlatedSendDownStep2.producerCount,
                                anExecStep.correlatedSendDownStep2.consumerCount,
                                anExecStep.correlatedSendDownStep2.isExtraStep,
                                anExecStep.correlatedSendDownStep2.destNodeList,
                                anExecStep.isFinalStep);
                    }
                }
                /*
                 * if (anExecStep.outerSubPlan != null) {
                 * executeQueryExecPlan(anExecStep.outerSubPlan); }
                 */
            }

            finalSelectClause = anExecutionPlan.finalProjString;

            /** substitute results from scalar subqueries */
            for (ExecutionPlan scalarPlan : anExecutionPlan.scalarPlanList) {
                finalSelectClause = scalarReplace(finalSelectClause,
                        scalarPlan.scalarPlaceholderNo, scalarPlan.scalarResult);
            }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @param unionResultSet
     * @param unionResultGroup
     */
    private void addUnionResultSet(ServerResultSetImpl unionResultSet,
            int unionResultGroup) {
        if (unionResultGroup == 1) {
            if (unionGroup1 == null) {
                unionGroup1 = new ArrayList<ServerResultSetImpl>();
            }

            unionGroup1.add(unionResultSet);
        } else {
            if (unionGroup2 == null) {
                unionGroup2 = new ArrayList<ServerResultSetImpl>();
            }

            unionGroup2.add(unionResultSet);
        }
    }

    /**
     * Executes UNION plan
     *
     * @param anExecPlan
     */
    private void executeUnionQuery(ExecutionPlan anExecPlan) throws SQLException {
        final String method = "executeUnionQuery";
        logger.entering(method);

        try {
            ExecutionPlan subPlan;

            for (int i = 0; i < anExecPlan.unionPlanList.size(); i++) {
                subPlan = anExecPlan.unionPlanList.get(i);

                executeQueryExecPlan(subPlan);

                // if we are a top level union, create a ResultSet

                if (anExecPlan.isTopLevelUnion) {
                    ExecutionStep lastStep = subPlan.stepList.get(subPlan.stepList.size()-1);

                    if (lastStep.aStepDetail.isProducer) {
                        Collection<? extends ResultSet> aRSVector = aMultinodeExecutor
                        .getFinalResultSets();

                        // Create a new ResultSet for this.
                        ServerResultSetImpl unionResultSet = new ServerResultSetImpl(
                                aRSVector,
                                lastStep.aStepDetail.finalUnionPartSortInfo,
                                lastStep.aStepDetail.finalUnionPartIsDistinct,
                                anExecPlan.getLimit(), anExecPlan.getOffset(),
                                new XDBResultSetMetaData(aQueryPlan
                                        .getMetaData()));
                        // We update temp table drop info in the finalResultSet
                        unionResultSet.setFinalCoordTempTableList(null);
                        unionResultSet
                        .setFinalNodeTempTableList(lastStep.aStepDetail.dropList);

                        // Now we want to save it for combining with other
                        // results
                        // later
                        addUnionResultSet(unionResultSet,
                                lastStep.aStepDetail.unionResultGroup);
                    }
                }
            }

            finalSelectClause = anExecPlan.finalProjString;
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Executes scalar query
     *
     * @param anExecPlan
     */
    private void executeScalarQuery(ExecutionPlan anExecPlan) throws SQLException{
        final String method = "executeScalarQuery";
        logger.entering(method);

        try {
            ResultSet aResultSet = null;
            String queryString;

            // first, execute as we normally would
            executeQueryExecPlan(anExecPlan);

            // Now, determine scalar value
            // we then need to substitute its value

            queryString = "SELECT * FROM "
                + IdentifierHandler.quote(anExecPlan.finalTempTableName);

            logger.debug(" scalar result query = " + queryString);
            QueryCombiner qc = new QueryCombiner(client, "");
            try {
                aResultSet = qc.queryOnCoord(queryString);
            } catch (XDBServerException xe) {
                qc.dropTempTables(Collections.singleton(anExecPlan.finalTempTableName));
                throw xe;
            }

            // There should be exactly one row
            // We save the result for substituion later.
            try {
                if (aResultSet.next()) {
                    boolean[] isQuoted = RSHelper.getQuoteInfo(aResultSet);
                    String value = aResultSet.getString(1);
                    if (value == null || !isQuoted[0]) {
                        anExecPlan.scalarResult = value;
                    } else {
                        anExecPlan.scalarResult = "'" + value.replaceAll("'", "''") + "'";
                        switch (aResultSet.getMetaData().getColumnType(1)) {
                        case Types.DATE:
                            anExecPlan.scalarResult = "date " + anExecPlan.scalarResult;
                            break;
                        case Types.TIME:
                            anExecPlan.scalarResult = "time " + anExecPlan.scalarResult;
                            break;
                        case Types.TIMESTAMP:
                            anExecPlan.scalarResult = "timestamp " + anExecPlan.scalarResult;
                            break;
                        }
                    }
                } else {
                    anExecPlan.scalarResult = null;
                }
            } catch (SQLException e) {
                // now we have a problem
                throw new XDBServerException(
                        e.getMessage() + "\nQUERY: " + queryString,
                        e, ErrorMessageRepository.SQL_EXEC_FAILURE_CODE);
            }

            logger.debug(" scalar value = " + anExecPlan.scalarResult);

            try {
                if (anExecPlan.scalarResult != null && aResultSet.next()) {
                    throw new XDBServerException(
                            ErrorMessageRepository.SCALAR_QUERY_RESULT_ERROR,
                            0,
                            ErrorMessageRepository.SCALAR_QUERY_RESULT_ERROR_CODE);
                }
            } catch (SQLException se) {
                // Good thing -- Only One result
            } finally {
                try {
                    aResultSet.close();
                } catch (Exception e) {
                }
                try {
                    qc.dropTempTables(Collections
                            .singleton(anExecPlan.finalTempTableName));
                } catch (Exception e) {
                }
            }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Replaces scalar query result
     *
     * @param selectStatement
     * @param placeholderNo
     * @param scalarValue
     * @return
     */
    private String scalarReplace(String selectStatement, int placeholderNo,
            String scalarValue) {
        final String method = "scalarReplace";
        logger.entering(method);

        try {
            // substitute place holder with value just determined.
            if (scalarValue != null) {
                return ParseCmdLine.replace(selectStatement, "&x" + placeholderNo
                        + "x&", scalarValue);
            } else {
                // replace value with NULL
                return ParseCmdLine.replace(selectStatement, "&x" + placeholderNo
                        + "x&", "NULL");
            }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Executes a step in the query
     *
     * @param aStepDetail
     * @param coordStepDetail
     * @param nodeUsageTable
     * @param producerCount
     * @param consumerCount
     * @param isExtraStep
     * @param destNodeList
     * @param isFinalStep
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    private void executeQueryStep(StepDetail aStepDetail,
            StepDetail coordStepDetail, Map nodeUsageTable,
            int producerCount, // number of nodes that are producers
            int consumerCount, // number of nodes taht are consumers
            boolean isExtraStep, List destNodeList, boolean isFinalStep)
    throws XDBServerException {
        final String method = "executeQueryStep";
        logger.entering(method);

        try {
            QueryCombiner qc = null;

            // See if we are on the last step, where we need to send to the
            // coordinator
            if (isFinalStep
                    && coordStepDetail != null
                    && coordStepDetail.isProducer
                    && (coordStepDetail.getDestType() == StepDetail.DEST_TYPE_COORD || coordStepDetail.getDestType() == StepDetail.DEST_TYPE_COORD_FINAL)) {
                try {
                    setFinalResultSetOnCoordinator(coordStepDetail.queryString,
                            coordStepDetail.unionResultGroup, coordStepDetail);

                    return;
                } catch (SQLException se) {
                    throw new XDBServerException(
                            se.getMessage() + "\nQUERY: " + coordStepDetail.queryString,
                            se, ErrorMessageRepository.SQL_EXEC_FAILURE_CODE);
                }
            }

            // If we are doing an extra OUTER step,
            // just execute on the combiner node

            // TODO: change this to use a nodeProducerThread on the coordinator
            if (isExtraStep) {
                qc = new QueryCombiner(client, coordStepDetail.targetTable);
                qc.sCreateStatement = coordStepDetail.targetSchema;
                qc.createTempTable();

                try {
                    qc.execute("INSERT INTO " + coordStepDetail.targetTable
                            + " " + coordStepDetail.queryString);
                } catch (SQLException se) {
                    logger.debug(" QP.eQS(): " + se.getMessage());

                    // I commented this out, since ClientRequest.java will also
                    // tryToReleaseConnections();
                    // client.tryToReleaseConnections();

                    throw new XDBServerException(
                            se.getMessage() + "\nQUERY: " + coordStepDetail.queryString,
                            se, ErrorMessageRepository.SQL_EXEC_FAILURE_CODE);
                }
                qc.finishInserts();
                qc.dropTempTables(coordStepDetail.dropList);
                return;
            }

            // only do insert select on main now when coord is both a consumer
            // and
            // producer and the nodes are not consumers
            if (coordStepDetail != null) {
                if (coordStepDetail.isConsumer && coordStepDetail.isProducer
                        && !aStepDetail.isConsumer) {
                    qc = new QueryCombiner(client, coordStepDetail.targetTable);
                    qc.sCreateStatement = coordStepDetail.targetSchema;
                    qc.createTempTable();

                    try {

                        qc.execute("INSERT INTO " + IdentifierHandler.quote(coordStepDetail.targetTable)
                                + " " + coordStepDetail.queryString);
                    } catch (SQLException se) {
                        logger.debug(" QP.eQS(): " + se.getMessage());

                        throw new XDBServerException(
                                se.getMessage() + "\nQUERY: " + coordStepDetail.queryString,
                                se, ErrorMessageRepository.SQL_EXEC_FAILURE_CODE);
                    }
                    // qc.finishInserts();
                    qc.dropTempTables(coordStepDetail.dropList);

                    return;
                }

            }

            logger.debug("- target: " + aStepDetail.targetTable);

            // Should eventually change this so that the same instance can
            // be used for the life of the connection
            qc = new QueryCombiner(client, aStepDetail.targetTable);

            logger.debug(" nodeUsageTable.size() = " + nodeUsageTable.size());

            // ------------------------------------------------------------
            // tell all of the nodes that they can join with it
            // ------------------------------------------------------------

            QueryTimer.startTimer();

            try {
                aMultinodeExecutor.executeStep(qc, aStepDetail,
                        coordStepDetail, nodeUsageTable, producerCount,
                        consumerCount);
            } catch (Exception e) {
                logger.debug(" QP.eQS(): " + e.getMessage());

                throw new XDBServerException(
                        e.getMessage() + "\nQUERY: " + aStepDetail.queryString,
                        e, ErrorMessageRepository.SQL_EXEC_FAILURE_CODE);
            }

            QueryTimer.stopTimer();

            logger.debug("+++++ Combiner Row Count = " + qc.getRowCount());

            // See if this was combined on coordinator and we need to send it
            // back
            if (aStepDetail.combineOnCoordFirst) {
                DataDownTimer.startTimer();
                try {
                    aMultinodeExecutor.sendTableDownDistinct(aStepDetail,
                            destNodeList, qc);
                } catch (Exception xdbex) {
                    logger.debug("Error: sendTableDownDistinct. "
                            + xdbex.getMessage());
                    throw new XDBServerException(
                            ErrorMessageRepository.ERROR_EXEC_STEP,
                            xdbex,
                            ErrorMessageRepository.ERROR_EXEC_STEP_CODE);
                }

                DataDownTimer.stopTimer();

                logger.debug("+++++ SendTableDownDistinct Row count = "
                        + qc.getRowCount());
            }

            logger.debug("QueryTime until now: " + QueryTimer.toString());

            return;
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Sets the results on the coordinator
     *
     * @param queryString
     * @param unionResultGroup
     * @param coordStepDetail
     * @throws java.sql.SQLException
     */
    private void setFinalResultSetOnCoordinator(String queryString,
            int unionResultGroup, StepDetail coordStepDetail)
    throws SQLException {
        final String method = "setFinalResultSetOnCoordinator";
        logger.entering(method);

        try {
            logger.debug("Query = " + queryString);

            QueryCombiner qc = new QueryCombiner(client, null);
            ResultSet coordResultSet = null;
            try {
                coordResultSet = qc.queryOnCoord(queryString);
            } catch (XDBServerException xe) {
                qc.dropTempTables(coordStepDetail.dropList);
                throw xe;
            }
            // Use ServerResultSetImpl so that we can drop temp tables later
            finalResultSet = new ServerResultSetImpl(
                    Collections.singleton(coordResultSet), null, false, -1, -1,
                    new XDBResultSetMetaData(aQueryPlan.getMetaData()));

            finalResultSet.setFinalCoordTempTableList(coordStepDetail.dropList);
            finalResultSet.setFinalNodeTempTableList(null);

            if (unionResultGroup > 0) {
                addUnionResultSet(finalResultSet, unionResultGroup);
            }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Constructor
     *
     * @param client
     * @param query
     */
    public QueryProcessor(XDBSessionContext client, QueryTree query) {
        this.client = client;
        DataDownTimer = new Timer();
        QueryTimer = new Timer();
        this.query = query;

    }

    private QueryTree query = null;

    private QueryTree resultTree = null;

    private QueryPlan aQueryPlan = null;

    private ExecutionPlan aExecPlan = null;

    /**
     * For query to update rowid or serial Actually INSERT privilege is needed
     * that already checked
     */
    private Collection skipPermissionCheck = null;

    /**
     *
     * @param skip
     */
    public void setSkipPermissionCheck(Collection skip) {
        skipPermissionCheck = skip;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IPreparable#isPrepared()
     */
    /**
     *
     * @return
     */
    public boolean isPrepared() {
        return aExecPlan != null;
    }


    /**
     * Clean up the query processor enabling to prepare and execute the same
     * query plan.
     */
    public void reset()
    {
        if (finalResultSet != null)
        {
            try {
                finalResultSet.close();
            } catch (SQLException se) {
                // ignore
            }
            finalResultSet = null;
        }
    }

    /**
     * 
     * @throws java.lang.Exception
     */
    public void prepare() throws Exception {
        prepare(false);
    }

    /**
     *
     * @throws java.lang.Exception
     */
    public void prepare(boolean createDestTable) throws Exception {

        final String method = "prepare";
        logger.entering(method);

        try {
            SysDatabase database = client.getSysDatabase();
            HashSet tables = new HashSet();
            getTablesFromQTree(database, query, tables);
            for (Iterator it = tables.iterator(); it.hasNext();) {
                SysTable sysTable = (SysTable) it.next();
                if (skipPermissionCheck == null
                        || !skipPermissionCheck.contains(sysTable
                                .getTableName().toUpperCase())) {
                    sysTable.ensurePermission(client.getCurrentUser(),
                            SysPermission.PRIVILEGE_SELECT);
                }
            }

            if ((createDestTable || !query.isInsertSelect())
                    && query.getIntoTableName() != null) {
                if (client.getCurrentUser().getUserClass() == SysLogin.USER_CLASS_STANDARD) {
                    XDBSecurityException ex = new XDBSecurityException(
                    "You are not allowed to create tables");
                    logger.throwing(ex);
                    throw ex;
                }
                if (database.isTableExists(query.getIntoTableName())
                        || client.getTempTableName(query.getIntoTableName()) != null) {
                    throw new XDBServerException(ErrorMessageRepository.DUP_TABLE_NAME
                            + "(" + query.getIntoTableName() + ")", 0,
                            ErrorMessageRepository.DUP_TABLE_NAME_CODE);
                }
            }

            QueryTree foldedTree = foldQueryTree(query);

            handleOrCondition(foldedTree);

            Optimizer anOptimizer = new Optimizer(client);
            resultTree = anOptimizer.determineQueryPath(foldedTree);

            aQueryPlan = new QueryPlan(client);
            aQueryPlan.createMainPlanFromTree(resultTree);
            aExecPlan = new ExecutionPlan(null, aQueryPlan, null, null, false,
                    client);
            aExecPlan.correctDestinations();
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Returns prepared query plan, or null if it is not prepared yet
     * @return
     */
    public QueryPlan getQueryPlan() {
        return aQueryPlan;
    }

    /**
     * Returns prepared execution plan, or null if it is not prepared yet
     * @return
     */
    public ExecutionPlan getExecPlan() {
    	return aExecPlan;
    }

    /**
     * Prepares parameters for the ExecutionPlan
     *
     * @param paramSqlExprValues - the parameter types used
     */
    public void prepareParameters (List<SqlExpression> paramSqlExprValues) {
        if (aExecPlan != null) {
            int paramTypes[] = new int[paramSqlExprValues.size()];
            for (SqlExpression aSqlExpression : paramSqlExprValues) {
                paramTypes[aSqlExpression.getParamNumber()-1] = aSqlExpression.getExprDataType().type;
            }
            aExecPlan.prepareParameters (paramTypes);
        }
    }

    /**
     * Resets the execution plan, substituting the passed in parameter values.
     *
     * @param paramSqlExprValues a Vactor containing the SqlExpression
     * parameter values.
     */
    public void resetExecPlan(List<SqlExpression> paramSqlExprValues) {

        String paramValues[] = new String[paramSqlExprValues.size()];
        for (SqlExpression aSqlExpression : paramSqlExprValues) {
            paramValues[aSqlExpression.getParamNumber()-1] = aSqlExpression.getParamValue();
        }

        aExecPlan.substituteParameterValues(paramValues);
    }

    /**
     * More efficiently execute queries where OR is prsent, where we can apply
     * limiting condition sooner.
     *
     * @param aQuery
     */
    private void handleOrCondition(QueryTree aQuery) {

        if (needsOrHandling(aQuery)) {
            handleOrQuery(aQuery);
        }

        // See if we are dealing with any subqueries
        for (Object element : aQuery.getRelationNodeList()) {
            RelationNode aRelationNode = (RelationNode) element;

            if (aRelationNode.getNodeType() == RelationNode.SUBQUERY_RELATION
                    || aRelationNode.getNodeType() == RelationNode.SUBQUERY_SCALAR
                    || aRelationNode.getNodeType() == RelationNode.SUBQUERY_NONCORRELATED) {

                // traverse down and handle subtrees
                // WITH handling
                if (aRelationNode.getSubqueryTree() != null) {
                    handleOrQuery(aRelationNode.getSubqueryTree());
                } else if (aRelationNode.isWithDerived()) {
                    handleOrQuery(aRelationNode.getBaseWithRelation().getSubqueryTree());
                }
            }
        }

        // See if we are dealing with any subqueries
        for (Object element : aQuery.getUnionQueryTreeList()) {
            QueryTree subTree = (QueryTree) element;

            handleOrQuery(subTree);
        }
    }

    /**
     *
     * @param aQuery
     */
    private void handleOrQuery(QueryTree aQuery) {

        List<QueryCondition> anOrConditions = getOrConditions(aQuery.getConditionList());
        Vector[] anAndConditions = new Vector[anOrConditions.size()];
        int i = 0;
        for (QueryCondition cond : anOrConditions) {
            Vector work = getAndConditions(cond);
            if (work != null && !work.isEmpty()) {
                anAndConditions[i++] = work;
            }

        }
        HashSet<RelationNode> aRelationNodes = new HashSet();
        if (anAndConditions.length > 0) {
            aRelationNodes
            .addAll(getRelationsOfOneNodeCondition(anAndConditions[0]));
            for (int j = 1; j < anAndConditions.length; j++) {
                HashSet<RelationNode> aRelationNodes1 = new HashSet();
                aRelationNodes1
                .addAll(getRelationsOfOneNodeCondition(anAndConditions[j]));
                aRelationNodes.retainAll(aRelationNodes1);
            }
            Vector aConditions = new Vector();
            for (int j = 0; j < aQuery.getRelationNodeList().size(); j++) {
                QueryCondition aRelQueryCondition = null;
                RelationNode aRelationNode = aQuery.getRelationNodeList()
                .get(j);
                if (aRelationNodes.contains(aRelationNode)) {
                    for (Vector element : anAndConditions) {
                        aConditions
                        .add(createCondition(element, aRelationNode));
                    }
                    aRelQueryCondition = createOperatorConditions(aConditions,
                    "OR");
                    aConditions.clear();
                    aQuery.getRelationNodeList().get(j).getConditionList()
                    .add(aRelQueryCondition);
                }

            }

        }
    }

    /**
     *
     * @param aConditions
     * @param aRelationNode
     * @return
     */
    private QueryCondition createCondition(Vector aConditions,
            RelationNode aRelationNode) {
        QueryCondition result = null;
        Vector anAndConditions = new Vector();
        for (int i = 0; i < aConditions.size(); i++) {
            QueryCondition aCond = (QueryCondition) aConditions.get(i);
            RelationNode aRel = getOneNodeCondition(aCond);
            if (aRelationNode.equals(aRel)) {
                anAndConditions.add(aCond);
            }
        }
        result = createOperatorConditions(anAndConditions, "AND");

        return result;
    }

    /**
     *
     * @param aConditions
     * @return
     */
    private Collection<RelationNode> getRelationsOfOneNodeCondition(
            Vector aConditions) {

        Collection<RelationNode> result = new HashSet<RelationNode>();

        for (int i = 0; i < aConditions.size(); i++) {
            RelationNode aRelationNode = getOneNodeCondition((QueryCondition) aConditions
                    .get(i));
            if (aRelationNode != null) {
                result.add(aRelationNode);
            }

        }
        return result;
    }

    /**
     *
     * @param aCondition
     * @return
     */
    private RelationNode getOneNodeCondition(QueryCondition aCondition) {
        RelationNode aRelationNode = null;
        if (aCondition.getCondType() == QueryCondition.QC_RELOP) {
            if (aCondition.getLeftCond().getCondType() == QueryCondition.QC_SQLEXPR) {
                if (aCondition.getLeftCond().getExpr().getExprType() == SqlExpression.SQLEX_COLUMN) {
                    aRelationNode = aCondition.getLeftCond().getExpr().getColumn().relationNode;
                }
            }
            if (aCondition.getRightCond().getCondType() == QueryCondition.QC_SQLEXPR) {
                if (aCondition.getRightCond().getExpr().getExprType() == SqlExpression.SQLEX_COLUMN) {
                    if (aRelationNode == null) {
                        aRelationNode = aCondition.getRightCond().getExpr().getColumn().relationNode;
                    } else {
                        aRelationNode = null;
                    }
                }
            }
        }

        return aRelationNode;
    }

    /**
     *
     * @param aCondition
     * @return
     */
    private Vector getAndConditions(QueryCondition aCondition) {
        Vector result = new Vector();
        if (aCondition.getCondType() == QueryCondition.QC_CONDITION
                && aCondition.getOperator().equals("AND")) {
            Vector left = getAndConditions(aCondition.getLeftCond());
            Vector right = getAndConditions(aCondition.getRightCond());
            result.addAll(left);
            result.addAll(right);
            return result;
        } else {
            result.add(aCondition);
        }
        return result;

    }

    /**
     *
     * @param aConditionList
     * @return
     */
    private List<QueryCondition> getOrConditions(List<QueryCondition> aConditionList) {
        List<QueryCondition> result = new LinkedList<QueryCondition>();
        for (int i = 0; i < aConditionList.size(); i++) {
            if (aConditionList.get(i).getCondType() != QueryCondition.QC_CONDITION) {
                continue;
            }
            result.addAll(getOrConditions(aConditionList.get(i)));
        }
        return result;
    }

    /**
     *
     * @param aCondition
     * @return
     */
    private Vector getOrConditions(QueryCondition aCondition) {
        Vector result = new Vector();
        if (aCondition.getCondType() == QueryCondition.QC_CONDITION
                && aCondition.getOperator().equals("OR")) {
            Vector left = getOrConditions(aCondition.getLeftCond());
            Vector right = getOrConditions(aCondition.getRightCond());
            result.addAll(left);
            result.addAll(right);
            return result;
        } else {
            result.add(aCondition);
        }
        return result;

    }

    /**
     *
     * @param aQuery
     * @return
     */
    private boolean needsOrHandling(QueryTree aQuery) {
        if (aQuery.getConditionList() == null || aQuery.getConditionList().size() == 0) {
            return false;
        }
        for (int i = 0; i < aQuery.getConditionList().size(); i++) {
            if (!isCanonicalCondition(aQuery.getConditionList()
                    .get(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     *
     * @param aCondition
     * @return
     */
    private boolean isCanonicalCondition(QueryCondition aCondition) {
        if (aCondition == null
                || aCondition.getCondType() == QueryCondition.QC_SQLEXPR) {
            return true;
        }
        if (aCondition.getCondType() == QueryCondition.QC_RELOP) {
            return true;
        }
        if (aCondition.getCondType() == QueryCondition.QC_CONDITION
                && aCondition.getOperator().equals("OR")) {
            if (isCanonicalCondition(aCondition.getLeftCond())) {
                return isCanonicalCondition(aCondition.getRightCond());
            }
        }
        if (aCondition.getCondType() == QueryCondition.QC_CONDITION
                && aCondition.getOperator().equals("AND")) {
            if (canMove(aCondition)) {
                return true;
            }
        }
        return false;
    }

    public ColumnMetaData[] getMetaData() {
        if (!isPrepared()) {
            try {
                prepare();
            } catch (Exception e) {
                logger.catching(e);
            }
        }
        return aQueryPlan.getMetaData();
    }

    /**
     *
     * @param engine
     * @throws java.lang.Exception
     * @return
     */
    public ExecutionResult execute(Engine engine) throws Exception {
        final String method = "execute";
        logger.entering(method);

        try {

            if (finalResultSet == null) {
                if (!isPrepared()) {
                    prepare();
                }

                // If a single step with lookups, balance it.
                if (this.isBalanceable()) {
                    setExecNode(client.getSysDatabase().getBalancer().getNextNodeId());
                }

                aMultinodeExecutor = client.getMultinodeExecutor(aQueryPlan.queryNodeTable.values());

                if (aQueryPlan.getIntoTable() != null) {
                    // Update last step with tablespace info
                    ExecutionPlan updatePlan;
                    // check for UNION to get right step.
                    if (aExecPlan.unionPlanList != null && aExecPlan.unionPlanList.size() > 0) {
                        updatePlan = aExecPlan.unionPlanList.get(aExecPlan.unionPlanList.size()-1);
                    } else {
                        updatePlan = aExecPlan;
                    }
                    setLastStepTablespace(updatePlan.stepList.get(updatePlan.stepList.size() - 1));
                }
                executeQueryExecPlan(aExecPlan);

                if (aExecPlan.isTopLevelUnion) {
                    ServerResultSetImpl unionResultSet1 = new ServerResultSetImpl(
                            unionGroup1, aQueryPlan.sortInfo, true,
                            aExecPlan.getLimit(), aExecPlan.getOffset(),
                            new XDBResultSetMetaData(aQueryPlan.getMetaData()));

                    if (unionGroup2 == null) {
                        // We just have a simple UNION
                        finalResultSet = unionResultSet1;
                    } else {
                        // We are dealing with a case where we have
                        // some UNIONs followed by zero or more UNION ALL.
                        // We combine the UNIONs up to now.
                        Vector<ServerResultSetImpl> aRSVector = new Vector<ServerResultSetImpl>();

                        aRSVector.add(unionResultSet1);
                        aRSVector.addAll(unionGroup2);

                        finalResultSet = new ServerResultSetImpl(aRSVector,
                                aQueryPlan.sortInfo, false,
                                aExecPlan.getLimit(), aExecPlan.getOffset(),
                                new XDBResultSetMetaData(
                                        aQueryPlan.getMetaData()));
                    }

                    // We update temp table drop info in the finalResultSet
                    finalResultSet.setFinalCoordTempTableList(aExecPlan.coordTempTableDropList);
                    finalResultSet.setFinalNodeTempTableList(aExecPlan.nodeTempTableDropList);
                } else if (aExecPlan.combineResults) {
                    // amart: MultinodeExecutor should take care about
                    // collecting final ResultSets
                    ExecutionStep lastStep = aExecPlan.stepList.get(aExecPlan.stepList.size()-1);
                    Collection<? extends ResultSet> aRSVector = aMultinodeExecutor.getFinalResultSets();
                    if (aRSVector.size() == 1
                            && lastStep.aStepDetail.dropList.size() == 0) {
                        ResultSet rs = aRSVector.iterator().next();
                        if (rs instanceof ResultSetImpl) {
                            ((ResultSetImpl) rs).setColumnMetaData(aQueryPlan.getMetaData());
                        }
                        return ExecutionResult.createResultSetResult(
                                ExecutionResult.COMMAND_SELECT, rs);
                    } else {
                        // Set up combined result set
                        finalResultSet = new ServerResultSetImpl(aRSVector,
                                aQueryPlan.sortInfo, aQueryPlan.isDistinct,
                                aExecPlan.getLimit(), aExecPlan.getOffset(),
                                new XDBResultSetMetaData(
                                        aQueryPlan.getMetaData()));

                        // We update temp table drop info in the finalResultSet
                        finalResultSet.setFinalCoordTempTableList(null);
                        finalResultSet.setFinalNodeTempTableList(lastStep.aStepDetail.dropList);
                    }
                }
            }

            if (aQueryPlan.getIntoTable() != null
                    && (aExecPlan.isTopLevelUnion || aExecPlan.combineResults)) {
                ExecutionPlan anExecPlan = aExecPlan;
                while (anExecPlan.stepList.size() == 0) {
                    anExecPlan = anExecPlan.unionPlanList.get(0);
                }
                ExecutionStep lastStep = anExecPlan.stepList.get(anExecPlan.stepList.size() - 1);
                aMultinodeExecutor.execute(lastStep.aStepDetail.targetSchema,
                        aQueryPlan.getIntoTable().getNodeList(), true);
                if (Props.XDB_USE_LOAD_FOR_STEP) {
                    Loader loader = new Loader(finalResultSet, null);
                    int partColumnIdx = -1;
                    SysColumn partColumn = aQueryPlan.getIntoTable().getPartitionedColumn();
                    if (partColumn != null) {
                        partColumnIdx = partColumn.getColSeq() - 1;
                    }
                    NodeDBConnectionInfo[] nodeInfos = new NodeDBConnectionInfo[aQueryPlan.getIntoTable().getNodeList().size()];
                    int i = 0;
                    for (DBNode dbNode : aQueryPlan.getIntoTable().getNodeList()) {
                        nodeInfos[i++] = dbNode.getNodeDBConnectionInfo();
                    }

                    loader.setTargetTableInfoExt(
                            lastStep.aStepDetail.targetTable, partColumnIdx,
                            null, aQueryPlan.getIntoTable().getPartitionMap(),
                            partColumn == null ? null : new ExpressionType(
                                    partColumn), -1, null, false, false,
                            nodeInfos);
                    try {
                        // loader.setVerbose(logger.isDebugEnabled());
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
                    } catch (Exception e) {
                        logger.catching(e);
                        XDBServerException ex = new XDBServerException(
                                "Can not send data to Nodes", e);
                        logger.throwing(ex);
                        throw ex;
                    }
                } else {
                    int partitionColumnIdx = 0;
                    StringBuffer sbInsert = new StringBuffer("INSERT INTO ");
                    sbInsert.append(
                            IdentifierHandler.quote(lastStep.aStepDetail.targetTable)).append(
                            " (");
                    for (SysColumn sysCol : aQueryPlan.getIntoTable().getColumns()) {
                        String colName = sysCol.getColName();
                        sbInsert.append(IdentifierHandler.quote(colName)).append(
                                ", ");
                    }
                    SysColumn partCol = aQueryPlan.getIntoTable().getPartitionedColumn();
                    ExpressionType partColType = null;
                    if (partCol != null) {
                        partitionColumnIdx = aQueryPlan.getIntoTable().getSysColumn(
                                partCol.getColName()).getColSeq() + 1;
                        partColType = new ExpressionType(partCol);
                    }
                    sbInsert.setLength(sbInsert.length() - 2);
                    sbInsert.append(") VALUES (");
                    ResultSetMetaData rsmd = finalResultSet.getMetaData();
                    int colCount = rsmd.getColumnCount();
                    while (finalResultSet.next()) {
                        StringBuffer sbValues = new StringBuffer();
                        int i = 1;
                        String partitioningValue = null;
                        if (partitionColumnIdx > 0
                                && partitionColumnIdx <= colCount) {
                            partitioningValue = finalResultSet.getString(partitionColumnIdx);
                        } else if (partCol != null) {
                            SqlExpression partExpr = partCol.getDefaultExpr(client);
                            if (partExpr != null) {
                                partitioningValue = partExpr.rebuildString();
                            }
                        }
                        Collection<DBNode> targetNodes = aQueryPlan.getIntoTable().getNode(
                                SqlExpression.createConstantExpression(
                                        partitioningValue, partColType).getNormalizedValue());
                        for (SysColumn sysCol : aQueryPlan.getIntoTable().getColumns()) {
                            String value = null;
                            if (i == partitionColumnIdx) {
                                value = partitioningValue;
                            } else {
                                if (i <= colCount) {
                                    value = finalResultSet.getString(i);
                                } else {
                                    SqlExpression defExpr = sysCol.getDefaultExpr(client);
                                    if (defExpr != null) {
                                        value = defExpr.rebuildString();
                                    }
                                }
                            }

                            if (value != null && RSHelper.getQuoteInfo(sysCol)) {
                                value = "'" + value.replaceAll("'", "''") + "'";
                                switch (sysCol.getColType()) {
                                case ExpressionType.TIME_TYPE:
                                    value = SqlExpression.normalizeTime(value);
                                    break;
                                case ExpressionType.TIMESTAMP_TYPE:
                                    value = SqlExpression.normalizeTimeStamp(value);
                                    break;
                                case ExpressionType.DATE_TYPE:
                                    value = SqlExpression.normalizeDate(value);
                                    break;
                                default:
                                    break;
                                }

                            }
                            sbValues.append(value);
                            sbValues.append(", ");
                        }
                        sbValues.setLength(sbValues.length() - 2);
                        String insertStr = sbInsert.toString()
                                + sbValues.toString() + ")";
                        if (engine.addToBatchOnNodes(insertStr, targetNodes,
                                client)) {
                            executeCurrentBatch(engine);
                        }
                    }
                    executeCurrentBatch(engine);
                }
            }
            // If we are dealing with a SELECT INTO, we have to "create"
            // the final table.
            if (aQueryPlan.getIntoTable() != null) {
                // Careful with INSERT SELECT
                if (aQueryPlan.isExistingInto()) {
                    return ExecutionResult.createSuccessResult(ExecutionResult.COMMAND_SELECT);
                } else {
                    ExecutionPlan anExecPlan = aExecPlan;
                    while (anExecPlan.stepList.size() == 0) {
                        anExecPlan = anExecPlan.unionPlanList.get(0);
                    }
                    ExecutionStep lastStep = anExecPlan.stepList.get(anExecPlan.stepList.size() - 1);
                    return createIntoTable(lastStep);
                }
            }
            return ExecutionResult.createResultSetResult(
                    ExecutionResult.COMMAND_SELECT, finalResultSet);
        } finally {
            logger.exiting(method);
        }
    }

    private void executeCurrentBatch(Engine engine) throws XDBServerException {
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

    /**
     * @return whether or nor the final tablespace for SELECT INTO
     * can be already set.
     */
    private boolean canPredefineTablespace() {
        return aQueryPlan.getIntoTable().isTemporary()
        && Props.XDB_TEMPORARY_INTERMEDIATE_TABLES
        || !aQueryPlan.getIntoTable().isTemporary()
        && !Props.XDB_TEMPORARY_INTERMEDIATE_TABLES;
    }

    /**
     * Modify the last step to use appropriate tablespace if possible
     * instead of trying to do that at the end
     *
     * @param lastStep the final step being modified to set tablespace info
     */
    private void setLastStepTablespace (ExecutionStep lastStep) {

        if (canPredefineTablespace()) {
            SysTablespace tablespace = aQueryPlan.getIntoTable().getTablespace();
            if (tablespace != null) {
                if (lastStep.aStepDetail.isConsumer) {
                    for (Integer nodeInt : tablespace.getLocations().keySet()) {
                        lastStep.aStepDetail.addCreateTablespace(nodeInt,
                                tablespace.getNodeTablespaceName(nodeInt));
                    }
                }
            }
        }
    }

    /**
     * If final table and target table have different persistence status create
     * new tables on nodes using SELECT INTO commands
     * @param lastStep
     * @return
     */
    private ExecutionResult createIntoTable(ExecutionStep lastStep) {
        if (aQueryPlan.getIntoTable().isTemporary()) {
            if (Props.XDB_TEMPORARY_INTERMEDIATE_TABLES) {
                // Just rename temp table on nodes
                renameFinalTable(lastStep.aStepDetail.targetTable,
                        aQueryPlan.getIntoTable().getTableName());
            } else {
                // Create temp table and copy over data
                copyFinalTable(lastStep.aStepDetail.targetTable,
                        aQueryPlan.getIntoTable().getTableName(), true);
            }
            client.getSysDatabase().addSysTempTable(aQueryPlan.getIntoTable());
            client.registerTempTableWithSession(
                    aQueryPlan.getIntoTableReferenceName(),
                    aQueryPlan.getIntoTable().getTableName());
        } else {
            SyncCreateTable syncCreateTable = aQueryPlan.getSyncCreateTable();
            MetaData.getMetaData().beginTransaction();
            try {
                syncCreateTable.execute(client);
                if (Props.XDB_TEMPORARY_INTERMEDIATE_TABLES) {
                    // Create table and copy over data
                    copyFinalTable(lastStep.aStepDetail.targetTable,
                            aQueryPlan.getIntoTable().getTableName(), false);
                } else {
                    // Just rename table on nodes
                    renameFinalTable(lastStep.aStepDetail.targetTable,
                            aQueryPlan.getIntoTable().getTableName());
                }
                MetaData.getMetaData().commitTransaction(syncCreateTable);
            } catch (Exception ex) {
                MetaData.getMetaData().rollbackTransaction();
                throw new XDBServerException("Failed to create target table",
                        ex);
            }
        }
        return ExecutionResult.createSuccessResult(ExecutionResult.COMMAND_SELECT);
    }

    /**
     * If final table and target table have the same persistence status rename
     * tables on nodes
     * @param oldTable
     * @param newTable
     */
    private void renameFinalTable(String oldTable, String newTable) {
        HashMap<String, String> m = new HashMap<String, String>();
        m.put("oldname", IdentifierHandler.quote(oldTable));
        m.put("newname", IdentifierHandler.quote(newTable));
        String renameCmd = ParseCmdLine.substitute(
                Props.XDB_SQLCOMMAND_RENAMETABLE_TEMPLATE, m);
        aMultinodeExecutor.executeCommand(renameCmd,
                aQueryPlan.getIntoTable().getNodeList(), true);
    }

    /**
     * Copies the final table in case we need to switch
     * temporary status on CREATE TABLE AS.
     */
    private void copyFinalTable(String oldTable, String newTable,
            boolean temporary) {
        HashMap<String, String> m = new HashMap<String, String>();
        m.put("oldname", IdentifierHandler.quote(oldTable));
        m.put("newname", IdentifierHandler.quote(newTable));
        String copyCmd = ParseCmdLine.substitute(
                temporary ? Props.XDB_SQLCOMMAND_SELECTINTOTEMP_TEMPLATE
                        : Props.XDB_SQLCOMMAND_SELECTINTO_TEMPLATE, m);

        SysTablespace tablespace = aQueryPlan.getIntoTable().getTablespace();
        if (tablespace != null) {
            Map<DBNode,String> commandMap = new HashMap<DBNode,String>();
            for (Integer nodeID : tablespace.getLocations().keySet()) {
                int pos = copyCmd.toUpperCase().indexOf("AS SELECT ");
                String copySpaceCmd = copyCmd.substring(0, pos)
                + " TABLESPACE "
                + tablespace.getNodeTablespaceName(nodeID)
                + " " + copyCmd.substring(pos);
                commandMap.put(client.getSysDatabase().getDBNode(nodeID),
                        copySpaceCmd);
            }
            aMultinodeExecutor.executeCommand(commandMap, true);
        } else {
            aMultinodeExecutor.executeCommand(copyCmd,
                    aQueryPlan.getIntoTable().getNodeList(), true);
        }
        aMultinodeExecutor.executeCommand("DROP TABLE " + m.get("oldname"),
                aQueryPlan.getIntoTable().getNodeList(), true);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#getCost()
     */
    private long cost = 0;

    /**
     *
     * @return
     */
    public long getCost() {
        final String method = "getCost";
        logger.entering(method);

        try {
            if (cost == 0) {
                try {
                    if (!isPrepared()) {
                        prepare();
                    }
                    cost = resultTree.getCost();
                    // Add any unioned subqueries to it.
                    for (Object element : resultTree.getUnionQueryTreeList()) {
                        QueryTree tree = (QueryTree) element;
                        cost += tree.getEstimatedCost();
                    }
                } catch (Exception e) {

                }
            }
            return cost;
        } finally {
            logger.exiting(method);
        }

    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#getLockSpecs(java.lang.Object)
     */
    /**
     *
     * @return
     */
    public LockSpecification<SysTable> getLockSpecs() {
        final String method = "getLockSpecs";
        logger.entering(method);

        try {
            SysDatabase database = client.getSysDatabase();
            HashSet<SysTable> readObjects = new HashSet<SysTable>();
            getTablesFromQTree(database, query, readObjects);
            Collection<SysTable> empty = Collections.emptyList();
            return new LockSpecification<SysTable>(readObjects, empty);
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Checks to see if the ExecutionPlan is made up of a single step on a
     * single node involving replicated tables.
     *
     * @return
     */
    public boolean isBalanceable() {
        if (isPrepared() && aExecPlan.stepList.size() == 1) {
            ExecutionStep anExecStep = aExecPlan.stepList.get(0);

            if (anExecStep.isLookupStep
                    && !anExecStep.aStepDetail.isConsumer
                    && anExecStep.correlatedSubPlan == null
                    && anExecStep.uncorrelatedSubPlanList == null
                    && aExecPlan.relationPlanList.size() == 0
                    && aExecPlan.unionPlanList.size() == 0
                    && aExecPlan.scalarPlanList.size() == 0) {
                return true;
            }
        }

        return false;
    }

    /**
     * For single node lookups, set the node to execute it on.
     *
     * @param nodeId
     */
    public void setExecNode(int nodeId) {
        ExecutionStep anExecStep = aExecPlan.stepList.get(0);

        anExecStep.setSingleExecNode(nodeId);
        aQueryPlan.setSingleQueryNode(nodeId);
    }

    /**
     *
     * @param query
     * @param database
     * @param readObjects
     */
    private void getTablesFromQTree(SysDatabase database, QueryTree query,
            Collection readObjects) {
        if (query == null) {
            return;
        }
        for (RelationNode aRelNode : query.getRelationNodeList()) {
            if (aRelNode.getNodeType() == RelationNode.TABLE) {
                String tableName = aRelNode.getTableName();
                SysTable sysTable = database.getSysTable(tableName);
                addWithChildren(sysTable, readObjects);
            } else {
                // We have a subquery
                getTablesFromQTree(database, aRelNode.getSubqueryTree(), readObjects);
            }
        }
        for (QueryTree unionTree : query.getUnionQueryTreeList()) {
            getTablesFromQTree(database, unionTree, readObjects);
        }
    }

    /**
     *
     * @param table
     * @param collection
     */
    private void addWithChildren(SysTable table, Collection collection) {
        if (!collection.contains(table)) {
            collection.add(table);
            Collection children = table.getChildrenTables();
            if (children != null) {
                for (Iterator it = children.iterator(); it.hasNext();) {
                    addWithChildren((SysTable) it.next(), collection);
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Parser.IXDBSql#getNodeList()
     */
    /**
     *
     * @return
     */
    public Collection<DBNode> getNodeList() {
        HashSet<DBNode> nodes = new HashSet<DBNode>();
        SysDatabase database = client.getSysDatabase();
        for (RelationNode aRelNode : query.getRelationNodeList()) {
            if (aRelNode.getNodeType() == RelationNode.TABLE) {
                String tableName = aRelNode.getTableName();
                SysTable sysTable = database.getSysTable(tableName);
                nodes.addAll(sysTable.getNodeList());
            }
        }
        SysTable intoTable = null;
        try {
            intoTable = query.createIntoTable(client);
        } catch (Exception ex) {
        }
        if (intoTable != null) {
            nodes.addAll(intoTable.getNodeList());
        }
        return nodes;
    }

    /**
     *
     * @param aQuery
     * @return
     */
    private QueryTree foldQueryTree(QueryTree aQuery) {
        QueryTree resultTree = null;

        switch (assignCase(aQuery)) {

        case 0: // the QueryTree has not have subquery or
            // this case does not define on the spec
            resultTree = aQuery;
            break;
        case 1:
        case 2: // Cases #1 and #2 Basic and Basic with condition
            resultTree = remakeQuery(aQuery, 1);
            break;
        case 3: // Cases #3 Aggregates with simple condition
            resultTree = remakeQuery(aQuery, 3);
            break;
        case 4: // Cases #4 Agregates and join
            resultTree = remakeQuery(aQuery, 5);
            break;
        case 5: // Cases #5 Aggregates using a view defined as an aggregate
            resultTree = remakeQuery(aQuery, 5);
            break;
        case 6: // Cases #6 Union with simple condition
            resultTree = remakeQuery(aQuery, 6);
            break;
        case 7: // Cases #7 Union with join
            resultTree = remakeQuery(aQuery, 7);
            break;

        default: // the QueryTree has not have subquery or
            // this case does not define on the sprc
            resultTree = aQuery;
        break;
        }

        return resultTree;
    }

    /**
     *
     * @param qc
     * @param aSubNode
     * @return
     */
    private QueryCondition getNewCondition(QueryCondition qc,
            RelationNode aSubNode) {
        Vector relNodes = new Vector();
        Vector ColumnList = new Vector();

        if (qc == null) {
            return null;
        }
        qc.getRelationNodeList().clear();
        qc.getColumnList().clear();

        if (qc.getCondType() == QueryCondition.QC_COMPOSITE) /*
         * 1001 -
         * INCLAUSE,BETWEEN
         * CLAUSE etc.
         */
        {

        } else if (qc.getCondType() == QueryCondition.QC_CONDITION || /*
         * 1000- AND
         * OR
         */
                qc.getCondType() == QueryCondition.QC_RELOP) /* 10000 - (<, =, >,>=,<=) */
        {
            qc.setLeftCond(getNewCondition(qc.getLeftCond(), aSubNode));
            relNodes.addAll(qc.getLeftCond().getRelationNodeList());
            ColumnList.addAll(qc.getLeftCond().getColumnList());
            qc.setRightCond(getNewCondition(qc.getRightCond(), aSubNode));
            relNodes.addAll(qc.getRightCond().getRelationNodeList());
            ColumnList.addAll(qc.getRightCond().getColumnList());
        } else if (qc.getCondType() == QueryCondition.QC_SQLEXPR) /*
         * 0100 - Any
         * SqlExpression
         */
        {
            qc.setExpr(remakeExpression(qc.getExpr(), aSubNode));
            Vector exps = SqlExpression.getNodes(qc.getExpr(), SqlExpression.SQLEX_COLUMN);
            for (Iterator iter = exps.iterator(); iter.hasNext();) {
                SqlExpression exp = (SqlExpression) iter.next();
                relNodes.add(exp.getColumn().relationNode);
                ColumnList.add(exp.getColumn());
            }
        }
        for (int i = 0; i < ColumnList.size(); i++) {
            ((AttributeColumn) ColumnList.get(i))
            .setParentQueryCondition(qc);
        }
        qc.setRelationNodeList(relNodes);
        qc.setColumnList(ColumnList);
        qc.rebuildCondString();
        return qc;
    }

    /**
     *
     * @param aQuery
     * @return
     */
    private int assignCase(QueryTree aQuery) {
        int result = 0;
        if (aQuery.getRelationSubqueryList() == null
                || aQuery.getRelationSubqueryList().size() == 0) {
            return 0;
        }

        if (aQuery.getRelationSubqueryList().size() != 1) {
            return 0;
        }

        if (aQuery.getOrderByList().size() != 0) {
            return 0;
        }

        for (RelationNode aRelNode : aQuery.getRelationNodeList()) {
            if (aRelNode.getSubqueryTree() != null
                    && aRelNode.getSubqueryTree().getOrderByList().size() != 0) {
                return 0;
            }
            if (aRelNode.getNodeType() != RelationNode.SUBQUERY_RELATION) {
                return 0;
            }

            if (aRelNode.getSubqueryTree().isHasUnion()
                    && !aRelNode.getSubqueryTree().isContainsAggregates()
                    && !aRelNode.getSubqueryTree().isCorrelatedSubtree()
                    && !aRelNode.getSubqueryTree().isPartOfExistClause()
                    && !aRelNode.getSubqueryTree().isDistinct()
                    && aRelNode.getSubqueryTree().getLastOuterLevel() == -1) {
                if (aQuery.isCorrelatedSubtree() || aQuery.isDistinct()
                        || aQuery.isContainsAggregates()
                        || aQuery.isPartitionedGroupBy()
                        || aQuery.isPartOfExistClause()
                        || aQuery.getLastOuterLevel() != -1) {
                    return 0;
                }
                if (aQuery.getConditionList().isEmpty()
                        && aQuery.getWhereRootCondition() == null) {
                    if (aQuery.getRelationNodeList() != null
                            && aQuery.getRelationNodeList().size() == 1) {
                        return 6;
                    } else {
                        return 0;
                    }
                } else {
                    return 7;
                }

            }
            if (aRelNode.getSubqueryTree().isContainsAggregates()
                    && !aRelNode.getSubqueryTree().isHasUnion()
                    && !aRelNode.getSubqueryTree().isCorrelatedSubtree()
                    && !aRelNode.getSubqueryTree().isPartOfExistClause()
                    && !aRelNode.getSubqueryTree().isDistinct()
                    && aRelNode.getSubqueryTree().getLastOuterLevel() == -1) {// case 3 or
                // 4 or 5
                if (aQuery.isCorrelatedSubtree() || aQuery.isDistinct()
                        || aQuery.isPartitionedGroupBy()
                        || aQuery.isPartOfExistClause()
                        || aQuery.getLastOuterLevel() != -1) {
                    return 0;
                }
                if (aQuery.isContainsAggregates()) {
                    return 5;
                }
                if (!aQuery.getConditionList().isEmpty()) {
                    return 4;
                }
                if (aQuery.getConditionList().isEmpty()) {
                    if (hasAggFunction(aQuery.getWhereRootCondition())
                            && canMove(aQuery.getWhereRootCondition())) {
                        return 3;
                    } else {
                        if (aQuery.getWhereRootCondition() == null) {
                            return 3;
                        } else {
                            return 0;
                        }
                    }
                } else {
                    return 0;
                }

            }
            if (!aRelNode.getSubqueryTree().isContainsAggregates()
                    && !aRelNode.getSubqueryTree().isHasUnion()
                    && !aRelNode.getSubqueryTree().isCorrelatedSubtree()
                    && !aRelNode.getSubqueryTree().isPartOfExistClause()
                    && !aRelNode.getSubqueryTree().isDistinct()
                    && aRelNode.getSubqueryTree().getLastOuterLevel() == -1) {// case 1 or
                // 2
                if (aQuery.isCorrelatedSubtree() || aQuery.isDistinct()
                        || aQuery.isContainsAggregates()
                        || aQuery.isPartitionedGroupBy()
                        || aQuery.isPartOfExistClause()
                        || aQuery.getLastOuterLevel() != -1 || aQuery.isHasUnion()) {
                    return 0;
                }
                return 1;
            }

        }

        return result;
    }

    /**
     *
     * @param anExp
     * @param aSubNode
     * @return
     */
    private SqlExpression remakeExpression(SqlExpression anExp,
            RelationNode aSubNode) {
        if (anExp.getExprType() == SqlExpression.SQLEX_COLUMN) {
            if (anExp.getMappedExpression() != null) {
                if (anExp.getColumn().relationNode == aSubNode) {
                    return anExp.getMappedExpression();
                }
            }
            return anExp;
        } else if (anExp.getExprType() == SqlExpression.SQLEX_CONSTANT) {
            return anExp;
        } else if (anExp.getExprType() == SqlExpression.SQLEX_FUNCTION) {
            if (anExp.getFunctionParams() != null
                    && anExp.getFunctionParams().size() != 0) {
                List<SqlExpression> paramList = new ArrayList<SqlExpression>();
                for (Object element : anExp.getFunctionParams()) {
                    SqlExpression el1 = (SqlExpression) element;
                    paramList.add(remakeExpression(el1, aSubNode));
                }
                anExp.setFunctionParams(paramList);
            }
            return anExp;
        } else if (anExp.getExprType() == SqlExpression.SQLEX_OPERATOR_EXPRESSION) {
            anExp.setLeftExpr(remakeExpression(anExp.getLeftExpr(), aSubNode));
            anExp.setRightExpr(remakeExpression(anExp.getRightExpr(), aSubNode));
        } else if (anExp.getExprType() == SqlExpression.SQLEX_CASE) {
        	SqlExpression.SCase newCase = anExp.new SCase();
        	for (Map.Entry<QueryCondition, SqlExpression> entry : anExp.getCaseConstruct().getCases().entrySet()) {
        		for (QueryCondition qcExp : QueryCondition.getNodes(entry.getKey(), QueryCondition.QC_SQLEXPR)) {
        			qcExp.setExpr(remakeExpression(qcExp.getExpr(), aSubNode));
        		}
        		newCase.addCase(entry.getKey(), remakeExpression(entry.getValue(), aSubNode));
        	}
        	SqlExpression defExpr = anExp.getCaseConstruct().getDefaultexpr();
        	if (defExpr != null) {
        		newCase.setDefaultexpr(remakeExpression(defExpr, aSubNode));
        	}
        	anExp.setCaseConstruct(newCase);
        }
        return anExp;

    }

    /**
     *
     * @param aWhereCond
     * @param anAddCond
     * @return
     */
    private QueryCondition addConditionToWhere(QueryCondition aWhereCond,
            QueryCondition anAddCond) {
        if (aWhereCond == null) {
            return null;
        }
        QueryCondition newCond = new QueryCondition();
        if (anAddCond != null) {
            newCond.setLeftCond(anAddCond);
            newCond.setRightCond(aWhereCond);
            newCond.setCondType(QueryCondition.QC_CONDITION);
            newCond.setOperator("AND");
            newCond.rebuildCondString();
        } else {
            newCond = aWhereCond;
        }
        return newCond;

    }

    /**
     *
     * @param aQuery
     * @param aCase
     * @return
     */
    private QueryTree remakeQuery(QueryTree aQuery, int aCase) {
        if (aCase >= 1 || aCase <= 7) {
            List<SqlExpression> prList = new ArrayList<SqlExpression>();
            RelationNode relNode = aQuery.getRelationSubqueryList().get(0);
            if (aCase == 5 || aCase == 4) {
                QueryCondition cond = aQuery.getWhereRootCondition();
                if (cond != null) {
                    List<RelationNode> relNodes = cond.getRelationNodeList();
                    if (relNodes.size() == 1 && relNodes.get(0) == relNode) {
                        QueryCondition subCond = relNode.getSubqueryTree().getWhereRootCondition();
                        QueryCondition newCond = createNewCondition(
                                aQuery.getWhereRootCondition(), null);
                        newCond = addConditionToWhere(newCond, subCond);
                        newCond.setParentQueryTree(relNode.getSubqueryTree());
                        for (QueryCondition qc : QueryCondition.getNodes(
                                newCond, QueryCondition.QC_SQLEXPR)) {
                            qc.getExpr().moveDown(relNode);
                        }
                        relNode.getSubqueryTree().setWhereRootCondition(newCond);
                        aQuery.getFromClauseConditions().clear();
                        relNode.getConditionList().clear();
                        relNode.getCondColumnList().clear();
                        relNode.getSubqueryTree().getConditionList().add(
                                newCond);
                    }
                    aQuery.rebuildString();
                }
                return aQuery;
            }
            if (aCase == 7) {
                QueryCondition cond = aQuery.getWhereRootCondition();
                if (cond != null) {
                    List<RelationNode> relNodes = cond.getRelationNodeList();
                    if (relNodes.size() == 1
                            && relNodes.get(0) == relNode) {
                        QueryCondition subCond = aQuery.getRelationSubqueryList()
                        .get(0).getSubqueryTree()
                        .getWhereRootCondition();
                        QueryCondition newCond = addConditionToWhere(aQuery
                                .getWhereRootCondition(), subCond);
                        aQuery.setWhereRootCondition(null);
                        aQuery.getRelationSubqueryList().get(0).getSubqueryTree()
                        .setWhereRootCondition(newCond);
                    }
                    aQuery.rebuildString();
                }
                return aQuery;
            }

            if (aCase == 6) {
                // Subquery with union
                QueryTree subTree = relNode.getSubqueryTree();
                List<List<SqlExpression>> newProjections = new ArrayList<List<SqlExpression>>(
                        subTree.getUnionQueryTreeList().size());
                for (int i = 0; i < subTree.getUnionQueryTreeList().size(); i++) {
                    newProjections.add(new ArrayList<SqlExpression>(
                            aQuery.getProjectionList().size()));
                }
                // Move down projection list from outermost QueryTree
                // for each item find referenced in the first union'ed query
                // then assign alias to other union'ed queries by position.
                for (SqlExpression exp : aQuery.getProjectionList()) {
                    int idx = findInProjectList(exp,
                            subTree.getProjectionList());
                    SqlExpression newExpr = subTree.getProjectionList().get(idx).copy();
                    newExpr.setAlias(exp.getAlias());
                    prList.add(newExpr);
                    QueryCondition whereCond = subTree.getWhereRootCondition();
                    if (whereCond != null) {
                        subTree.setWhereRootCondition(changeExpression(
                                whereCond,
                                subTree.getProjectionList().get(idx), newExpr));
                    }
                    Iterator<List<SqlExpression>> itNewProjections = newProjections.iterator();
                    for (QueryTree unionedTree : subTree.getUnionQueryTreeList()) {
                        newExpr = unionedTree.getProjectionList().get(idx);
                        newExpr.setAlias(exp.getAlias());
                        itNewProjections.next().add(newExpr);
                        whereCond = unionedTree.getWhereRootCondition();
                        if (whereCond != null) {
                            unionedTree.setWhereRootCondition(changeExpression(
                                    whereCond,
                                    unionedTree.getProjectionList().get(idx),
                                    newExpr));
                        }
                    }
                }
                subTree.setProjectionList(prList);
                Iterator<List<SqlExpression>> itNewProjections = newProjections.iterator();
                // change projList for union;
                for (QueryTree union : subTree.getUnionQueryTreeList()) {
                    union.setProjectionList(itNewProjections.next());
                }
                subTree.setLimit(aQuery.getLimit());
                subTree.setOffset(aQuery.getOffset());
                subTree.copyIntoTableInfo(aQuery);
                return subTree;
            }
            if (aCase == 3 || aCase == 1 || aCase == 2) {
                List<SqlExpression> anAllProjectLict = aQuery.getProjectionList();
                List<RelationNode> anAllRelNodes = aQuery.getRelationNodeList();
                List<SqlExpression> aSubProjectLict = relNode.getSubqueryTree().getProjectionList();
                QueryCondition anAllWhere = aQuery.getWhereRootCondition();
                QueryCondition aSubWhere = relNode.getSubqueryTree()
                .getWhereRootCondition();
                List<QueryCondition> anAllConditions = aQuery.getConditionList();
                Vector aNewConditions = new Vector();
                List<QueryCondition> aSubConditions = relNode.getSubqueryTree().getConditionList();

                // remake projList
                List<SqlExpression> newPrList = new ArrayList<SqlExpression>(
                        anAllProjectLict.size());
                for (SqlExpression exp : anAllProjectLict) {
                    SqlExpression newExp = remakeExpression(exp, relNode);
                    newExp = changeInExpression(newExp, aSubProjectLict,
                            aSubProjectLict);
                    newExp.setBelongsToTree(relNode.getSubqueryTree());
                    newPrList.add(newExp);
                }
                anAllConditions.addAll(aSubConditions);
                for (Object element : anAllConditions) {
                    QueryCondition qc = (QueryCondition) element;
                    qc.getRelationNodeList().remove(relNode);
                    aNewConditions.add(getNewCondition(qc, relNode));
                }

                QueryCondition aNewWhere = new QueryCondition();
                aNewWhere = addConditionToWhere(anAllWhere, aSubWhere);
                // aNewWhere = getNewCondition(aNewWhere,relNode);
                QueryCondition aNewHaving = null;
                if (hasAggFunction(aNewWhere)) {
                    aNewHaving = new QueryCondition();
                    Vector anAndCond = getAndCondition(aNewWhere);
                    Vector havingConditions = new Vector();
                    for (Iterator iter = anAndCond.iterator(); iter.hasNext();) {
                        QueryCondition qc = (QueryCondition) iter.next();
                        if (hasAggFunction(qc)) {
                            havingConditions.add(qc);
                        }
                    }
                    aNewHaving = createOperatorConditions(havingConditions,
                    "AND");
                    aNewHaving = getNewCondition(aNewHaving, relNode);
                    anAndCond.removeAll(havingConditions);
                    aNewWhere = createOperatorConditions(anAndCond, "AND");

                }
                QueryTree newQuery = relNode.getSubqueryTree();
                newQuery.setProjectionList(newPrList);
                newQuery.setConditionList(aNewConditions);
                newQuery.setWhereRootCondition(aNewWhere);
                // if(aNewWhere != null)
                // {
                // aQuery.conditionList.add(aNewWhere);
                // }
                if (aNewHaving != null) {
                    newQuery.getHavingList().add(aNewHaving);
                }
                anAllRelNodes.remove(relNode);
                newQuery.getRelationNodeList().addAll(anAllRelNodes);
                for (int i = 0; i < newQuery.getRelationNodeList().size(); i++) {
                    newQuery.getRelationNodeList().get(i).setNodeId(i + 1);
                }

                if (relNode.getConditionList().size() != 0) {
                    // QueryCondition nn = getNewCondition((QueryCondition)
                    // relNode.conditionList.get(0),relNode);
                    QueryCondition qc = relNode.getConditionList()
                    .get(0);
                    Vector sqlExprList = QueryCondition.getNodes(qc,
                            QueryCondition.QC_SQLEXPR);
                    for (Iterator iter = sqlExprList.iterator(); iter.hasNext();) {
                        QueryCondition el = (QueryCondition) iter.next();
                        if (el.getCondType() == QueryCondition.QC_SQLEXPR
                                && el.getExpr().getMappedExpression() != null) {
                            SqlExpression expr = el.getExpr().getMappedExpression();
                            // expr.alias = expr.column.columnName;
                            expr.getColumn().relationNode.getConditionList()
                            .add(getNewCondition(qc, relNode));

                        }
                    }
                }
                createJoinListOfRelNodes(newQuery.getRelationNodeList(),
                        newQuery.getConditionList());
                newQuery.setLimit(aQuery.getLimit());
                newQuery.setOffset(aQuery.getOffset());
                newQuery.copyIntoTableInfo(aQuery);
                return foldQueryTree(newQuery);
            }
        }
        return aQuery;
    }

    /**
     * @param relationNodeList
     * @param conditionList
     */
    private void createJoinListOfRelNodes(List<RelationNode> relationNodeList,
            List<QueryCondition> conditionList) {
        for (RelationNode rn : relationNodeList) {
            for (QueryCondition qc : conditionList) {
                if (qc.isJoin()) {
                    if (qc.getRelationNodeList().contains(rn)) {
                        HashSet setRelNodes
                        = new HashSet(qc.getRelationNodeList());
                        if(setRelNodes.contains(rn)) {
                            setRelNodes.remove(rn);
                        }
                        rn.getJoinList().addAll(setRelNodes);
                    }
                }
            }
            // kill dupes
            HashSet<RelationNode> nodesList = new HashSet(rn.getJoinList());
            rn.setJoinList(new Vector(nodesList));
        }
    }

    /**
     *
     * @param newExpr
     * @param oldProjList
     * @param newProjList
     * @return
     */
    private SqlExpression changeInExpression(SqlExpression newExpr,
            List<SqlExpression> oldProjList, List<SqlExpression> newProjList) {
        int idx = oldProjList.indexOf(newExpr);
        if (idx > -1) {
            return newProjList.get(idx);
        }
        if (!(newExpr.getExprType() == SqlExpression.SQLEX_COLUMN || newExpr.getExprType() == SqlExpression.SQLEX_CONSTANT)) {

            if (newExpr.getLeftExpr() != null) {
                idx = oldProjList.indexOf(newExpr.getLeftExpr());
                if (idx == -1) {
                    changeInExpression(newExpr.getLeftExpr(), oldProjList,
                            newProjList);
                } else {
                    newExpr.setLeftExpr(newProjList.get(idx));
                }
            }
            if (newExpr.getRightExpr() != null) {
                idx = oldProjList.indexOf(newExpr.getRightExpr());
                if (idx == -1) {
                    changeInExpression(newExpr.getRightExpr(), oldProjList,
                            newProjList);
                } else {
                    newExpr.setRightExpr(newProjList.get(idx));
                }
            }

            if (newExpr.getFunctionParams() != null
                    && newExpr.getFunctionParams().size() > 0) {
                for (int i = 0; i < newExpr.getFunctionParams().size(); i++) {
                    idx = oldProjList.indexOf(newExpr.getFunctionParams().get(i));
                    if (idx == -1) {
                        changeInExpression(newExpr.getFunctionParams().get(i),
                                oldProjList, newProjList);
                    } else {
                        newExpr.getFunctionParams().set(i, newProjList.get(idx));
                    }
                }
            }
            return newExpr;
        }
        /* TODO
         * The code below tries to copy expression info from the parent
         * into the subtree (the soon-to-be parent). There is a problem
         * here with queries that use "*" and select with the same name.
         * It will not resolve the names properly.
         * For example: t1 and t2 both have col1 and col2 defined:
         * select * from (select * from (select * from t1 inner join t2
         * on t1.col2 = t2.col2) as a) as b;
         *
         * The problem appears only once we hit a depth of 3, but can
         * be worked-around by explicitly naming/aliasing columns.
         */
        SqlExpression useExpr = null;
        for (Object element : oldProjList) {
            SqlExpression oldExpr = (SqlExpression) element;
            if (newExpr.getOuterAlias().equalsIgnoreCase(oldExpr.getOuterAlias())
                    && newExpr.getOuterAlias().length() > 0
                    || newExpr.getAlias().equalsIgnoreCase(oldExpr.getAlias())
                    && newExpr.getAlias().length() > 0) {
                // We check all to make sure we got the right one, since
                // sometimes columns may have the same alias name.
                if (useExpr == null) {
                    useExpr = oldExpr;
                } else {
                    if (newExpr.getExprString().equalsIgnoreCase(oldExpr.getExprString())) {
                        useExpr = oldExpr;
                    }
                }
            }
        }
        if (useExpr != null) {
            SqlExpression.copy(useExpr, newExpr);
        }

        return newExpr;
    }

    /**
     *
     * @param oldCond
     * @param parentCondition
     * @return
     */
    private QueryCondition createNewCondition(QueryCondition oldCond,
            QueryCondition parentCondition) {
        if (oldCond == null) {
            return null;
        }
        QueryCondition qc = new QueryCondition();
        qc.setACompositeClause(oldCond.getACompositeClause());
        qc.setAnyAllFlag(oldCond.isAnyAllFlag());
        qc.setAnyAllString(oldCond.getAnyAllString());
        qc.setCondType(oldCond.getCondType());
        qc.setExpr(oldCond.getExpr());
        qc.setAtomic(oldCond.isAtomic());
        qc.setInPlan(oldCond.isInPlan());
        qc.setJoin(oldCond.isJoin());
        qc.setPositive(oldCond.isPositive());
        qc.setLeftCond(createNewCondition(oldCond.getLeftCond(), qc));
        qc.setOperator(oldCond.getOperator());
        qc.setParentQueryCondition(parentCondition);
        qc.setParentQueryTree(oldCond.getParentQueryTree());
        qc.setProjectedColumns(oldCond.getProjectedColumns());
        qc.setRightCond(createNewCondition(oldCond.getRightCond(), qc));
        qc.rebuildCondString();
        return qc;
    }

    /**
     *
     * @param cond
     * @param oldExpr
     * @param newExpr
     * @return
     */
    private QueryCondition changeExpression(QueryCondition cond,
            SqlExpression oldExpr, SqlExpression newExpr) {
        if (cond == null) {
            return null;
        }

        for (QueryCondition qc : QueryCondition.getNodes(cond,
                QueryCondition.QC_SQLEXPR)) {
            for (SqlExpression expr : SqlExpression.getNodes(qc.getExpr(),
                    SqlExpression.SQLEX_COLUMN)) {
                AttributeColumn aColumn = expr.getColumn();
                if (aColumn.columnAlias == oldExpr.getAlias()) {
                    aColumn.columnAlias = newExpr.getAlias();
                }
            }
        }
        return cond;
    }

    /**
     *
     * @return
     * @param aProjList
     * @param exp
     */
    private int findInProjectList(SqlExpression exp, List<SqlExpression> aProjList) {
        int result = -1;
        for (result = 0; result < aProjList.size(); result++) {
            SqlExpression expUnion = aProjList.get(result);
            if (expUnion.getAlias() != null
                    && expUnion.getAlias().equalsIgnoreCase(exp.getAlias())) {
                return result;
            }
            if (expUnion.getColumn().columnAlias != null
                    && expUnion.getAlias().equalsIgnoreCase(exp.getColumn().columnAlias)) {
                return result;
            }
            if (expUnion.getColumn().columnName != null
                    && expUnion.getAlias().equalsIgnoreCase(exp.getColumn().columnName)) {
                return result;
            }

        }
        return result;
    }

    /**
     *
     * @return
     * @param aQc
     */
    private Vector getAndCondition(QueryCondition aQc) {
        Vector result = new Vector();
        if (aQc == null) {
            return result;
        }
        if (aQc.getCondType() != QueryCondition.QC_CONDITION) {
            result.add(aQc);
            return result;
        }
        if (aQc.getOperator() == "AND") {
            result.add(aQc.getRightCond());
            result.addAll(getAndCondition(aQc.getLeftCond()));
            return result;
        } else {
            result.add(aQc);
            return result;
        }

    }

    /**
     *
     * @param aQc
     * @return
     */
    private boolean canMove(QueryCondition aQc) {
        if (aQc == null) {
            return true;
        }
        if (aQc.getCondType() == QueryCondition.QC_SQLEXPR
                || aQc.getCondType() == QueryCondition.QC_COMPOSITE
                || aQc.getCondType() == QueryCondition.QC_RELOP) {
            return true;
        }
        if (aQc.getCondType() != QueryCondition.QC_CONDITION) {
            return false;
        }
        if (aQc.getOperator() == "AND") {
            return canMove(aQc.getRightCond()) & canMove(aQc.getLeftCond());
        } else {
            return false;
        }

    }

    /**
     *
     * @param aQc
     * @return
     */
    private boolean hasAggFunction(QueryCondition aQc) {
        if (aQc == null) {
            return false;
        }
        Vector sqlExprList = QueryCondition.getNodes(aQc,
                QueryCondition.QC_SQLEXPR);
        for (Iterator iter = sqlExprList.iterator(); iter.hasNext();) {
            QueryCondition el = (QueryCondition) iter.next();
            if (el.getExpr().isAggregateExpression()) {
                return true;
            } else if (el.getExpr().getMappedExpression() != null
                    && el.getExpr().getMappedExpression().isAggregateExpression()) {
                return true;
            }
        }
        return false;
    }

    /**
     *
     * @param aCommonConditions
     * @param op
     * @return
     */
    private QueryCondition createOperatorConditions(Vector aCommonConditions,
            String op) {
        for (Iterator iter = aCommonConditions.iterator(); iter.hasNext();) {
            if (iter.next() == null) {
                iter.remove();
            }
        }
        QueryCondition qc = new QueryCondition();
        Vector aCopyCommonConditions = new Vector();
        aCopyCommonConditions.addAll(aCommonConditions);
        if (aCopyCommonConditions == null || aCopyCommonConditions.isEmpty()) {
            return null;
        }
        if (aCopyCommonConditions.size() == 1) {
            if (aCopyCommonConditions.get(0) == null) {
                return null;
            }
            return (QueryCondition) aCopyCommonConditions.get(0);
        }
        if (aCopyCommonConditions.get(1) == null) {
            return null;
        }

        qc.setCondType(QueryCondition.QC_CONDITION);
        qc.setOperator(op);
        qc.setLeftCond((QueryCondition) aCopyCommonConditions.get(0));
        aCopyCommonConditions.remove(0);
        qc.setRightCond(createOperatorConditions(aCopyCommonConditions, op));
        qc.rebuildCondString();
        return qc;

    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#needCoordinatorConnection()
     */
    /**
     *
     * @return
     */
    public boolean needCoordinatorConnection() {
        return true;
    }

	@Override
	public boolean isReadOnly() {
		return true;
	}

}
