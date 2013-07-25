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
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.postgresql.stado.common.ActivityLog;
import org.postgresql.stado.common.ColumnMetaData;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IParametrizedSql;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.engine.io.DataTypes;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.optimizer.OrderByElement;
import org.postgresql.stado.optimizer.QueryCondition;
import org.postgresql.stado.optimizer.QueryTree;
import org.postgresql.stado.optimizer.RelationNode;
import org.postgresql.stado.optimizer.SqlExpression;
import org.postgresql.stado.parser.core.syntaxtree.Select;
import org.postgresql.stado.parser.core.syntaxtree.WithDef;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.ColumnNameListHandler;
import org.postgresql.stado.parser.handler.IdentifierHandler;
import org.postgresql.stado.parser.handler.OrderByClauseHandler;
import org.postgresql.stado.parser.handler.QueryTreeHandler;
import org.postgresql.stado.parser.handler.QueryTreeTracker;
import org.postgresql.stado.queryproc.QueryProcessor;

// -----------------------------------------------------------------
/**
 * The information class which collects information from a SELECT statement and
 * converts it into a tree structure
 */
public class SqlSelect extends DepthFirstVoidArguVisitor implements IXDBSql,
        IPreparable, IParametrizedSql {
    private static final XLogger logger = XLogger.getLogger(SqlSelect.class);

    private XDBSessionContext client;

    SysTable targetTable;

    public QueryTree aQueryTree = null;

    private SysDatabase database;

    private Command commandToExecute;

    private QueryProcessor qProcessor;

    private String selectString;

    public String selectType; // all or distinct

    private String specialMetadataRequest = null;

    /** whether or not this has been prepared for PreparedStatements */
    private boolean isParameterPrepared = false;

    // -----------------------------------
    /**
     * Constructor
     */
    public SqlSelect(XDBSessionContext client) {
        this.client = client;
        database = client.getSysDatabase();
        // I will add a new command object to the QueryTree handler
        // and then set that particular object with the command object
        commandToExecute = new Command(Command.SELECT, this,
                new QueryTreeTracker(), client);

        // Secondly I will create a Query Tree Tracker object and will also
        // set it here in all other location below this when ever a QueryTree
        // Handler is created the query tree tracker will always set it.
        aQueryTree = new QueryTree();
    }

    /**
     * Get the target Sys table.
     *
     * @return SysTable
     * @throws XDBServerException
     *                 if table if not found.
     */
    public SysTable getTargetTable(String tableName) throws XDBServerException {
        if (targetTable == null) {
            targetTable = commandToExecute.getClientContext().getSysDatabase().getSysTable(
                    tableName);
        }
        return targetTable;
    }

    /**
     * Grammar production: f0 -> SelectWithoutOrderWithParenthesis(prn) f1 -> [
     * OrderByClause(prn) ] f2 -> [ LimitClause(prn) ] f3 -> [ OffsetClause(prn) ]
     */
    @Override
    public void visit(Select n, Object obj) {
        QueryTreeHandler aQueryTreeHandler = new QueryTreeHandler(
                commandToExecute);
        n.f0.accept(aQueryTreeHandler, aQueryTree);
        OrderByClauseHandler aOrderByClauseHandler = new OrderByClauseHandler(
                commandToExecute);
        n.f1.accept(aOrderByClauseHandler, aQueryTree);
        aQueryTree.setOrderByList(aOrderByClauseHandler.orderByList);
        aQueryTree.preProcessOrderByList();
        QueryTreeHandler.FillSQLExpressionInformation(aQueryTree, aQueryTree.getOrderByList(),
                commandToExecute);
        n.f2.accept(aQueryTreeHandler, aQueryTree);
        n.f3.accept(aQueryTreeHandler, aQueryTree);
        preProcessUnionList();
    }
    
    /**
     * For WITH
     * 
     * Grammar production:
     * f0 -> Identifier(prn)
     * f1 -> [ColumnNameListWithParentheses]
     * f2 -> <AS_>
     * f3 -> <PARENTHESIS_START_>
     * f4 -> SelectWithoutOrder(prn)
     * f5 -> <PARENTHESIS_CLOSE_>
     */
    @Override
    public void visit(WithDef n, Object obj) {
        
        ColumnNameListHandler aColumnListHandler = null;
        String tableAlias = (String) n.f0.accept(new IdentifierHandler(), obj);
   
        // Create a RelationNode to represent the WITH subquery definition
        RelationNode aRelationNode = aQueryTree.newRelationNode(); 
        aRelationNode.setNodeType(RelationNode.SUBQUERY_RELATION);
        aRelationNode.setAlias(tableAlias);
        aRelationNode.setTableName("");
        aRelationNode.setIsWith(true);
        aQueryTree.addToTopWithList(aRelationNode);
        
        // Remove from the tree's relation node list.
        // It may only feed into other WITH's, and if present here,
        // the code will try and use it in a CROSS JOIN
        // We will create wrapper nodes that refer to these later
        aQueryTree.getRelationNodeList().remove(aRelationNode);
        
        if (n.f1.present()) {
            aColumnListHandler = new ColumnNameListHandler();
            n.f1.accept(aColumnListHandler, null);
            // We do further processing below
        }
        
        
        // The SubQuery Tree will have the query tree which will
        // be filled up with the select statement information
        QueryTree aSubQueryTree = new QueryTree();
        aRelationNode.setSubqueryTree(aSubQueryTree);
        
        aSubQueryTree.setParentQueryTree(aQueryTree);
        aSubQueryTree.setTopMostParentQueryTree(aQueryTree.getTopMostParentQueryTree());
        

        // We will now try to get the Query SubTree
        QueryTreeHandler aQueryTreeHandler = new QueryTreeHandler(
                commandToExecute);
        n.f4.accept(aQueryTreeHandler, aSubQueryTree);
                
        // Previous handler set this, don't include in this list 
        // as we included it in the WITH list
        aQueryTree.getRelationSubqueryList().remove(aRelationNode);
        
        //will be needed in case of LeftOuter (+) handling
        aQueryTree.getRelHandlerInfo().put(aRelationNode.getTableName(), aRelationNode);
        
        // Check if we need to set aliases based on column list
        // Example: WITH newrel (col1, col2)...
        if (aColumnListHandler != null) {
            List<String> colNameList = aColumnListHandler.getColumnNameList();
            
            if (aSubQueryTree.getProjectionList().size() < colNameList.size()) {
                throw new XDBServerException(
                "WITH for " + tableAlias + " specifies more column names than columns");
            }
            for (int i = 0; i < colNameList.size(); i++) {
                aSubQueryTree.getProjectionList().get(i).setAlias(colNameList.get(i));
                aSubQueryTree.getProjectionList().get(i).setOuterAlias(colNameList.get(i));
            }
        }
        
        aQueryTreeHandler.populateSubRelationProjectionList(aRelationNode, database);
        
        aQueryTreeHandler.FillAllExprDataTypes(aSubQueryTree, commandToExecute);
        
        // Fix projection types
        for (SqlExpression aSqlExpression: aRelationNode.getProjectionList()) {
            if (aSqlExpression.getExprDataType() == null) {
                if (aSqlExpression.getExprType() == SqlExpression.SQLEX_COLUMN) {
                    aSqlExpression.setExprDataType(aSqlExpression.getColumn().getColumnType(database));
                }
            }
        }
    }

    /**
     *
     */
    private void preProcessUnionList() {
        if (aQueryTree.isHasUnion()) {
            for (int i = 0; i < aQueryTree.getUnionQueryTreeList().size(); i++) {
                aQueryTree.getUnionQueryTreeList().get(i).checkExpressionTypes(aQueryTree.getProjectionList());
            }
        }
    }

    /**
     * This function pre process the order by list , The pre processing of the
     * order by list allows us to get the right expression from the select list
     * if we have a numeric number in the order list
     */
    private void preProcessOrderByList() {
        // The pre processing of the order by list allows us to get the right
        // expression from the select list if we have a numeric number in the
        // order list
        for (OrderByElement aOrderExpr : aQueryTree.getOrderByList()) {
            // Check out if we have any numeric expressions
            // Get the SQL Expression from this orderExpression
            SqlExpression orderExpressionValue = aOrderExpr.orderExpression;
            // Check to see if we have a SqlExpression of type constant and
            // if it is numeric
            String exprString = orderExpressionValue.rebuildString();

            try {
                int parsedIntValue = Integer.parseInt(exprString);
                // Incase we get the parsed int value - we should replace this
                // particular
                // expression with the corresponding expression from the select
                // statement

                // The index that this element will access is therefore
                int indexToSelect = parsedIntValue - 1;
                // Check if we have a valid number
                if (indexToSelect >= aQueryTree.getProjectionList().size()
                        || indexToSelect < 0) {

                    throw new XDBServerException(
                            ErrorMessageRepository.ORDERBY_CLAUSE_POINTS_TO_ILLEGAL_PROJ_COLUMN,
                            0,
                            ErrorMessageRepository.ORDERBY_CLAUSE_POINTS_TO_ILLEGAL_PROJ_COLUMN_CODE);
                    // throw new XDBSemanticException("Value in the order list
                    // is greater than the number of " +
                    // "projected expressions in the query OR is less than 1 ");
                } else {
                    SqlExpression aProjectedExpression = aQueryTree.getProjectionList().get(indexToSelect);
                    aOrderExpr.orderExpression = new SqlExpression();
                    // Replace the expression in the order element by this
                    // expression
                    SqlExpression.copy(aProjectedExpression,
                            aOrderExpr.orderExpression);
                }
            } catch (NumberFormatException ex) {
                // This could be a column or an alias name we just let it pass
                // by and allow the
                // next step of finding the column in the used tables to take
                // place.
                continue;
            }
        }

    }

    /**
     * This will return the cost of executing this statement in time , milli
     * seconds
     */
    public long getCost() {
        final String method = "getCost";
        logger.entering(method);
        try {

            try {
                if (!isPrepared()) {
                    prepare();
                }
                if (specialMetadataRequest != null) {
                    return 0;
                }
                return qProcessor.getCost();
            } catch (Exception e) {

            }
            return 0;

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * This return the Lock Specification for the system
     *
     * @param theMData
     * @return
     */
    public LockSpecification<SysTable> getLockSpecs() {
        final String method = "getLockSpecs";
        logger.entering(method);
        try {

            try {
                if (!isPrepared()) {
                    prepare();
                }
                if (specialMetadataRequest != null) {
                    Collection<SysTable> empty = Collections.emptySet();
                    return new LockSpecification<SysTable>(empty, empty);
                }
                return qProcessor.getLockSpecs();
            } catch (Exception e) {

            }
            return null;

        } finally {
            logger.exiting(method);
        }
    }

    public Collection<DBNode> getNodeList() {
        try {
            if (!isPrepared()) {
                prepare();
            }
            if (specialMetadataRequest != null) {
                Collection<DBNode> empty = Collections.emptySet();
                return empty;
            }
            return qProcessor.getNodeList();
        } catch (Exception e) {

        }
        return Collections.emptyList();
    }

    private HashSet<String> noPermissionCheck = new HashSet<String>();

    public void addSkipPermissionCheck(String tableName) {
        noPermissionCheck.add(tableName.toUpperCase());
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IPreparable#isPrepared()
     */
    public boolean isPrepared() {
        return specialMetadataRequest != null || qProcessor != null
                && qProcessor.isPrepared();
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

            if (specialMetadataRequest != null) {
                return;
            }
            qProcessor = new QueryProcessor(client, aQueryTree);
            if (!noPermissionCheck.isEmpty()) {
                qProcessor.setSkipPermissionCheck(noPermissionCheck);
            }
            qProcessor.prepare();

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

            if (getParamCount() > 0 && specialMetadataRequest == null) {
                if (!isParameterPrepared) {
                    qProcessor.prepareParameters(commandToExecute.getParameters());
                    isParameterPrepared = true;
                }
                qProcessor.resetExecPlan(commandToExecute.getParameters());
            }

            if (specialMetadataRequest != null) {
                Connection oConn = client.getAndSetCoordinatorConnection();
                try {
                    ResultSet rs;
                    List<SqlExpression> parameters = commandToExecute.getParameters();
                    if (parameters == null) {
                        Statement stmt = oConn.createStatement();
                        rs = stmt.executeQuery(specialMetadataRequest);
                    } else {
                        // Replace Postgres' placeholders with JDBCs
                        for (int i = parameters.size(); i > 0; i--) {
                            specialMetadataRequest = specialMetadataRequest.replaceAll(
                                    "\\$" + i, "?");
                        }
                        PreparedStatement stmt = oConn.prepareStatement(specialMetadataRequest);
                        for (int i = 0; i < parameters.size();) {
                            SqlExpression param = parameters.get(i++);
                            DataTypes.setParameter(stmt, i, param.getParamValue(),
                                    param.getExprDataType().type);
                        }

                        rs = stmt.executeQuery();
                    }
                    return ExecutionResult.createResultSetResult(
                            ExecutionResult.COMMAND_SELECT, rs);
                } catch (SQLException se) {
                    logger.catching(se);
                    oConn.rollback();
                    return ExecutionResult.createErrorResult(se);
                }

            }
            if (Props.XDB_ENABLE_ACTIVITY_LOG) {
                ActivityLog.startRequest(client.getStatementId(), selectString);
            }
            return qProcessor.execute(engine);

        } finally {
            if (Props.XDB_ENABLE_ACTIVITY_LOG) {
                ActivityLog.endRequest(client.getStatementId());
            }
            logger.exiting(method);
        }
    }

    public ColumnMetaData[] getMetaData() throws Exception {
        if (!isPrepared()) {
            prepare();
        }
        return qProcessor.getMetaData();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#needCoordinatorConnection()
     */
    public boolean needCoordinatorConnection() {
        return qProcessor == null ? false
                : qProcessor.needCoordinatorConnection();
    }

    /**
     * We try and do a fast manual parse of a simple update statement
     *
     * @param cmd -
     *                The command to parse
     *
     * @return whether or not select was parseable.
     *
     */
    public boolean manualParse(String cmd) {
        String token = null;
        String tableName;
        SysTable aSysTable;

        selectString = cmd;

        // Check if that is a Metadata request, we have special handling for it
        if (isSpecialMetadataRequest(cmd)) {
            specialMetadataRequest = transformSpecialMetadataRequest(cmd);
            handleMetadataParams(specialMetadataRequest);
            return true;
        }

        Lexer aLexer = new Lexer(cmd);

        if (!aLexer.hasMoreTokens()
                || !aLexer.nextToken().equalsIgnoreCase("SELECT")) {
            return false;
        }

        if (!aLexer.hasMoreTokens()) {
            return false;
        }
        token = aLexer.nextToken();

        // Check if the query is distinct
        if (token.equalsIgnoreCase("DISTINCT")
                || token.equalsIgnoreCase("UNIQUE")) {
            aQueryTree.setDistinct(true);
        }

        // We jump ahead to the FROM clause
        int startFrom = cmd.toUpperCase().indexOf(" FROM ");
        if (startFrom == -1) {
        	// Check for the from on a new line
        	startFrom = cmd.toUpperCase().indexOf("\nFROM ");
        }

        aLexer = new Lexer(cmd.substring(startFrom + 6));

        if (!aLexer.hasMoreTokens()) {
            return false;
        }

        tableName = aLexer.nextToken();

        // Get tree ready, used in costing
        QueryTreeTracker aQueryTreeTracker = commandToExecute.getaQueryTreeTracker();
        aQueryTreeTracker.registerTree(aQueryTree);

        RelationNode aRelationNode = aQueryTree.newRelationNode();
        aRelationNode.setNodeType(RelationNode.TABLE);

        aRelationNode.setTableName(tableName);
        aRelationNode.setTemporaryTable(client.getTempTableName(tableName) != null);
        aRelationNode.setClient(commandToExecute.getClientContext());
        aRelationNode.setAlias("");

        if (aLexer.hasMoreTokens()) {
            // See if we have an alias
            token = aLexer.nextToken();

            if (!token.equals(";") && !token.equalsIgnoreCase("WHERE")
                    && !token.equalsIgnoreCase("ORDER")) {
                aRelationNode.setAlias(token);

                // if we find anything else other than WHERE or ORDER next, we
                // have a complex SELECT, like a UNION
                if (aLexer.hasMoreTokens()) {
                    token = aLexer.nextToken();

                    if (!token.equalsIgnoreCase("WHERE")
                            && !token.equalsIgnoreCase("ORDER")) {
                        return false;
                    }
                }
            }
        }

        // --------------------------------------------------------
        // Now handle projections in SELECT clause
        aSysTable = client.getSysDatabase().getSysTable(tableName);
        if (!ParserHelper.parseProjectionsForSingleTable(cmd.substring(7,
                startFrom), aQueryTree, aRelationNode, aSysTable, client)) {
            // parse error
            return false;
        }

        if (token.equals(";")) {
            /*
             * Before returning the tree we should get all the SQLExpressions and
             * fill there Final Data Types
             */
            QueryTreeHandler.FillAllExprDataTypes(aQueryTree, commandToExecute);
            return true;
        }

        // --------------------------------------------------------
        // Handle WHERE clause
        int wherePos = cmd.toUpperCase().indexOf(" WHERE ", startFrom);
        if (wherePos > 0) {
            aLexer = new Lexer(cmd.substring(wherePos + 6));

            // We assume simple conditions only
            while (true) {
                QueryCondition aQC = ParserHelper.getSimpleCondition(aLexer,
                        client.getSysDatabase().getSysTable(tableName),
                        aQueryTree, client);
                if (aQC == null) {
                    // parser error
                    return false;
                }
                QueryTreeHandler.ProcessWhereCondition(aQC, aQueryTree,
                        commandToExecute);

                if (!aLexer.hasMoreTokens()) {
                    return true;
                }

                // see if we have more anded together
                token = aLexer.nextToken();
                if (!token.equalsIgnoreCase("AND")) {
                    break;
                }
            }
        } else {
            // ORDER BY without WHERE
            int orderPos = cmd.toUpperCase().indexOf(" ORDER ", startFrom);
            if (orderPos > 0) {
                aLexer = new Lexer(cmd.substring(orderPos));
                token = aLexer.nextToken();
            }
        }

        // now handle order by
        if (token.equals(";")) {
            /*
             * Before returning the tree we should get all the SQLExpressions and
             * fill there Final Data Types
             */
            QueryTreeHandler.FillAllExprDataTypes(aQueryTree, commandToExecute);
            return true;
        }
        if (!token.equalsIgnoreCase("ORDER")) {
            return false;
        }

        if (!aLexer.hasMoreTokens()
                || !aLexer.nextToken().equalsIgnoreCase("BY")) {
            return false;
        }

        if (!aLexer.hasMoreTokens()) {
            return false;
        }

        String next = null;

        while (true) {
            String columnName;

            OrderByElement aOrderByElement = null;

            String first = aLexer.nextToken();
            if (aLexer.hasMoreTokens()) {
                next = aLexer.nextToken();

                if (next.equals(".")) {
                    if (!next.equalsIgnoreCase(tableName)
                            && !next.equalsIgnoreCase(aRelationNode.getAlias())) {
                        return false;
                    }
                    if (!aLexer.hasMoreTokens()) {
                        return false;
                    }
                    columnName = aLexer.nextToken();
                } else {
                    columnName = first;
                }
            } else {
                columnName = first;
            }

            if (aSysTable.getSysColumn(columnName) == null) {
                int ordinalPos;
                // See if it is a position marker
                try {
                    ordinalPos = Integer.parseInt(columnName);
                } catch (NumberFormatException n) {
                    return false;
                }

                aOrderByElement = new OrderByElement();
                aOrderByElement.orderExpression = aQueryTree.getProjectionList().get(ordinalPos - 1);

                aOrderByElement.orderDirection = OrderByElement.ASC;
                aQueryTree.getOrderByList().add(aOrderByElement);
            } else {
                // explicit column
                // We just look for it in the projection list
                boolean found = false;

                for (SqlExpression aSE : aQueryTree.getProjectionList()) {
                    if (aSE.getExprType() != SqlExpression.SQLEX_COLUMN) {
                        continue;
                    }
                    if (columnName.equalsIgnoreCase(aSE.getColumn().columnName)
                            || columnName.equalsIgnoreCase(aSE.getColumn().columnAlias)) {
                        // match
                        aOrderByElement = new OrderByElement();
                        aOrderByElement.orderExpression = aSE;
                        aQueryTree.getOrderByList().add(aOrderByElement);
                        found = true;
                        break;
                    }
                }
                // We did not find it
                if (!found) {
                    return false;
                }
            }

            if (next.equalsIgnoreCase("ASC")) {
                aOrderByElement.orderDirection = OrderByElement.ASC;
                if (!aLexer.hasMoreTokens()) {
                    /*
                     * Before returning the tree we should get all the SQLExpressions and
                     * fill there Final Data Types
                     */
                    QueryTreeHandler.FillAllExprDataTypes(aQueryTree, commandToExecute);
                    return true;
                }
                next = aLexer.nextToken();
            } else if (next.equalsIgnoreCase("DESC")) {
                aOrderByElement.orderDirection = OrderByElement.DESC;
                if (!aLexer.hasMoreTokens()) {
                    /*
                     * Before returning the tree we should get all the SQLExpressions and
                     * fill there Final Data Types
                     */
                    QueryTreeHandler.FillAllExprDataTypes(aQueryTree, commandToExecute);
                    return true;
                }
                next = aLexer.nextToken();
            }

            if (next == null) {
                break;
            }
            if (next.equals(";")) {
                break;
            }
            if (next.equals(",")) {
                continue;
            } else {
                // error
                return false;
            }
        }

        aQueryTree.preProcessOrderByList();
        QueryTreeHandler.FillSQLExpressionInformation(aQueryTree, aQueryTree.getOrderByList(),
                commandToExecute);

        if (next == null) {
            /*
             * Before returning the tree we should get all the SQLExpressions and
             * fill there Final Data Types
             */
            QueryTreeHandler.FillAllExprDataTypes(aQueryTree, commandToExecute);
            return true;
        }
        if (!aLexer.hasMoreTokens()) {
            /*
             * Before returning the tree we should get all the SQLExpressions and
             * fill there Final Data Types
             */
            QueryTreeHandler.FillAllExprDataTypes(aQueryTree, commandToExecute);
            return true;
        }
        token = aLexer.nextToken();
        if (!token.equals(";")) {
            return false;
        }
        if (aLexer.hasMoreTokens()) {
            return false;
        }

        // everything is ok
        /*
         * Before returning the tree we should get all the SQLExpressions and
         * fill there Final Data Types
         */
        QueryTreeHandler.FillAllExprDataTypes(aQueryTree, commandToExecute);
        return true;
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
        /*
         * If set new parameter values existing result set should be invalidated
         */
        if (qProcessor != null)
            qProcessor.reset();
        commandToExecute.getParameter(index + 1).setParamValue(value);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.engine.IParametrizedSql#setParamValues(java.lang.String[])
     */
    public void setParamValues(String[] values)
            throws ArrayIndexOutOfBoundsException, XDBServerException {
        /*
         * If set new parameter values existing result set should be invalidated
         */
        if (qProcessor != null)
            qProcessor.reset();
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
        if (specialMetadataRequest != null) {
            for (int i = dataTypes.length; i > 0; i--) {
                specialMetadataRequest = specialMetadataRequest.replaceAll(
                        "\\$" + i, "?");
            }
        }
        for (int i = 0; i < dataTypes.length; i++) {
            ExpressionType type = commandToExecute.getParameter(i + 1).getExprDataType();
            if (type == null) {
                type = new ExpressionType();
                commandToExecute.getParameter(i + 1).setExprDataType(type);
            }
            type.type = dataTypes[i];
        }
    }

    /*
     * Set the select string
     */
    public void setSelectString(String value) {
        selectString = value;
    }

    /**
     * Detect if command is a special metadata request.
     * 
     * @param cmd
     * @return
     */
    private boolean isSpecialMetadataRequest(String cmd) {

        cmd = cmd.toLowerCase().replace('\r', ' ').replace('\n', ' ')
                .replace('\t', ' ');
        
        if (cmd.matches(".*\\s+from\\s+\\W*pg_.*")
                || cmd.matches(".*\\s+from\\s+\\W*information_schema.*")) {
            return true;
        }
        return false;
    }

    /**
     * Transform special metadata request to query returning expected results.
     * 
     * @param cmd
     * @return
     */
    private String transformSpecialMetadataRequest(String cmd) {
        StringBuffer result = new StringBuffer();
        Lexer aLexer = new Lexer(cmd);
        while (aLexer.hasMoreTokens()) {
            String token = aLexer.nextToken();
            if (token.equalsIgnoreCase("pg_database")) {
                result.append(" ").append("__stado.pg_database");
            } else if (token.equalsIgnoreCase("pg_catalog")) {
                String tableName = aLexer.peekToken(1);
                if (tableName.equalsIgnoreCase("pg_database")) {
                    // Skip "."
                    aLexer.nextToken();
                    // Skip "pg_database"
                    aLexer.nextToken();
                    result.append(" ").append("__stado.pg_database");
                } else {
                    result.append(" ").append(token);
                }
            } else {
                if (aLexer.lastTokenType() == Lexer.IDENTIFIER) {
                    result.append(" \"").append(token).append("\"");
                } else {
                    result.append(" ").append(token);
                }
            }
        }
        if (Props.XDB_BSU_SUBSTITUTE) {
            // For Business Objects compatibility. It appears to double-escape 
            // underscore (_) sometimes when it should not
            return result.toString().replace("\\\\_", "_");
        } else {
            return result.toString();
        }
    }

    private void handleMetadataParams(String cmd) {

        String[] sub = cmd.split("\\$");

        // skip the first one
        for (int i=1; i < sub.length; i++) {
            String paramStr = sub[i];
            int p = 0;
            while (paramStr.length() > p && paramStr.charAt(p) >= '0'
                    && paramStr.charAt(p) <= '9') {
                p++;
            }
            if (p <= 0) {
                continue;
            }

            SqlExpression aSqlExpression = new SqlExpression(); //getSqlExpression();
            aSqlExpression.setExprType(SqlExpression.SQLEX_PARAMETER);
            aSqlExpression.setExprDataType(new ExpressionType());
            int number = Integer.parseInt(paramStr.substring(0, p));
            commandToExecute.registerParameter(number, aSqlExpression);
            aSqlExpression.setParamNumber(number);
        }
    }

	@Override
	public boolean isReadOnly() {
		return true;
	}
}
