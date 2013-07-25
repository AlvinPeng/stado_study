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
 * ExecutionPlanPreparedHandler.java
 *
 *  
 *
 */

package org.postgresql.stado.planner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.optimizer.SqlExpression;
import org.postgresql.stado.parser.ExpressionType;


/**
 * Handles parameter substitution for PreparedStatements
 * in ExecutionPlans. 
 *
 * 
 */
public class ExecutionPlanPreparedHandler {

    /** List of statements that need to be prepared. */
    private List<PlanPrepStatement> planPrepStatementList;

    /** List of parameters for the plan */
    private List<Parameter> parameterList;

    /** The client session context */
    private XDBSessionContext client;

    /** array containing the types for all parameters */
    private int[] paramTypes;

    /**
     * An inner class that handles the individual steps in the plan that
     * require parameter substitution.      
     */
    class PlanPrepStatement {

        /** ExecutionStep containing statement. */
        private ExecutionStep anExecutionStep;

        /** The original query string, before substitutions. */
        private String originalQueryString;

        /** The SqlExpression containing parameter info */
        private SqlExpression parameterExpression;

        /**
         * @param anExecutionStep - the ExecutionStep containing the statement
         * that requires parameter substitution.
         */
        PlanPrepStatement(ExecutionStep anExecutionStep) {
            this.anExecutionStep = anExecutionStep;
            originalQueryString = anExecutionStep.aStepDetail.queryString;
            parameterExpression = null;
        }

        /**
         * Substitutes the particular parameter value for the specified 
         * parameteter number.
         *
         * @param paramNum - parameter number
         * @param paramValue - parameter value
         * @param addQuote - whether or not to enclose the value in quotes.
         */
        public void substitute(int paramNum, String paramValue, boolean addQuote) {

            if (paramValue == null) {
                anExecutionStep.aStepDetail.queryString = anExecutionStep.aStepDetail.queryString.replace(
                        "&xp" + paramNum + "xp&", "null");
            } else {
                anExecutionStep.aStepDetail.queryString = anExecutionStep.aStepDetail.queryString.replace(
                        "&xp" + paramNum + "xp&", addQuote ? "'" + paramValue + "'"
                                : paramValue);
            }

            if (parameterExpression != null) {
                updateDestinationFromParameter(anExecutionStep, paramValue,
                        parameterExpression);
            }
        }

        /**
         * Update the node usage table based on the parameter value,
         * for PreparedStatements.
         *
         * @param anExecutionStep - the ExecutionStep whose destination
         * should be updated.
         * @param paramValue - parameter value
         * @param tableName - the name of the table to use to base partitioning
         * decisions on.
         */
        protected void updateDestinationFromParameter(
                ExecutionStep anExecutionStep, String paramValue,
                SqlExpression parameterExpression) {

            int execNodeId;

            SysTable sysTable = client.getSysDatabase().getSysTable(
                    parameterExpression.getColumn().getTableName());

            Collection<DBNode> cDBNode = sysTable.getNode(SqlExpression.createConstantExpression(
                    paramValue, parameterExpression.getExprDataType()).getNormalizedValue());

            execNodeId = ((DBNode) (cDBNode.toArray())[0]).getNodeId();

            Iterator itUsage = anExecutionStep.nodeUsageTable.values().iterator();

            while (itUsage.hasNext()) {
                NodeUsage aNodeUsage = (NodeUsage) itUsage.next();

                NodeUsage aNodeUsageElement = anExecutionStep.nodeUsageTable.get(new Integer(
                        aNodeUsage.nodeId));

                if (aNodeUsageElement != null) {
                    if (aNodeUsageElement.nodeId == execNodeId) {
                        aNodeUsageElement.isProducer = true;
                    } else {
                        aNodeUsageElement.isProducer = false;
                    }
                }
            }
        }

        /**
         * Resets the statement to the original one, without replaced 
         * parameters.
         */
        public void reset() {
            anExecutionStep.aStepDetail.queryString = originalQueryString;
        }

        /**
         * Sets the partition parameter expression information.
         *
         * @param parameterExpression 
         */
        public void setPartitionParameterInfo(SqlExpression parameterExpression) {
            this.parameterExpression = parameterExpression;
        }
    }

    /**
     * Inner class for handling PreparedStatement parameters.
     *
     */
    private class Parameter {

        /** parameter number */
        private int paramNum;

        /** whether or not we need to quote parameter values */
        private boolean addQuote;

        /** PlanPrepStatement that this parameter belongs to */
        private PlanPrepStatement aPlanPrepStatement;

        /**
         * Instantiates Parameter
         *
         * @param paramNum - parameter number
         * @param paramType - parameter tye
         * @param aPlanPrepStatement - the PlanPrepStatement that uses this
         * parameter.
         */
        Parameter(int paramNum, int paramType,
                PlanPrepStatement aPlanPrepStatement) {
            this.paramNum = paramNum;
            this.aPlanPrepStatement = aPlanPrepStatement;

            // We will add a quote if it is not a numeric data type.
            try {
                addQuote = !ExpressionType.isNumeric(paramType);
            } catch (Exception e) {
                // Unexpected type, but try and quote it.
                addQuote = true;
            }
        }

        /**
         * Substitutes the parameter value in the statement that it belongs to.
         *
         * @param paramValue - parameter value
         */
        public void substitute(String paramValue) {
            aPlanPrepStatement.substitute(paramNum, paramValue, addQuote);
        }
    }

    /**
     * Creates a new instance of ExecutionPlanPreparedHandler
     * 
     * @param client
     */
    public ExecutionPlanPreparedHandler(XDBSessionContext client,
            int[] paramTypes) {
        this.client = client;
        this.paramTypes = paramTypes;
    }

    /**
     * Prepares the plan parameters.
     * 
     * @param anExecPlan - ExecutionPlan
     */
    public void preparePlanParameters(ExecutionPlan anExecPlan) {
        // We step through resulting plan and look for which steps require 
        // parameter substitution.

        ExecutionPlan scalarPlan;

        if (anExecPlan.unionPlanList.size() > 0) {
            prepareUnionPlanParameters(anExecPlan.unionPlanList);
        }

        // Check to see if we have any scalar subqueries
        Iterator itScalar = anExecPlan.scalarPlanList.iterator();

        while (itScalar.hasNext()) {
            scalarPlan = (ExecutionPlan) itScalar.next();

            preparePlanParameters(scalarPlan);
        }

        // execute any relation subplans
        Iterator itRelation = anExecPlan.relationPlanList.iterator();

        while (itRelation.hasNext()) {
            ExecutionPlan relationPlan = (ExecutionPlan) itRelation.next();

            preparePlanParameters(relationPlan);
        }

        // ok, now we can start processing the steps of our query
        Iterator itExecSteps = anExecPlan.stepList.iterator();

        while (itExecSteps.hasNext()) {
            ExecutionStep anExecStep = (ExecutionStep) itExecSteps.next();

            // See if we need to handle any correlated subplans first
            if (anExecStep.correlatedSubPlan != null) {
                // I don't rhink we need to correct here
                preparePlanParameters(anExecStep.correlatedSubPlan);
            }

            QueryPlan.CATEGORY_QUERYFLOW.info(anExecStep);

            // see if we have any uncorrelated subqueries
            if (anExecStep.uncorrelatedSubPlanList != null) {
                Iterator itUncor = anExecStep.uncorrelatedSubPlanList.iterator();

                while (itUncor.hasNext()) {
                    ExecutionPlan uncorSubPlan = (ExecutionPlan) itUncor.next();

                    // Update uncorrelated subplan
                    preparePlanParameters(uncorSubPlan);
                }
            }

            prepareStepParameters(anExecStep);
        }
    }

    /**
     * Handles preparing parameters for UNION queries
     *
     * @param unionPlanList - list of ExecutionPlans that are unioned
     */
    private void prepareUnionPlanParameters(List unionPlanList) {

        ExecutionPlan subPlan;

        for (int i = 0; i < unionPlanList.size(); i++) {
            subPlan = (ExecutionPlan) unionPlanList.get(i);

            preparePlanParameters(subPlan);
        }
    }

    /**
     * Adds a statement to the statement list for parameter substitution.
     *
     * @param anExecutionStep - the ExecutionStep to prepare.
     *
     * @return the PlanPrepStatement created to represent the statement.
     */
    private PlanPrepStatement addPlanPrepStatement(ExecutionStep anExecutionStep) {

        if (planPrepStatementList == null) {
            planPrepStatementList = new ArrayList<PlanPrepStatement>();
        }

        PlanPrepStatement aPlanPrepStatement = new PlanPrepStatement(
                anExecutionStep);
        aPlanPrepStatement.setPartitionParameterInfo(anExecutionStep.getPartitionParameterExpression());
        planPrepStatementList.add(aPlanPrepStatement);

        return aPlanPrepStatement;
    }

    /**
     * Adds parameter information of PreparedStatement to the plan.
     * This allows us to perform substitutions later without having to 
     * replan.     
     *
     * @param paramNum - parameter number
     * @param paramType - paramter type
     * @param aPlanPrepStatement - the step statement the parameter belongs to.
     */
    private void addParameter(int paramNum, int paramType,
            PlanPrepStatement aPlanPrepStatement) {
        if (parameterList == null) {
            parameterList = new ArrayList<Parameter>();
        }
        parameterList.add(new Parameter(paramNum, paramType, aPlanPrepStatement));
    }

    /**
     * Main method for preparing the ExecutionStep for parameter substitution.
     *
     * @param anExecStep - the ExecutionStep to process
     */
    private void prepareStepParameters(ExecutionStep anExecStep) {

        StepDetail aStepDetail = anExecStep.aStepDetail;

        if (aStepDetail.queryString != null) {
            int offset = 0;
            offset = aStepDetail.queryString.indexOf("&xp");
            if (offset >= 0) {
                PlanPrepStatement aPlanPrepStatement = addPlanPrepStatement(anExecStep);
                while (offset >= 0) {
                    offset += 3;
                    int startOffset = offset;

                    offset = aStepDetail.queryString.indexOf("xp&", offset);
                    int paramNum = Integer.valueOf(aStepDetail.queryString.substring(
                            startOffset, offset));

                    addParameter(paramNum, paramTypes[paramNum - 1],
                            aPlanPrepStatement);

                    offset = aStepDetail.queryString.indexOf("&xp", offset);
                }
            }
        }
    }

    /**
     * Replaces the appropriate parameter values from the parameter list
     * for PreparedStatements
     *
     * @param paramValues 
     */
    public void substituteParameterValues(String[] paramValues) {

        resetStatements();
        for (Parameter parameter : parameterList) {
            if (parameter.paramNum <= paramValues.length) {
                parameter.substitute(paramValues[parameter.paramNum - 1]);
            }
        }
    }

    /**
     * Reset the prepared statements for parameter substitution.
     */
    private void resetStatements() {
        for (PlanPrepStatement aPPS : planPrepStatementList) {
            aPPS.reset();
        }
    }
}
