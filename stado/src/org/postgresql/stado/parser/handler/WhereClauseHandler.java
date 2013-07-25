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
package org.postgresql.stado.parser.handler;

import java.util.HashSet;
import java.util.Vector;

import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.optimizer.AttributeColumn;
import org.postgresql.stado.optimizer.QueryCondition;
import org.postgresql.stado.optimizer.QueryTree;
import org.postgresql.stado.optimizer.RelationNode;
import org.postgresql.stado.optimizer.SqlExpression;
import org.postgresql.stado.parser.Command;
import org.postgresql.stado.parser.core.syntaxtree.WhereClause;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;


/**
 * The WhereClauseHandler is responsible to take care of anything that is
 * between the Where Clause and the Group by Clause.
 */
public class WhereClauseHandler extends DepthFirstVoidArguVisitor {

    short outerCounter = 0;

    QueryTree aQueryTree;

    Command commandToExecute;

    SysTable targetTable;

    /**
     *
     * @param commandToExecute
     */
    public WhereClauseHandler(Command commandToExecute) {
        this.commandToExecute = commandToExecute;
    }
    /**
     * Get the target Sys table.
     *
     * @return SysTable
     * @param tableName
     * @throws XDBServerException if table not found.
     */
   public SysTable getTargetTable(String tableName) throws XDBServerException {
       if(targetTable == null) {
           targetTable = commandToExecute.getClientContext().getSysDatabase().getSysTable(tableName);
       }
       return targetTable;
   }
    /**
     * f0 -> <WHERE_> f1 -> SQLComplexExpression(prn)
     * @param n
     * @param argu
     * @return
     */
    @Override
    public void visit(WhereClause n, Object argu) {
        // TODO: Dummy Call, revisit to see if it can be removed safely
        n.f0.accept(this, argu);
        QueryConditionHandler qch = new QueryConditionHandler(commandToExecute);

        n.f1.accept(qch, argu);

        aQueryTree = (QueryTree) argu;

        if(qch.isHandleLeftOuterNeeded()) {
            handleLeftOuter(qch);
        } else {
            //clear lists that are no longer needed
            qch.queryCondList.clear();
            qch.innerQueryCondList.clear();
        }

        // the condition list should only be filled with
        // main conditions and that to only if they have a join in
        // and of there sub conditions.

        // -- I will there fore add another variable to the query tree --
        // will have the where root condition.

        // aQueryTree.conditionList.addAll(allConditions);
        aQueryTree.setWhereRootCondition(qch.aRootCondition);
    }

    /**
     * @Return table Names associated with left outer/inner join
     * @Parameters Vector<QueryCondition> queryCondList
     */
    private Vector<String> getTableNames(Vector<QueryCondition> queryCondList) {
        Vector<String> tableNames = new Vector<String>();
        for (QueryCondition rootCond : queryCondList) {
            for (QueryCondition exprCond : QueryCondition.getNodes(rootCond, QueryCondition.QC_SQLEXPR)) {
                for (SqlExpression columnExpr : SqlExpression.getNodes(exprCond.getExpr(), SqlExpression.SQLEX_COLUMN)) {
                    AttributeColumn column = columnExpr.getColumn();
                    RelationNode relNode = column.relationNode;
                    if (relNode != null) {
                        tableNames.add(relNode.getTableName());
                    } else if (column.getTableName() != null && column.getTableName().length() > 0) {
                        tableNames.add(column.getTableName());
                    } else if (column.getTableAlias() != null && column.getTableAlias().length() > 0) {
                        tableNames.add(aQueryTree.getTableNameOfAlias(column.getTableAlias()));
                    }
                }
            }
        }
        return tableNames;
    }
    /**
     * handling for left outer(+)
     */
    private void handleLeftOuter(Object argu) {
        aQueryTree.setOuterJoin(true);
        QueryConditionHandler qch = (QueryConditionHandler)argu;
        try {
            //get table names associated with left outer(+)
            Vector<String> tableNames = getTableNames(qch.queryCondList);
            for(int i=0; i<qch.queryCondList.size(); i++) {
                QueryCondition qc = qch.queryCondList.get(i);
                aQueryTree.getFromClauseConditions().add(qc);
                Vector<QueryCondition> expressions = QueryCondition.getNodes(qc, QueryCondition.QC_SQLEXPR);
                Vector<SqlExpression> vCondExpr = new Vector<SqlExpression>();
                for (QueryCondition qcExpr : expressions) {
                    vCondExpr.add(qcExpr.getExpr());
                }
                QueryTreeHandler.analyzeAndCompleteColInfoAndNodeInfo(vCondExpr.iterator(),
                        aQueryTree.getRelationNodeList(), null, QueryTreeHandler.CONDITION,
                        commandToExecute);
                HashSet<RelationNode> parents = new HashSet<RelationNode>();
                //last relation will be emp in (dept.deptno = emp.deptno)
                RelationNode lastRelation = aQueryTree.getRelHandlerInfo().get(tableNames.get(i));

                for (RelationNode relNode : aQueryTree.getRelationNodeList()) {
                    if (relNode == lastRelation) {
                        continue;
                    }
                    for (QueryCondition qcExpr : expressions) {
                        if (qcExpr.getExpr().contains(relNode)) {
                            parents.add(relNode);
                            break;
                        }
                    }
                }
                lastRelation.addParentNodes(parents, true, ++outerCounter);
            }

            /////////////////////////////////////////////////////////////////////////////
            ///////now handle inner join conditions if there exists some/////////////////
            /////////////////////////////////////////////////////////////////////////////
            if(qch.isHandleInnerJoinNeeded()) {
                // get table names assoicated with inner join
                tableNames = getTableNames(qch.innerQueryCondList);
                for(int i=0; i<qch.innerQueryCondList.size(); i++) {
                    QueryCondition qc = qch.innerQueryCondList.get(i);
                    aQueryTree.getFromClauseConditions().add(qc);
                    Vector<QueryCondition> expressions = QueryCondition.getNodes(qc, QueryCondition.QC_SQLEXPR);
                    Vector<SqlExpression> vCondExpr = new Vector<SqlExpression>();
                    for (QueryCondition qcExpr : expressions) {
                        vCondExpr.add(qcExpr.getExpr());
                    }
                    QueryTreeHandler.analyzeAndCompleteColInfoAndNodeInfo(vCondExpr.iterator(),
                            aQueryTree.getRelationNodeList(), null, QueryTreeHandler.CONDITION,
                            commandToExecute);
                    //last relation will be emp in (dept.deptno = emp.deptno)
                    RelationNode lastRelation = aQueryTree.getRelHandlerInfo().get(tableNames.get(i));

                    for (RelationNode relNode : aQueryTree.getRelationNodeList()) {
                        if (relNode == lastRelation) {
                            continue;
                        }
                        for (QueryCondition qcExpr : expressions) {
                            if (qcExpr.getExpr().contains(relNode)) {
                                lastRelation.addSiblingJoin(relNode);
                            }
                        }
                    }
                }
            }
        } finally {
            qch.innerQueryCondList.clear();
            qch.queryCondList.clear();
        }
    }
}
