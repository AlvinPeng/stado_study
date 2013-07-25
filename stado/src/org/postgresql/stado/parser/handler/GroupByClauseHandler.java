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

import java.util.Vector;

import org.postgresql.stado.optimizer.QueryCondition;
import org.postgresql.stado.parser.Command;
import org.postgresql.stado.parser.core.syntaxtree.HavingClause;
import org.postgresql.stado.parser.core.syntaxtree.SQLExpressionList;
import org.postgresql.stado.parser.core.syntaxtree.SQLExpressionListItem;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;

/**
 * This class is responsible for handling the group by clause visitors. It holds
 * the information about the group by clause.
 * 
 */
public class GroupByClauseHandler extends DepthFirstVoidArguVisitor {
    private Command commandToExecute;

    /**
     * Class constructor
     * @param commandToExecute 
     */
    public GroupByClauseHandler(Command commandToExecute) {
        this.commandToExecute = commandToExecute;
    }

    /**
     * This contains the list of expression for which we intend to do group by.
     */
    public Vector expressionList = new Vector();

    /**
     * This contains the root condition for the having clause if any.
     */
    public Vector havingList = new Vector();

    /**
     * f0 -> SQLExpressionListItem(prn) f1 -> ( "," SQLExpressionListItem(prn) )*
     * 
     * @param n
     * @param argu
     * @return
     */
    @Override
    public void visit(SQLExpressionList n, Object argu) {
        n.f0.accept(this, argu);
        n.f1.accept(this, argu);
    }

    /**
     * f0 -> SQLSimpleExpression(prn)
     * @param n 
     * @param argu 
     * @return 
     */
    @Override
    public void visit(SQLExpressionListItem n, Object argu) {
        SQLExpressionHandler sqlHandler = new SQLExpressionHandler(
                commandToExecute);
        n.f0.accept(sqlHandler, argu);
        expressionList.add(sqlHandler.aroot);
    }

    /**
     * f0 -> "HAVING" f1 -> SQLComplexExpression(prn)
     * @param n 
     * @param argu 
     * @return 
     */
    @Override
    public void visit(HavingClause n, Object argu) {
        n.f0.accept(this, argu);
        QueryConditionHandler qch = new QueryConditionHandler(commandToExecute);
        n.f1.accept(qch, argu);
        QueryCondition qc = qch.aRootCondition;
        havingList.add(qc);
    }
}
