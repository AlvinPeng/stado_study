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

import org.postgresql.stado.optimizer.OrderByElement;
import org.postgresql.stado.parser.Command;
import org.postgresql.stado.parser.core.syntaxtree.NodeToken;
import org.postgresql.stado.parser.core.syntaxtree.OrderByItem;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;

/**
 * This class is responsible to take care of the ORDER BY clause in a query at the parser level.
 */
public class OrderByClauseHandler extends DepthFirstVoidArguVisitor {
    Command commandToExecute;

    /**
     * Class constructor
     * @param commandToExecute 
     */
    public OrderByClauseHandler(Command commandToExecute) {
        this.commandToExecute = commandToExecute;
    }

    /**
     * This variable contains the order by list -which is a collection of
     * orderbyElement class.
     */
    public Vector<OrderByElement> orderByList = new Vector<OrderByElement>();

    /**
     * f0 -> SQLSimpleExpression(prn) f1 -> [ "ASC" | "DESC" ]
     * @param n 
     * @param argu 
     * @return 
     */
    @Override
    public void visit(OrderByItem n, Object argu) {
        SQLExpressionHandler aSqlExpressionHandler = new SQLExpressionHandler(
                commandToExecute);
        n.f0.accept(aSqlExpressionHandler, argu);
        OrderByElement aOrderByElement = new OrderByElement();

        aOrderByElement.orderExpression = aSqlExpressionHandler.aroot;
        orderByList.add(aOrderByElement);

        aOrderByElement.orderDirection = OrderByElement.ASC;
        n.f1.accept(this, aOrderByElement);
    }

    /**
     * The information whether the order by clause is ASC or DESC is extracted
     * from the user specified string here
     * 
     * @param n
     * @param argu
     * @return
     */
    @Override
    public void visit(NodeToken n, Object argu) {
        if (n.tokenImage.equalsIgnoreCase("ASC")) {
            OrderByElement argument = (OrderByElement) argu;
            argument.orderDirection = OrderByElement.ASC;
        }

        if (n.tokenImage.equalsIgnoreCase("DESC")) {
            OrderByElement argument = (OrderByElement) argu;
            argument.orderDirection = OrderByElement.DESC;
        }
    }
}
