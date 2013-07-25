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

import org.postgresql.stado.parser.Command;
import org.postgresql.stado.parser.core.syntaxtree.SQLExpressionList;
import org.postgresql.stado.parser.core.syntaxtree.SQLExpressionListItem;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;

/**
 * This class constructs the list of SqlExpression objects that is utilized
 * by QueryConditionHandler class.
 */
public class SQLExpressionListHandler extends DepthFirstVoidArguVisitor {
    Command commandToExecute;

    public SQLExpressionListHandler(Command commandToExecute) {
        this.commandToExecute = commandToExecute;
    }

    Vector vSqlExpressionList = new Vector();

    /**
     * f0 -> SQLExpressionListItem(prn) f1 -> ( "," SQLExpressionListItem(prn) )*
     */
    @Override
    public void visit(SQLExpressionList n, Object argu) {
        n.f0.accept(this, argu);
        n.f1.accept(this, argu);
    }

    /**
     * f0 -> SQLSimpleExpression(prn)
     */
    @Override
    public void visit(SQLExpressionListItem n, Object argu) {
        SQLExpressionHandler aSqlExpressionHandler = new SQLExpressionHandler(
                commandToExecute);
        n.f0.accept(aSqlExpressionHandler, argu);
        vSqlExpressionList.add(aSqlExpressionHandler.aroot);
    }

}
