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

import java.util.Stack;

import org.postgresql.stado.optimizer.QueryTree;


/**
 * This class acts as the stack holder to hold a QueryTree as the query passes
 * through Parser to Optimizer phases.
 */
public class QueryTreeTracker {
    Stack<QueryTree> queryTreeStack = new Stack<QueryTree>();

    /**
     * Class constructor.
     */
    public QueryTreeTracker() {

    }

    /**
     * It adds the QueryTree represented by aQueryTree in the QueryTree stack.
     * @param aQueryTree A QueryTree object to register and add in the stack.
     */
    public void registerTree(QueryTree aQueryTree) {
        if (!queryTreeStack.isEmpty())
        {
            aQueryTree.setParentQueryTree(queryTreeStack.peek());
            aQueryTree.setTopMostParentQueryTree(queryTreeStack.peek().getTopMostParentQueryTree());
        }
        queryTreeStack.push(aQueryTree);
    }

    /**
     * It removes and returns the QueryTree from the top of QueryTree stack.
     * @return A QueryTree object.
     */
    public QueryTree deRegisterCurrentTree() {
        QueryTree aCheckTree = queryTreeStack.pop();
        return aCheckTree;
    }

    /**
     * It returns the QueryTree object from the top of stack without removing
     * it from stack.
     * @return A QueryTree object.
     */
    public QueryTree GetCurrentTree() {
        return queryTreeStack.peek();
    }
}
