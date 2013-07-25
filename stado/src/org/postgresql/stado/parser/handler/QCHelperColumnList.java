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

/**
 * This class acts as an helper class and is utilized by QueryTreeHandler class to 
 * transfer information about a query condition such that the columns are grouped in
 * 2 categories
 * 1. Columns in the query condition which belong to this particular tree 
 * 2. Columns in query condition which belong to some other tree
 */
public class QCHelperColumnList {
    /**
     * It represents the columns in the query condition which belong to the specific
     * tree.
     */ 
    private Vector columnExprListThisTree;

    /**
     * It represents the columns in the query condition which belong to the parent
     * tree.
     */ 
    private Vector columnExprListParentTree;

    /**
     * Class constructor.
     * @param columnExprListThisTree Represents the columns in the query condition which belong to the 
     * specific tree.
     * @param columnExprListParentTree Represents the columns in the query condition which belong to the 
     * parent tree.
     */
    public QCHelperColumnList(Vector columnExprListThisTree,
            Vector columnExprListParentTree) {
        this.columnExprListParentTree = columnExprListParentTree;
        this.columnExprListThisTree = columnExprListThisTree;
    }

    public Vector getColumnExprListThisTree() {
        return columnExprListThisTree;
    }

    public Vector getColumnExprListParentTree() {
        return columnExprListParentTree;
    }
}
