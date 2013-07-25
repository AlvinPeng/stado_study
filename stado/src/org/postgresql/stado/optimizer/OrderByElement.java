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
 * OrderByClause.java
 *
 */

package org.postgresql.stado.optimizer;

/**
 * 
 * DS
 */
public class OrderByElement implements IRebuildString {

    public static final int ASC = 1;

    public static final int DESC = 2;

    public SqlExpression orderExpression;

    public int orderDirection = ASC;

    /** Creates a new instance of OrderByClause */
    public OrderByElement() {

    }

    /**
     * 
     * @return 
     */

    public String getDirectionString() {
        switch (orderDirection) {
        case ASC:
            return "ASC";

        case DESC:
            return "DESC";

        default:
            // raise error?
        }
        return "";
    }

    /**
     * 
     * @return 
     */

    public String rebuildString() {
        String orderByString = "";
        orderExpression.rebuildExpression();
        orderByString = orderExpression.getExprString() + " " + getDirectionString();
        return orderByString;
    }
}
