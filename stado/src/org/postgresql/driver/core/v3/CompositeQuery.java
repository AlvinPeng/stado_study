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
package org.postgresql.driver.core.v3;

import org.postgresql.driver.core.*;

/**
 * V3 Query implementation for queries that involve multiple statements.
 * We split it up into one SimpleQuery per statement, and wrap the
 * corresponding per-statement SimpleParameterList objects in
 * a CompositeParameterList.
 *
 * @author Oliver Jowett (oliver@opencloud.com)
 */
class CompositeQuery implements V3Query {
    CompositeQuery(SimpleQuery[] subqueries, int[] offsets) {
        this.subqueries = subqueries;
        this.offsets = offsets;
    }

    public ParameterList createParameterList() {
        SimpleParameterList[] subparams = new SimpleParameterList[subqueries.length];
        for (int i = 0; i < subqueries.length; ++i)
            subparams[i] = (SimpleParameterList)subqueries[i].createParameterList();
        return new CompositeParameterList(subparams, offsets);
    }

    public String toString(ParameterList parameters) {
        StringBuffer sbuf = new StringBuffer(subqueries[0].toString());
        for (int i = 1; i < subqueries.length; ++i)
        {
            sbuf.append(';');
            sbuf.append(subqueries[i]);
        }
        return sbuf.toString();
    }

    public String toString() {
        return toString(null);
    }

    public void close() {
        for (int i = 0; i < subqueries.length; ++i)
            subqueries[i].close();
    }

    public SimpleQuery[] getSubqueries() {
        return subqueries;
    }

    private final SimpleQuery[] subqueries;
    private final int[] offsets;
}
