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

import org.postgresql.driver.core.Query;

/**
 * Common interface for all V3 query implementations.
 *
 * @author Oliver Jowett (oliver@opencloud.com)
 */
interface V3Query extends Query {
    /**
     * Return a list of the SimpleQuery objects that
     * make up this query. If this object is already a
     * SimpleQuery, returns null (avoids an extra array
     * construction in the common case).
     *
     * @return an array of single-statement queries, or <code>null</code>
     *   if this object is already a single-statement query.
     */
    SimpleQuery[] getSubqueries();
}
