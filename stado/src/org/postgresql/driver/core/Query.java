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
package org.postgresql.driver.core;

/**
 * Abstraction of a generic Query, hiding the details of
 * any protocol-version-specific data needed to execute
 * the query efficiently.
 *<p>
 * Query objects should be explicitly closed when no longer
 * needed; if resources are allocated on the server for this
 * query, their cleanup is triggered by closing the Query.
 *
 * @author Oliver Jowett (oliver@opencloud.com)
 */
public interface Query {
    /**
     * Create a ParameterList suitable for storing parameters
     * associated with this Query.
     *<p>
     * If this query has no parameters, a ParameterList will
     * be returned, but it may be a shared immutable object.
     * If this query does have parameters, the returned
     * ParameterList is a new list, unshared by other callers.
     *
     * @return a suitable ParameterList instance for this query
     */
    ParameterList createParameterList();

    /**
     * Stringize this query to a human-readable form, substituting
     * particular parameter values for parameter placeholders.
     *
     * @param parameters a ParameterList returned by this Query's
     *  {@link #createParameterList} method, or <code>null</code> to
     *  leave the parameter placeholders unsubstituted.
     * @return a human-readable representation of this query
     */
    String toString(ParameterList parameters);

    /**
     * Close this query and free any server-side resources associated
     * with it. The resources may not be immediately deallocated, but
     * closing a Query may make the deallocation more prompt.
     *<p>
     * A closed Query should not be executed.
     */
    void close();
}
