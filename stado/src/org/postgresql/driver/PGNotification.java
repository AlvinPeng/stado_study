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
package org.postgresql.driver;

/**
 *    This interface defines the public PostgreSQL extension for Notifications
 */
public interface PGNotification
{
    /**
     * Returns name of this notification
     * @since 7.3
     */
    public String getName();

    /**
     * Returns the process id of the backend process making this notification
     * @since 7.3
     */
    public int getPID();

    /**
     * Returns additional information from the notifying process.
     * This feature has only been implemented in server versions 9.0
     * and later, so previous versions will always return an empty String.
     *
     * @since 8.0
     */
    public String getParameter();

}

