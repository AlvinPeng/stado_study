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

import org.postgresql.driver.PGNotification;

public class Notification implements PGNotification
{
    public Notification(String p_name, int p_pid)
    {
        this(p_name, p_pid, "");
    }

    public Notification(String p_name, int p_pid, String p_parameter)
    {
        m_name = p_name;
        m_pid = p_pid;
        m_parameter = p_parameter;
    }

    /*
     * Returns name of this notification
     */
    public String getName()
    {
        return m_name;
    }

    /*
     * Returns the process id of the backend process making this notification
     */
    public int getPID()
    {
        return m_pid;
    }

    public String getParameter()
    {
        return m_parameter;
    }

    private String m_name;
    private String m_parameter;
    private int m_pid;

}

