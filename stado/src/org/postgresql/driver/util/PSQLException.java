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
package org.postgresql.driver.util;

import java.sql.SQLException;

public class PSQLException extends SQLException
{

    private ServerErrorMessage _serverError;

    public PSQLException(String msg, PSQLState state, Throwable cause)
    {
        super(msg, state == null ? null : state.getState());
        initCause(cause);
    }

    public PSQLException(String msg, PSQLState state)
    {
        this(msg, state, null);
    }

    public PSQLException(ServerErrorMessage serverError)
    {
        this(serverError.toString(), new PSQLState(serverError.getSQLState()));
        _serverError = serverError;
    }

    public ServerErrorMessage getServerErrorMessage()
    {
        return _serverError;
    }

}