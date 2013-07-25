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
package org.postgresql.stado.engine.copy;

import java.sql.SQLException;

/**
 * This is a wrapper class for CopyIn interface of the Postgres driver
 * Delegates respective method calls
 */
public class PgCopyIn implements CopyIn {
    private org.postgresql.driver.copy.CopyIn copyIn;

    /**
     * Constructs new instance of PgCopyIn
     * @param copyIn
     *          Pogtgres driver's CopyIn object
     */
    PgCopyIn(org.postgresql.driver.copy.CopyIn copyIn) {
        this.copyIn = copyIn;
    }

    /**
     * Delegate the call to Postgres CopyIn
     * @param buffer
     *          byte array containing data
     * @param offset
     *          where the data begin
     * @param length
     *          number of data bytes
     * @throws SQLException
     *          if SQL error occurs
     */
    public void writeToCopy(byte[] buffer, int offset, int length) throws SQLException {
        copyIn.writeToCopy(buffer, offset, length);
    }

    /**
     * Delegate the call to Postgres CopyIn
     * @return number of rows affected
     * @throws SQLException
     *          if SQL error occurs
     */
    public long endCopy() throws SQLException {
        return copyIn.endCopy();
    }

    /**
     * Delegate the call to Postgres CopyIn
     * @throws SQLException
     *          if SQL error occurs
     */
    public void cancelCopy() throws SQLException {
        copyIn.cancelCopy();
    }
}
