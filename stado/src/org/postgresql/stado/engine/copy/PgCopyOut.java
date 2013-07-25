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

import java.io.IOException;
import java.sql.SQLException;

/**
 * This is a wrapper class for CopyIn interface of the Postgres driver
 * Delegates respective method calls
 */
public class PgCopyOut implements CopyOut {

    private org.postgresql.driver.copy.CopyOut copyOut;

    /**
     * Constructs new instance of PgCopyOut
     * @param copyOut
     *          Postgres driver's CopyOut object
     */
    PgCopyOut(org.postgresql.driver.copy.CopyOut copyOut) {
        this.copyOut = copyOut;
    }

    /**
     * Delegate the call to Postgres CopyOut
     * @return field count
     */
    public int getFieldCount() {
        return copyOut.getFieldCount();
    }

    /**
     * Delegate the call to Postgres CopyOut
     * @return byte array containing the data
     * @throws SQLException
     *          if SQL error occurs
     */
    public byte[] readFromCopy() throws SQLException {
        return copyOut.readFromCopy();
    }

    /**
     * Delegate the call to Postgres CopyOut
     * @throws SQLException
     *          if SQL error occurs
     */
    public void cancelCopy() throws SQLException {
        copyOut.cancelCopy();
    }
    
    public void cancelCopyFinish() throws IOException {
        copyOut.cancelCopyFinish();
    }
}
