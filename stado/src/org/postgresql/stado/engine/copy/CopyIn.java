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
 * Objects implementing this interface are used to control running COPY FROM command
 */
public interface CopyIn {

    /**
     * Use specified data as a source for the COPY command
     * @param buffer
     *          byte array containing data
     * @param offset
     *          where the data begin
     * @param length
     *          number of data bytes
     * @throws SQLException
     *          if SQL error occurs
     */
    void writeToCopy(byte[] buffer, int offset, int length) throws SQLException;

    /**
     * Finish the COPY command
     * @return number of rows affected
     * @throws SQLException
     *          if SQL error occurs
     */
    long endCopy() throws SQLException;

    /**
     * Cancel the COPY command
     * @throws SQLException
     *          if SQL error occurs
     */
    void cancelCopy() throws SQLException;
}
