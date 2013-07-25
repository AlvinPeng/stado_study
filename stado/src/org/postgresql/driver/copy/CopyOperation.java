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
package org.postgresql.driver.copy;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Exchange bulk data between client and PostgreSQL database tables.
 * See CopyIn and CopyOut for full interfaces for corresponding copy directions.
 */
public interface CopyOperation {

    /**
     * @return number of fields in each row for this operation
     */
    int getFieldCount();

    /**
     * @return overall format of each row: 0 = textual, 1 = binary
     */
    int getFormat();

    /**
     * @param field number of field (0..fieldCount()-1)
     * @return format of requested field: 0 = textual, 1 = binary
     */
    int getFieldFormat(int field);
    
    /**
     * @return is connection reserved for this Copy operation?
     */
    boolean isActive();
    
    /**
     * Cancels this copy operation, discarding any exchanged data.
     * @throws SQLException if cancelling fails
     */
    void cancelCopy() throws SQLException;

    /**
     * Post cancelCopy cleanup, because cancelCopy may have been 
     * called from another thread and have a lock and 
     * pending messages
     * 
     * @throws SQLException if canceling fails
     */
    void cancelCopyFinish() throws IOException;
    
    /**
     * After succesful end of copy, returns the number
     * of database records handled in that operation.
     * Only implemented in PostgreSQL server version 8.2 and up.
     * Otherwise, returns -1.
     * @return number of handled rows or -1
     */
    public long getHandledRowCount();
}
