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

import java.io.IOException;
import java.sql.SQLException;

import org.postgresql.driver.copy.CopyOut;

/**
 * Anticipated flow of a COPY TO STDOUT operation:
 * 
 * CopyManager.copyOut()
 *   ->QueryExecutor.startCopy()
 *       - sends given query to server
 *       ->processCopyResults():
 *           - receives CopyOutResponse from Server
 *           - creates new CopyOutImpl
 *           ->initCopy():
 *              - receives copy metadata from server
 *              ->CopyOutImpl.init()
 *              ->lock() connection for this operation
 *   - if query fails an exception is thrown
 *   - if query returns wrong CopyOperation, copyOut() cancels it before throwing exception
 * <-returned: new CopyOutImpl holding lock on connection
 * repeat CopyOut.readFromCopy() until null
 *   ->CopyOutImpl.readFromCopy()
 *       ->QueryExecutorImpl.readFromCopy()
 *           ->processCopyResults()
 *               - on copydata row from server
 *                   ->CopyOutImpl.handleCopydata() stores reference to byte array
 *               -  on CopyDone, CommandComplete, ReadyForQuery
 *                   ->unlock() connection for use by other operations
 * <-returned: byte array of data received from server or null at end.
 */
public class CopyOutImpl extends CopyOperationImpl implements CopyOut {
    private byte[] currentDataRow;

    public byte[] readFromCopy() throws SQLException {
        currentDataRow = null;
        queryExecutor.readFromCopy(this);
        return currentDataRow;
    }

    void handleCopydata(byte[] data) {
        currentDataRow = data;
    }
    
    public void cancelCopyFinish() throws IOException {
        queryExecutor.cancelCopyFinish(this);
    }
}
