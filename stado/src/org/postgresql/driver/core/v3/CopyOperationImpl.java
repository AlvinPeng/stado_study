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

import org.postgresql.driver.copy.CopyOperation;
import org.postgresql.driver.util.GT;
import org.postgresql.driver.util.PSQLException;
import org.postgresql.driver.util.PSQLState;

public class CopyOperationImpl implements CopyOperation {
    QueryExecutorImpl queryExecutor;
    int rowFormat;
    int[] fieldFormats;
    long handledRowCount = -1;
    
    void init(QueryExecutorImpl q, int fmt, int[] fmts) {
        queryExecutor = q;
        rowFormat = fmt;
        fieldFormats = fmts;
    }

    public void cancelCopy() throws SQLException {
        queryExecutor.cancelCopy(this);
    }

    public void cancelCopyFinish() throws IOException {
        queryExecutor.cancelCopyFinish(this);
    }
        
    public int getFieldCount() {
        return fieldFormats.length;
    }

    public int getFieldFormat(int field) {
        return fieldFormats[field];
    }

    public int getFormat() {
        return rowFormat;
    }

    public boolean isActive() {
        synchronized(queryExecutor) {
            return queryExecutor.hasLock(this);
        }
    }
    
    public void handleCommandStatus(String status) throws PSQLException {
        if(status.startsWith("COPY")) {
            int i = status.lastIndexOf(' ');
            handledRowCount = i > 3 ? Long.parseLong(status.substring( i + 1 )) : -1;
        } else {
            throw new PSQLException(GT.tr("CommandComplete expected COPY but got: " + status), PSQLState.COMMUNICATION_ERROR);
        }
    }

    public long getHandledRowCount() {
        return handledRowCount;
    }
}
