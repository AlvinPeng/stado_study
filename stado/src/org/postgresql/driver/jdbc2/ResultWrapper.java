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
package org.postgresql.driver.jdbc2;

import java.sql.*;

/**
 * Helper class that storing result info. This handles both the
 * ResultSet and no-ResultSet result cases with a single interface for
 * inspecting and stepping through them.
 *
 * @author Oliver Jowett (oliver@opencloud.com)
 */
public class ResultWrapper {
    public ResultWrapper(ResultSet rs) {
        this.rs = rs;
        this.updateCount = -1;
        this.insertOID = -1;
    }

    public ResultWrapper(int updateCount, long insertOID) {
        this.rs = null;
        this.updateCount = updateCount;
        this.insertOID = insertOID;
    }

    public ResultSet getResultSet() {
        return rs;
    }

    public int getUpdateCount() {
        return updateCount;
    }

    public long getInsertOID() {
        return insertOID;
    }

    public ResultWrapper getNext() {
        return next;
    }

    public void append(ResultWrapper newResult) {
        ResultWrapper tail = this;
        while (tail.next != null)
            tail = tail.next;

        tail.next = newResult;
    }

    private final ResultSet rs;
    private final int updateCount;
    private final long insertOID;
    private ResultWrapper next;
}
