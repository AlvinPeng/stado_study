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
package org.postgresql.stado.metadata;

import java.sql.ResultSet;
import java.sql.Types;
import java.util.Iterator;
import java.util.Map;

import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.XDBGeneratorException;


/**
 * 
 */
public class SysSerialIDHandler extends SysSerialGenerator {
    private SysColumn column;

    private XDBSessionContext client;

    SysSerialIDHandler(SysColumn column) {
        this.column = column;
        // Modify limit for smallint and int data type
        // Keep Long.MAX_VALUE by default
        switch (column.getColType()) {
        case Types.SMALLINT:
            setMaxValue(Short.MAX_VALUE);
            break;

        case Types.INTEGER:
            setMaxValue(Integer.MAX_VALUE);
            break;

        default:
            break;
        }
    }

    public long allocateRange(long length, XDBSessionContext client)
            throws XDBGeneratorException {
        this.client = client;
        try {
            return allocateRange(length);
        } finally {
            this.client = null;
        }
    }

    @Override
    protected void update() throws XDBGeneratorException {
        if (client == null) {
            throw new XDBGeneratorException(
                    "Can not update SerialID generator - client session is not supplied");
        }
        String rowIDSelect = "select max(" + column.getColName() + " ) from "
                + column.getSysTable().getTableName();
        Engine engine = Engine.getInstance();
        try {
            long maxValue = 0;
            Map results = engine.executeQueryOnMultipleNodes(rowIDSelect,
                    column.getSysTable().getJoinNodeList(), client);
            for (Iterator it = results.values().iterator(); it.hasNext();) {
                ResultSet rs = (ResultSet) it.next();
                try {
                    if (rs.next()) {
                        long nodeMax = rs.getLong(1);
                        if (nodeMax > maxValue) {
                            maxValue = nodeMax;
                        }
                    }
                } finally {
                    rs.close();
                }
            }
            update(maxValue + 1);
        } catch (Exception ex) {
            throw new XDBGeneratorException(
                    "Failed to update SerialID generator", ex);
        }
    }
}
