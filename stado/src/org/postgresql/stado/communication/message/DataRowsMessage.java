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
/**
 * 
 */
package org.postgresql.stado.communication.message;

import java.util.Arrays;
import java.util.Collection;

/**
 * 
 * 
 */
public class DataRowsMessage extends NodeMessage {
    private static final long serialVersionUID = 1422470324462405896L;

    /**
     * When SEND_DATA or SEND_DATA_BROADCAST, this is the data message sequence
     * number, also used in acknowledgements.
     */
    private long dataSeqNo;

    /**
     * Contains the insertion string info.
     */
    private String rowData[] = null;

    private int rowCount;

    private int rowSize;

    private boolean autocommit = false; 
    
    /** Parameterless constructor required for serialization */
    public DataRowsMessage() {
    }

    /**
     * @param messageType
     */
    protected DataRowsMessage(int messageType) {
        super(messageType);
    }

    /**
     * Add row data, returns the row number (from 0)
     */
    @Override
    public int addRowData(String rowString) {
        if (rowData == null) {
            rowData = new String[MAX_DATA_ROWS];
        }
        rowData[rowCount] = rowString;
        rowSize += (rowString == null ? 0 : rowString.length());
        return rowCount++;
    }

    @Override
    public String[] getRowData() {
        if (rowData == null) {
            return null;
        }
        String[] out = new String[rowCount];
        System.arraycopy(rowData, 0, out, 0, rowCount);
        return out;
    }

    @Override
    public int getRowCount() {
        return rowCount;
    }

    @Override
    public boolean canAddRows() {
        return rowCount < MAX_DATA_ROWS && rowSize < MAX_DATA_SIZE;
    }

    @Override
    public void setDataSeqNo(long value) {
        dataSeqNo = value;
    }

    @Override
    public long getDataSeqNo() {
        return dataSeqNo;
    }

    // MSG_DROP_TEMP_TABLES
    @Override
    public void setTempTables(Collection<String> tableNames) {
        if (tableNames == null) {
            rowData = null;
        } else {
            rowData = tableNames.toArray(new String[tableNames.size()]);
        }
    }

    @Override
    public Collection getTempTables() {
        return rowData == null ? null : Arrays.asList(rowData);
    }

    @Override
    public boolean getAutocommit() {
        return autocommit;
    }

    @Override
    public void setAutocommit(boolean autocommit) {
        this.autocommit = autocommit;
    }

    @Override
    public Object clone() {
        DataRowsMessage message = (DataRowsMessage) super.clone();
        if (rowData != null) {
            message.rowData = new String[rowData.length];
            System.arraycopy(rowData, 0, message.rowData, 0, rowData.length);
        }
        return message;
    }
}
