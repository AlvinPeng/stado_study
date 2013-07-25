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
package org.postgresql.stado.engine.loader;

import java.io.IOException;
import java.util.Map;

import org.postgresql.stado.common.util.ParseCmdLine;
import org.postgresql.stado.common.util.Props;


/**
 * Data processor which process data from a Result Set (sending intermediate
 * results)
 *
 * @author amart
 */
public class ResultSetProcessorThread extends DataProcessorThread<String[]> {

    private String[] colsValue;

    /*
     * We will use rowValueBuffer in order to process each row value. We declare
     * it here as we want a single buffer instance for row value processing in
     * this instance of data processor thread.
     */
    private final StringBuilder rowValueBuffer = new StringBuilder(8192);

    private byte[] outputRow;

    /**
     * Create new processor instance
     * @param loader
     * @param id
     * @param b
     * @throws Exception
     */
    public ResultSetProcessorThread(ILoaderConfigInformation loader, int id, DataReaderAndProcessorBuffer<String[]> b) throws Exception{
        super(loader, id, b);
    }

    @Override
    protected boolean parseRow() {
        outputRow = null;
        return (colsValue = getNextRowValue()) != null;
    }

    @Override
    protected void insertGeneratedValues(Map<Integer, String> values) {
	if (values.size() > 0) {
	    int i = 0;
	    String[] newValue = new String[colsValue.length + values.size()];
	    for (int j = 0; j < newValue.length; j++) {
		if (values.containsKey(j)) {
		    newValue[j] = values.get(j);
		} else {
		    newValue[j] = colsValue[i++];
		}
	    }
	    colsValue = newValue;
	}
    }

    @Override
    protected String getValue(int colIndex) {
        return colsValue[colIndex - 1];
    }

    @Override
    protected void outputRow(INodeWriter writer) throws IOException {
        if (outputRow == null) {
            outputRow = buildValueList(colsValue, Props.XDB_LOADER_ROW_VALUE_ESCAPE_BACKSLASHES).getBytes();
        }
        writer.writeRow(outputRow);
    }

    /**
     * This is based on prepareValueMap Exported with minimal changes from the
     * previous implementation of Loader class
     */
    private String buildValueList(String[] values, boolean escapeBackslashes) {
        rowValueBuffer.setLength(0);

        int rsColCount = suppresSendingNodeId ? values.length - 1 : values.length;

        for (int i = 0; i < rsColCount; i++) {
            if (i > 0) {
                rowValueBuffer.append(Props.XDB_LOADER_NODEWRITER_DEFAULT_DELIMITER);
            }

            if (values[i] == null) {
                rowValueBuffer.append(Props.XDB_LOADER_NODEWRITER_DEFAULT_NULL);
            } else {
                if (escapeBackslashes) {
                    values[i] = ParseCmdLine.escape(values[i], -1, false);
                }
                rowValueBuffer.append(values[i]);
            }
        }
        return rowValueBuffer.toString();
    }

    @Override
    protected int getColumnCount() {
        return colsValue.length;
    }

}
