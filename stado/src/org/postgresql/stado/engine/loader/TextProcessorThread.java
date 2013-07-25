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
import java.util.TreeMap;

import org.postgresql.stado.common.util.ParseCmdLine;



/**
 * Data processor which process data in plain text format
 *
 * @author amart
 */
public class TextProcessorThread extends DataProcessorThread<byte[]> {

    protected final byte[] nullValueTemplate;

    protected byte[] outputRow;

    protected int[] columnEnds;

    protected int colCount;

    /**
     * Create new processor instance
     * @param loader
     * @param id
     * @param b
     * @throws Exception
     */
    public TextProcessorThread(ILoaderConfigInformation loader, int id, DataReaderAndProcessorBuffer<byte[]> b) throws Exception{
        super(loader, id, b);
        nullValueTemplate = NULLValue.getBytes();
        columnEnds = new int[getColumnCount()];
    }

    /* (non-Javadoc)
     * @see org.postgresql.stado.engine.loader.DataProcessorThread#getValue(int)
     */
    @Override
    protected String getValue(int colIndex) {
        int beginIndex = colIndex == 1 ? 0 : columnEnds[colIndex - 2] + 1;
        String value = new String(outputRow, beginIndex, columnEnds[colIndex - 1] - beginIndex);
        return value.equals(NULLValue) ? null : value;
    }

    /* (non-Javadoc)
     * @see org.postgresql.stado.engine.loader.DataProcessorThread#insertGeneratedValues(java.util.Map)
     */
    @Override
    protected void insertGeneratedValues(Map<Integer,String> values) throws Exception {
        int[] newColumnEnds = new int[colCount + values.size()];
        TreeMap<Integer,byte[]> sortedMap = new TreeMap<Integer,byte[]>();
        int insertedSize = 0;
        for (Map.Entry<Integer,String> entry : values.entrySet()) {
            byte[] byteValue = encodeValue(entry.getValue());
            sortedMap.put(entry.getKey(), entry.getValue().getBytes());
            insertedSize += byteValue.length + 1;
        }
        byte[] newOutputRow = new byte[outputRow.length + insertedSize];
        int srcIndex = 0;
        int destIndex = 0;
        int srcPos = 0;
        int destPos = 0;
        for (Map.Entry<Integer,byte[]> entry : sortedMap.entrySet()) {
            boolean append = entry.getKey() - destIndex + srcIndex == colCount;
            if (srcIndex < entry.getKey() - destIndex + srcIndex) {
                // Copy data before inserted value
                int count = columnEnds[entry.getKey() - destIndex + srcIndex - 1] - srcPos;
                if (!append) {
                    count++;
                }
                System.arraycopy(outputRow, srcPos, newOutputRow, destPos, count);
                // move and update line ends
                int shift = destPos - srcPos;
                while (srcIndex < entry.getKey()) {
                    newColumnEnds[destIndex++] = columnEnds[srcIndex++] + shift;
                }
                srcPos += count;
                destPos += count;
            }
            if (append) {
                newOutputRow[destPos++] = (byte) separator;
            }
            // Copy inserted value
            byte[] aValue = entry.getValue();
            System.arraycopy(aValue, 0, newOutputRow, destPos, aValue.length);
            destPos += aValue.length;
            newColumnEnds[destIndex++] = destPos;
            if (!append) {
                newOutputRow[destPos++] = (byte) separator;
            }
        }
        if (srcIndex < colCount) {
            // Copy remaining
            int count = columnEnds[colCount - 1] - srcPos;
            System.arraycopy(outputRow, srcPos, newOutputRow, destPos, count);
            // move and update line ends
            int shift = destPos - srcPos;
            while (srcIndex < colCount) {
                newColumnEnds[destIndex++] = columnEnds[srcIndex++] + shift;
            }
        }
        columnEnds = newColumnEnds;
        outputRow = newOutputRow;
        colCount += values.size();
    }

    protected byte[] encodeValue(String value) {
        return value == null ? nullValueTemplate : ParseCmdLine.escape(value,
                separator, false).getBytes();
    }

    /* (non-Javadoc)
     * @see org.postgresql.stado.engine.loader.DataProcessorThread#outputRow(org.postgresql.stado.engine.loader.INodeWriter)
     */
    @Override
    protected void outputRow(INodeWriter writer) throws IOException {
        writer.writeRow(outputRow, 0, suppresSendingNodeId ? columnEnds[colCount - 2] : columnEnds[colCount - 1]);
    }

    /* (non-Javadoc)
     * @see org.postgresql.stado.engine.loader.DataProcessorThread#parseRow()
     */
    @Override
    protected boolean parseRow() throws Exception{
        colCount = 0;
        outputRow = getNextRowValue();
        if (outputRow == null) {
            return false;
        }
        int pos = 0;
        int nextPos = find(outputRow, (byte) separator, pos);
        while (nextPos > -1) {
            pos = nextPos + 1;
            // Ensure the separator we just found is not escaped
            boolean escaped = false;
            for (int i = nextPos - 1; i >= 0; i--) {
                if (outputRow[i] == '\\') {
                    escaped = !escaped;
                } else {
                    break;
                }
            }
            if (!escaped) {
                columnEnds[colCount++] = nextPos;
                if (colCount == getColumnCount()) {
                    throw new Exception("Extra data found after last column");
                }

            }
            nextPos = find(outputRow, (byte) separator, pos);
        }
        columnEnds[colCount++] = outputRow.length;
        return true;
    }

    /**
     * Find a byte in supplied buffer
     *
     * @param buffer
     * @param ch byte to find
     * @param pos index to start search from
     * @return index in the buffer or -1 if not found
     */
    protected int find(byte[] buffer, byte ch, int pos) {
        for (int i = pos; i < buffer.length; i++) {
            if (ch == buffer[i]) {
                return i;
            }
        }
        return -1;
    }
}
