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



/**
 * Data processor which process data in CSV format
 *
 *
 * @author amart
 */
public class CsvProcessorThread extends TextProcessorThread {

    private byte[] nullValueTemplate;

    /**
     * Create new processor instance
     * @param loader
     * @param id
     * @param b
     * @throws Exception
     */
    public CsvProcessorThread(ILoaderConfigInformation loader, int id, DataReaderAndProcessorBuffer<byte[]> b) throws Exception{
        super(loader, id, b);
        nullValueTemplate = NULLValue.getBytes();
    }

    private StringBuilder strBuilder = new StringBuilder();
    /* (non-Javadoc)
     * @see org.postgresql.stado.engine.loader.TextProcessorThread#encodeValue(java.lang.String)
     */
    @Override
    protected byte[] encodeValue(String value) {
        if (value == null) {
            return nullValueTemplate;
        }
        boolean needQuote = value.length() > 0 && value.charAt(0) == getQuoteChar() ||
            value.equals(nullValueTemplate);
        for (int i = 0; i < value.length() && !needQuote; i++) {
            char ch = value.charAt(i);
            needQuote |= ch == separator || ch == '\r' || ch == '\n';
        }
        if (needQuote) {
            strBuilder.append(getQuoteChar());
            int pos = 0;
            int nextPos = value.indexOf(getQuoteChar(), pos);
            while (nextPos != -1) {
                strBuilder.append(value.substring(pos, nextPos - 1));
                strBuilder.append(getQuoteEscape()).append(getQuoteChar());
                pos = nextPos + 1;
                nextPos = value.indexOf(getQuoteChar(), pos);
            }
            strBuilder.append(value.substring(pos));
            strBuilder.append(getQuoteChar());
            return strBuilder.toString().getBytes();
        }
        return value.getBytes();
    }

    /* (non-Javadoc)
     * @see org.postgresql.stado.engine.loader.TextProcessorThread#parseRow()
     */
    @Override
    protected boolean parseRow() throws Exception {
        outputRow = getNextRowValue();
        if (outputRow == null) {
            return false;
        }
        boolean openQuote = false;
        columnEnds = new int[getColumnCount()];
        colCount = 0;
        int pos = 0;
        int nextPos = 0;
        while (colCount < columnEnds.length) {
            if (openQuote) {
                nextPos = find(outputRow, (byte) getQuoteChar(), pos);
                if (nextPos == -1) {
                    // Not completed quoted value, append next row
                    byte[] nextRow = getNextRowValue();
                    if (nextRow == null) {
                        throw new Exception("Closing quote not found");
                    }
                    byte[] newOutputRow;
                    try {
                        newOutputRow = new byte[outputRow.length + nextRow.length];
                    } catch (OutOfMemoryError e) {
                        outputRow = null;
                        throw new Exception("Not enough heap memory, probably doe to malformed input");
                    }
                    System.arraycopy(outputRow, 0, newOutputRow, 0, outputRow.length);
                    System.arraycopy(nextRow, 0, newOutputRow, outputRow.length, nextRow.length);
                    outputRow = newOutputRow;
                } else {
                    if (getQuoteChar() == getQuoteEscape() && nextPos < outputRow.length - 1 && outputRow[nextPos + 1] == getQuoteChar()) {
                        // Skip escape and quote quote
                        pos = nextPos + 2;
                    } else if (getQuoteChar() != getQuoteEscape() && outputRow[nextPos - 1] == getQuoteEscape()) {
                        // Skip escaped quote
                        pos = nextPos + 1;
                    } else {
                        columnEnds[colCount++] = ++nextPos;
                        if (nextPos == outputRow.length || outputRow[nextPos] == '\n' || outputRow[nextPos] == '\r') {
                            // end of the row
                            return true;
                        } else if (outputRow[nextPos] == separator) {
                            pos = nextPos + 1;
                            openQuote = false;
                        } else {
                            throw new Exception("Delimiter is expected after closing quote");
                        }
                    }
                }
            } else {
                if (pos < outputRow.length && outputRow[pos] == getQuoteChar()) {
                    openQuote = true;
                    pos++;
                    continue;
                } else {
                    nextPos = find(outputRow, (byte) separator, pos);
                    if (nextPos == -1) {
                        // Last value, point to eol marker if present
                        nextPos = outputRow.length;
                        while (nextPos > 1 && (outputRow[nextPos - 1] == '\n' || outputRow[nextPos - 1] == '\r')) {
                            nextPos--;
                        }
                        columnEnds[colCount++] = nextPos;
                        return true;
                    } else {
                        columnEnds[colCount++] = nextPos;
                        pos = nextPos + 1;
                    }
                }
            }
        }
        throw new Exception("Extra data found afer last field");
    }

    protected String getValue(int idx) {
    	String result = super.getValue(idx);
    	// Strip off quotes if they are present
    	if (result != null && result.length() > 1 
    			&& result.charAt(0) == getQuoteChar() 
    			&& result.charAt(result.length() - 1) == getQuoteChar()) {
    		result = result.substring(1, result.length() - 1);
    		String escapedQuote = "" + getQuoteEscape() + getQuoteChar();
    		int first = 0;
    		int pos;
    		while ((pos = result.indexOf(escapedQuote, first)) >= 0) {
    			result = result.substring(0, pos) + result.substring(pos + 1);
    			first = pos + 1;
    		}
    	}
    	return result;
    }
}
