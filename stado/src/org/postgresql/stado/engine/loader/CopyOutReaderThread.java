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

import java.util.concurrent.Callable;

import org.postgresql.stado.engine.copy.CopyOut;
import org.postgresql.stado.exception.XDBDataReaderException;


/**
 * Purpose of Data Reader is to split data source on rows
 * ResultSetReaderThread assumes the data source is a java.sql.ResultSet
 *
 * @author amart
 */
public class CopyOutReaderThread implements Callable<Boolean> {

    private DataReaderAndProcessorBuffer<byte[]> loadBuffer;

    private CopyOut copyOut;

    /**
     * Creates a new instance of ResultSetReaderThread
     * @param copyOut
     *
     * @param buffer
     * @param groupHashList List of expression strings to use to create
     *        a hashable String on. Used for GROUP BY processing
     * @throws XDBDataReaderException
     */
    public CopyOutReaderThread(CopyOut copyOut,
            DataReaderAndProcessorBuffer<byte[]> buffer,
            int[] groupHashList)
            throws XDBDataReaderException {
        try {
            this.copyOut = copyOut;
            loadBuffer = buffer;
        } catch (Exception ex) {
            throw new XDBDataReaderException(ex);
        }
    }

    /**
     * Closes data source.
     *
     */
    public void close() {
        try {
            if (copyOut != null) {
                copyOut.cancelCopy();
            }
        } catch (Exception ex) {
            // Ignore exception message.
        }
    }

    /**
     * @see java.util.concurrent.Callable#call()
     */
    public Boolean call() throws XDBDataReaderException {
        byte[] rowColsValue;
        try {
            while ((rowColsValue = copyOut.readFromCopy()) != null) {
            	// Remove possible trailing EOL characters
            	int i = rowColsValue.length - 1;
            	// Find index of last non-EOL char
            	while (i-- > 0 && (rowColsValue[i] == '\n' || rowColsValue[i] == '\r')) /*NOOP*/;
            	int newLen = i + 1;
            	if (newLen < rowColsValue.length) {
            		byte[] newBuf = new byte[newLen];
            		if (newLen > 0) {
            			System.arraycopy(rowColsValue, 0, newBuf, 0, newLen);
            		}
            		rowColsValue = newBuf;
            	}
                loadBuffer.putRowValue(rowColsValue, null);
            }
            return true;
        } catch (Exception ex) {
            if (ex instanceof XDBDataReaderException) {
                throw (XDBDataReaderException) ex;
            } else {
                throw new XDBDataReaderException(ex);
            }
        } finally {
            close();
            if (loadBuffer != null) {
                loadBuffer.markFinished();
            }
        }
    }
}
