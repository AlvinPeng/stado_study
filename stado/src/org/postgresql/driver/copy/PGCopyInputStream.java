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
import java.io.InputStream;
import java.sql.SQLException;

import org.postgresql.driver.PGConnection;
import org.postgresql.driver.util.GT;
import org.postgresql.driver.util.PSQLException;
import org.postgresql.driver.util.PSQLState;

/**
 * InputStream for reading from a PostgreSQL COPY TO STDOUT operation
 */
public class PGCopyInputStream extends InputStream implements CopyOut {
    private CopyOut op;
    private byte[] buf;
    private int at, len;

    /**
     * Uses given connection for specified COPY TO STDOUT operation
     * @param connection database connection to use for copying (protocol version 3 required)
     * @param sql COPY TO STDOUT statement
     * @throws SQLException if initializing the operation fails
     */
    public PGCopyInputStream(PGConnection connection, String sql) throws SQLException {
        this(connection.getCopyAPI().copyOut(sql));
    }

    /**
     * Use given CopyOut operation for reading
     * @param op COPY TO STDOUT operation
     * @throws SQLException if initializing the operation fails
     */
    public PGCopyInputStream(CopyOut op) {
        this.op = op;
    }

    private boolean gotBuf() throws IOException {
        if(at >= len) {
            try {
                buf = op.readFromCopy();
            } catch(SQLException sqle) {
                throw new IOException(GT.tr("Copying from database failed: {0}", sqle));
            }
            if(buf == null) {
                at = -1;
                return false;
            } else {
                at = 0;
                len = buf.length;
                return true;
            }
        }
        return buf != null;
    }

    private void checkClosed() throws IOException {
        if (op == null) {
            throw new IOException(GT.tr("This copy stream is closed."));
        }
    }

    
    public int available() throws IOException {
        checkClosed();
        return ( buf != null ? len - at : 0 );
    }
    
    public int read() throws IOException {
        checkClosed();
        return gotBuf() ? buf[at++] : -1;
    }

    public int read(byte[] buf) throws IOException {
        return read(buf, 0, buf.length); 
    }
    
    public int read(byte[] buf, int off, int siz) throws IOException {
        checkClosed();
        int got = 0;
        while( got < siz && gotBuf() ) {
            buf[off+got++] = this.buf[at++];
        }
        return got;
    }

    public byte[] readFromCopy() throws SQLException {
        byte[] result = buf;
        try {
            if(gotBuf()) {
                if(at>0 || len < buf.length) {
                    byte[] ba = new byte[len-at];
                    for(int i=at; i<len; i++)
                        ba[i-at] = buf[i];
                    result = ba;
                }
                at = len; // either partly or fully returned, buffer is exhausted
            }
        } catch(IOException ioe) {
            throw new PSQLException(GT.tr("Read from copy failed."), PSQLState.CONNECTION_FAILURE);
        }
        return result;
    }

    public void close() throws IOException {
        // Don't complain about a double close.
        if (op == null)
            return;

        try {
            op.cancelCopy();
        } catch(SQLException se) {
            IOException ioe = new IOException("Failed to close copy reader.");
            ioe.initCause(se);
            throw ioe;
        }
        op = null;
    }

    public void cancelCopy() throws SQLException {
        op.cancelCopy();
    }

    public void cancelCopyFinish() throws IOException {
        op.cancelCopyFinish();
    }
        
    public int getFormat() {
        return op.getFormat();
    }
    
    public int getFieldFormat(int field) {
        return op.getFieldFormat(field);
    }

    public int getFieldCount() {
        return op.getFieldCount();
    }

    public boolean isActive() {
        return op.isActive();
    }

    public long getHandledRowCount() {
        return op.getHandledRowCount();
    }
}
