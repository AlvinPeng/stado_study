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
package org.postgresql.stado.engine.loader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import org.postgresql.stado.common.util.ParseCmdLine;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.copy.CopyIn;
import org.postgresql.stado.engine.copy.CopyManager;
import org.postgresql.stado.metadata.NodeDBConnectionInfo;


/**
 * Derived from PgWriter
 */
public class PostgresWriter implements INodeWriter {
    private static final XLogger logger = XLogger.getLogger(PostgresWriter.class);

    private String writerID;

    private NodeDBConnectionInfo connectionInfo;

    /**
     */
    public PostgresWriter(NodeDBConnectionInfo connectionInfo, 
    		String template, Map<String,String> m) {
    	this.connectionInfo = connectionInfo;

        this.writerID = connectionInfo.getDbName() + "@"
                + connectionInfo.getDbHost();
        m.put("dbhost", connectionInfo.getDbHost());
        if (connectionInfo.getDbPort() > 0) {
            m.put("dbport", "" + connectionInfo.getDbPort());
        }
        m.put("database", connectionInfo.getDbName());
        m.put("dbusername", connectionInfo.getDbUser());
        m.put("dbpassword", connectionInfo.getDbPassword());
        this.copyQuery = ParseCmdLine.substitute(template, m);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Util.loader.INodeWriter#start(boolean)
     */
    private String copyQuery;

    private Connection conn = null;

    private CopyIn copyIn;
    
    private long rowCount = 0;

    public void start() throws IOException {

        try {
            conn = LoaderConnectionPool.getConnectionPool().getConnection(connectionInfo);
            conn.createStatement().executeUpdate("BEGIN");
        } catch (Exception e) {
            throw new IOException("Can not start Writer " + writerID + ":"
                    + e.getMessage());
        }
    }

    private byte[] ROW_VALUES_DELIMITER = Props.XDB_LOADER_NODEWRITER_ROW_DELIMITER.getBytes();

    public synchronized void writeRow(byte[] row) throws IOException {
        writeRow(row, 0, row.length);
    }

    public synchronized void writeRow(byte[] row, int offset, int length) throws IOException {
    	try {
    		if (copyIn == null) {
                copyIn = CopyManager.getCopyManager(conn).copyIn(copyQuery);
    		}
	        copyIn.writeToCopy(row, offset, length);
	        copyIn.writeToCopy(ROW_VALUES_DELIMITER, 0, ROW_VALUES_DELIMITER.length);
    	} catch (SQLException se) {
    		throw new IOException(se.getMessage());
    	}
    }

    public void commit() throws SQLException {
        conn.createStatement().executeUpdate("COMMIT");
    }

    public void rollback() throws SQLException {
        conn.createStatement().executeUpdate("ROLLBACK");
    }

    public void close() throws SQLException {
    	LoaderConnectionPool.getConnectionPool().releaseConnection(connectionInfo, conn);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Util.loader.INodeWriter#finish()
     */
    public synchronized void finish(boolean success) throws IOException {
    	if (copyIn != null) {
	    	try {
	    		rowCount += copyIn.endCopy();
		    	copyIn = null;
	    	} catch (SQLException se) {
		    	copyIn = null;
	    		logger.catching(se);
	            if (success) {
	                throw new IOException(writerID + ": " + se.getMessage());
	            }
	    	}
    	}
    }

    public String getStatistics() {
        return "";
    }

    /**
     * @return row count from writer
     */
    public long getRowCount() {
        /*
         * sent only successfully loaded rows count.
         */
        return rowCount;
    }
}
