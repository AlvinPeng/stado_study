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

import java.sql.Connection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.postgresql.stado.common.util.ParseCmdLine;
import org.postgresql.stado.common.util.Property;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.engine.JDBCPool;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.NodeDBConnectionInfo;


/**
 * Class to manage connection pools used by the Loader, both to send 
 * intermediate results and for internal COPY command
 * This is the single instance class
 * @author amart
 */
public class LoaderConnectionPool {
	private static LoaderConnectionPool instance = null;
	
    public static final LoaderConnectionPool getConnectionPool() {
    	if (instance == null) {
    		instance = new LoaderConnectionPool();
    	}
    	return instance;
    }

    /**
     * Loader sends data directly to node databases, and we want to pool connections.
     * We need a pool of connections to each database on each node.
     * Keys are database name and node number
     */
    private Map<String, JDBCPool> loaderConnections = null;
    
    /**
     * Get specified connection from the pool or create new one, if does not exists 
     * @param connInfo
     * 				connection specification (DB name, node, etc.)
     * @return
     * 				connection to specified node database
     */
    public synchronized Connection getConnection(NodeDBConnectionInfo connInfo) {
    	if (loaderConnections == null) {
    		loaderConnections = new HashMap<String, JDBCPool>();
    	}
    	
    	JDBCPool pool = loaderConnections.get(connInfo.getDbName());
		if (pool == null) {
			String basePropKey = "xdb.node." + connInfo.getNodeID() + ".";
            String aJdbcString = Property.get(basePropKey + "jdbcstring",
                    Props.XDB_DEFAULT_JDBCSTRING);
            String aJdbcDriver = Property.get(basePropKey + "jdbcdriver",
                    Props.XDB_DEFAULT_JDBCDRIVER);
            Map<String,String> m = new HashMap<String, String>();
            m.put("dbhost", connInfo.getDbHost());
            if (connInfo.getDbPort() > 0) {
                m.put("dbport", "" + connInfo.getDbPort());
            }
            m.put("database", connInfo.getDbName());
            m.put("dbusername", connInfo.getDbUser());
            m.put("dbpassword", connInfo.getDbPassword());
			pool = new JDBCPool(aJdbcDriver, 
					ParseCmdLine.substitute(aJdbcString, m),
					m.get("dbusername"), 
					m.get("dbpassword"),
					1, Integer.MAX_VALUE);
            pool.setGetTimeout(Property.getInt(
                    "xdb.jdbc.pool.timeout",
                    Props.XDB_DEFAULT_THREADS_POOL_TIMEOUT));
            pool.setMaxLifetime(Property.getInt(
                    "xdb.jdbc.pool.max_lifetime",
                    Props.XDB_DEFAULT_THREADS_POOL_MAX_LIFETIME));
            pool.setReleaseTimeout(Property.getInt(
                    "xdb.jdbc.pool.idle",
                    Props.XDB_DEFAULT_THREADS_POOL_IDLE));
			loaderConnections.put(connInfo.getDbName(), pool);
		}
		
		return pool.getConnection();
    }
    
    /**
     * Release loader connection back to pool 
     * @param connInfo
     * 				connection specification (DB name, node)
     * @param conn
     * 				the connection
     * @throws XDBServerException
     * 				if error occurs
     */
    public synchronized void releaseConnection(NodeDBConnectionInfo connInfo, Connection conn) throws XDBServerException {
    	JDBCPool pool = loaderConnections.get(connInfo.getDbName());
    	pool.releaseConnection(conn);
    }
    
    /**
     * Invoked when the database being stopped
     * Remove associated pool and close connections
     * @param dbName
     */
    public synchronized void removeDatabase(String dbName) {
    	if (loaderConnections == null) {
    		return;
    	}
    	for (Iterator<Map.Entry<String, JDBCPool>> it = loaderConnections.entrySet().iterator(); it.hasNext();) {
    		Map.Entry<String, JDBCPool> entry = it.next();
    		if (entry.getKey().startsWith(dbName) && entry.getKey().lastIndexOf("N") == dbName.length()) {
    			entry.getValue().destroy();
    			it.remove();
    		}
    	}
    }
}
