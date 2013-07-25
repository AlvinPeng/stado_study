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

import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;

import org.postgresql.stado.common.util.ParseCmdLine;
import org.postgresql.stado.common.util.Property;


//-----------------------------------------------------------
public class Node {
    private int nodeid;

    private String jdbcDriver = "";

    private String jdbcUrl = "";

    private String host = "";

    private int port = 0;

    private String jdbcUser = "";

    private String jdbcPassword = "";

    private boolean up = false;

    private Map<String,DBNode> dbNodeList = new HashMap<String,DBNode>();

    // ------------------------------------------------------------------------
    Node(int NodeNumber, String jdbcDriver, String jdbcUrl, String host,
            int port, String jdbcUser, String jdbcPassword) {
        nodeid = NodeNumber;
        this.jdbcDriver = jdbcDriver;
        this.jdbcUrl = jdbcUrl;
        this.jdbcUser = jdbcUser;
        this.jdbcPassword = jdbcPassword;
        this.host = host;
        this.port = port;
    }

    // ------------------------------------------------------------------------
    public String getJdbcString(String databaseName) {
        HashMap<String,String> valueMap = new HashMap<String,String>();

        valueMap.put("dbhost", host);
        valueMap.put("dbport", "" + port);
        valueMap.put("database", getNodeDatabaseString(databaseName));

        return ParseCmdLine.substitute(jdbcUrl, valueMap);
    }

    // This is for handling simulating multiple nodes on one node
    // ------------------------------------------------------------------------
    public String getNodeDatabaseString(String databaseName) {
        return "__" + databaseName + "__N" + nodeid;
    }

    /**
     * @return is database up
     */
    public boolean isUp() {
        synchronized (MetaData.getMetaData().getStartupLock()) {
            return up;
        }
    }

    /**
     * @return a list of DBNodes to connect to
     */
    Collection<DBNode> setUp() {
        LinkedList<DBNode> dbNodesToConnect = new LinkedList<DBNode>();
        Object startupLock = MetaData.getMetaData().getStartupLock();
        synchronized (startupLock) {
            up = true;
            for (DBNode dbNode : getDBNodeList()) {
                if (dbNode.getDatabase().isStarted()) {
                    dbNode.setOffline();
                    dbNodesToConnect.add(dbNode);
                }
            }
            startupLock.notifyAll();
        }
        return dbNodesToConnect;
    }

    synchronized void addDbNode(DBNode dbNode) {
        dbNodeList.put(dbNode.getDatabase().getDbname(), dbNode);
    }

    public synchronized Collection<DBNode> getDBNodeList() {
        return dbNodeList.values();
    }

    /**
     * @param database
     */
    public synchronized DBNode getDBNode(String database) {
        return (DBNode) dbNodeList.get(database);
    }

    /**
     * @param dbname
     */
    synchronized void removeDBNode(String dbname) {
        dbNodeList.remove(dbname);
    }

    /**
     * @return Returns the nodeid.
     */
    public int getNodeid() {
        return nodeid;
    }

    /**
     * @return Returns the jdbcDriver.
     */
    public String getSJdbcDriver() {
        return jdbcDriver;
    }

    /**
     * @return Returns the jdbcUser.
     */
    public String getJdbcUser() {
        return jdbcUser;
    }

    /**
     * @return Returns the jdbcPassword.
     */
    public String getJdbcPassword() {
        return jdbcPassword;
    }

    /**
     * @return Returns the host.
     */
    public String getSHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    /**
     * 
     */
    void setDown() {
        Object startupLock = MetaData.getMetaData().getStartupLock();
        synchronized (startupLock) {
            up = false;
            for (Iterator<DBNode> it = getDBNodeList().iterator(); it.hasNext();) {
                it.next().setOffline();
            }
            startupLock.notifyAll();
        }
    }

    @Override
    public String toString() {
        return "Nd:" + nodeid;
    }

    /**
     * @param dbName
     * @return the connection info for specified databases
     * @see org.postgresql.stado.metadata.NodeDBConnectionInfo
     */
    public NodeDBConnectionInfo getNodeDBConnectionInfo(String dbName) {
        DBNode dbNode = getDBNode(dbName);
        if (dbNode != null) {
            return dbNode.getNodeDBConnectionInfo();
        }
        Properties props = new Properties();
        String defaultPrefix = "xdb.default.custom.";
        String nodePrefix = "xdb.node." + nodeid + ".custom.";
        Enumeration propertyNames = Property.getProperties().propertyNames();
        while (propertyNames.hasMoreElements()) {
            String name = (String) propertyNames.nextElement();
            if (name.startsWith(nodePrefix)) {
                String key = name.substring(nodePrefix.length());
                String value = Property.get(name);
                props.setProperty(key, value);
            } else if (name.startsWith(defaultPrefix)) {
                String key = name.substring(nodePrefix.length());
                String value = Property.get(name);
                if (!props.containsKey(key)) {
                    props.setProperty(key, value);
                }
            }
        }
        return new NodeDBConnectionInfo(nodeid, host, port,
                getNodeDatabaseString(dbName), jdbcUser, jdbcPassword, props);
    }
}
