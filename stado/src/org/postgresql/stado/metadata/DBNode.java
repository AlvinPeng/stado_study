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
/*
 *  
 */
package org.postgresql.stado.metadata;

import java.util.Enumeration;
import java.util.Properties;

import org.postgresql.stado.common.util.Property;


/**
 *  
 * 
 */
public class DBNode {

    private static final int POOL_SIZE = 5;

    private static final long POOL_TIMEOUT = 60000L;

    private Node aNode;

    private SysDatabase aDatabase;

    private boolean online = false;

    private int poolSize = POOL_SIZE;

    private long poolTimeout = POOL_TIMEOUT;

    /**
     * 
     */
    DBNode(Node aNode, SysDatabase aDatabase) {
        this.aNode = aNode;
        this.aDatabase = aDatabase;
        aNode.addDbNode(this);
        aDatabase.addDbNode(this);
    }

    void remove() {
        // Just to ensure this lock was acquired
        aNode.removeDBNode(aDatabase.getDbname());
        aDatabase.removeDBNode(aNode.getNodeid());
    }

    /**
     * @return the database
     */
    public SysDatabase getDatabase() {
        return aDatabase;
    }

    /**
     * @return the node
     */
    public Node getNode() {
        return aNode;
    }

    /**
     * @return is the database online (client can connect to it)
     */
    public boolean isOnline() {
        synchronized (MetaData.getMetaData().getStartupLock()) {
            return online;
        }
    }

    /**
     * 
     */
    public void setOnline() {
        Object startupLock = MetaData.getMetaData().getStartupLock();
        synchronized (startupLock) {
            online = true;
            startupLock.notifyAll();
        }
    }

    /**
     * 
     */
    public void setOffline() {
        Object startupLock = MetaData.getMetaData().getStartupLock();
        synchronized (startupLock) {
            online = false;
            startupLock.notifyAll();
        }
    }

    /**
     * 
     * @return the JDBC URI
     */
    public String getJdbcString() {
        return aNode.getJdbcString(aDatabase.getDbname());
    }

    /**
     * @return the max size of the connection pool 
     */
    public int getPoolSize() {
        return poolSize;
    }

    /**
     * @return the timeout
     */
    public long getPoolTimeout() {
        return poolTimeout;
    }

    /**
     * @return nodeId the node ID
     */
    public int getNodeId() {
        return aNode.getNodeid();
    }

    @Override
    public String toString() {
        return "{" + aDatabase + "," + aNode + "}";
    }

    private NodeDBConnectionInfo connectionInfo;

    public NodeDBConnectionInfo getNodeDBConnectionInfo() {
        if (connectionInfo == null) {
            Properties props = new Properties();
            String defaultPrefix = "xdb.default.custom.";
            String nodePrefix = "xdb.node." + aNode.getNodeid() + ".custom.";
            Enumeration propertyNames = Property.getProperties()
                    .propertyNames();
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
            connectionInfo = new NodeDBConnectionInfo(aNode.getNodeid(), aNode
                    .getSHost(), aNode.getPort(), aNode
                    .getNodeDatabaseString(aDatabase.getDbname()), aNode
                    .getJdbcUser(), aNode.getJdbcPassword(), props);
        }
        return connectionInfo;
    }
}
