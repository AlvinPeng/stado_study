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
package org.postgresql.stado.communication;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Level;
import org.postgresql.stado.common.util.Property;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.communication.message.NodeMessage;
import org.postgresql.stado.exception.XDBServerException;


/**
 * Factory class to create Connectors
 * 
 * @see org.postgresql.stado.communication.AbstractConnector
 * 
 *  
 * @version 1.0
 */
public class ConnectorFactory {

    private static final XLogger logger = XLogger
            .getLogger(ConnectorFactory.class);

    /**
     * Constant to mark Connector undefined
     */
    public static final int CONNECTOR_TYPE_UNKNOWN = -1;

    /**
     * Constant to define local (in-process) Connector
     * 
     * @see org.postgresql.stado.communication.LocalConnector
     */
    public static final int CONNECTOR_TYPE_LOCAL = 0;

    /**
     * Constant to define TCP Socket Connector Removed, channels are used
     * instead
     */
    public static final int CONNECTOR_TYPE_SOCKET = 1;

    /**
     * Constant to define TCP Socket Connector using J2SE 1.4 channels
     */
    public static final int CONNECTOR_TYPE_CHANNEL = 2;

    /**
     * Constant to define UDP Socket (broadcast) Connector
     */
    public static final int CONNECTOR_TYPE_BROADCAST = 3;

    /**
     * msgQTable is used for simple communications between in-process (running
     * within the same Java VM) Nodes. Key of the Hashtable is Node's number,
     * value is a message queue (actually is java.util.LinkedList). Entries are
     * created by the org.postgresql.stado..communication.LocalConnector instances, 
     * once they are initialized.
     */
    private static final ConcurrentHashMap<Integer, BlockingQueue<NodeMessage>> msgQTable = new ConcurrentHashMap<Integer, BlockingQueue<NodeMessage>>();

    private static HashMap<Integer, AbstractConnector> localConnectors = new HashMap<Integer, AbstractConnector>();

    private static HashMap<Integer, AbstractConnector> socketConnectors = new HashMap<Integer, AbstractConnector>();

    private static HashMap<Integer, AbstractConnector> broadcastConnectors = new HashMap<Integer, AbstractConnector>();

    /**
     * Look at the configuration and determine connector type needed to send a
     * message from one node to another.
     * 
     * @param src
     *            number of node to send message from
     * @param dst
     *            number of node to send message to
     * @return CONNECTOR_TYPE_UNKNOWN - failed to determine connector
     *         CONNECTOR_TYPE_LOCAL - local connector (shared array)
     *         CONNECTOR_TYPE_SOCKET - the same as CONNECTOR_TYPE_CHANNEL
     *         CONNECTOR_TYPE_CHANNEL - TCP/IP connector (nio channel)
     *         CONNECTOR_TYPE_BROADCAST - UDP/IP connector
     */
    public static final int getConnectorType(int src, int dst) {
        String connectorTypeParam = "xdb.connector." + src + "." + dst;

        // First look for explicit connector type
        int connectorType = Property.getInt(connectorTypeParam,
                                            CONNECTOR_TYPE_UNKNOWN);

        if (connectorType == CONNECTOR_TYPE_UNKNOWN) {
            try {
                InetAddress sourceAddr = InetAddress.getByName(Property.get(
                        "xdb." + ((src == 0) ? "coordinator"
                                : "node." + src) + ".host", "localhost"));
                InetAddress targetAddr = InetAddress.getByName(Property.get(
                        "xdb." + ((dst == 0) ? "coordinator"
                                : "node." + dst) + ".host", "localhost"));
    
                if (sourceAddr.equals(targetAddr)) {
                    connectorType = CONNECTOR_TYPE_LOCAL;
                } else {
                    connectorType = CONNECTOR_TYPE_CHANNEL;
                }
            } catch (UnknownHostException uhe) {
                // ???
                connectorType = CONNECTOR_TYPE_BROADCAST;
            }
        }
        return connectorType;
    }

    /**
     * Factory method to construct Connector between specified Nodes
     * 
     * @param sourceNodeNum
     * @param targetNodeNum
     * @throws org.postgresql.stado.exception.XDBServerException
     * @return
     */
    public static final AbstractConnector getConnector(int sourceNodeNum,
            int targetNodeNum) throws XDBServerException {
        switch (getConnectorType(sourceNodeNum, targetNodeNum)) {
        case CONNECTOR_TYPE_LOCAL:
            logger.log(Level.DEBUG,
                    "Requested local Connector from %0% to %1%", new Object[] {
                            new Integer(sourceNodeNum),
                            new Integer(targetNodeNum) });

            // Source node already exist, no need to create it
            // and create source connector before target node to
            // avoid recursion when target node will create connector back
            AbstractConnector sourceConnector = getLocalConnector(sourceNodeNum);

            if (msgQTable.get(targetNodeNum) == null) {
                // Method creates node if it doesn't exist yet
                if (targetNodeNum == 0) {
                    XDBServerException ex = new XDBServerException(
                            "Could not find Coordinator's LocalConnector - configuration inconsistent");
                    logger.throwing(ex);
                    throw ex;
                }
                    NodeAgent.getNodeAgent(targetNodeNum).getConnector(
                            sourceNodeNum);
            }

            return sourceConnector;

        case CONNECTOR_TYPE_SOCKET:
        case CONNECTOR_TYPE_CHANNEL:
            synchronized (socketConnectors) {
                Integer src = new Integer(sourceNodeNum);
                AbstractConnector connector = socketConnectors.get(src);
                if (connector == null) {
                    try {
                        Class socketClass = Class
                                .forName("org.postgresql.stado.communication.SocketConnector");
                        connector = (AbstractConnector) socketClass
                                .getConstructor(new Class[] { int.class })
                                .newInstance(new Object[] { src });
                        socketConnectors.put(src, connector);
                    } catch (Throwable t) {
                        return getLocalConnector(sourceNodeNum);
                    }
                }
                return connector;
            }

        case CONNECTOR_TYPE_BROADCAST:
            return getBroadcastConnector(sourceNodeNum);
        default:
            XDBServerException ex = new XDBServerException(
                    "Requested connector type does not supported");
            logger.throwing(ex);
            throw ex;
        }
    }

    /**
     * Returns connector to local node
     * 
     * @param sourceNodeNum
     * @return
     */
    private static AbstractConnector getLocalConnector(int sourceNodeNum) {
        synchronized (localConnectors) {
            AbstractConnector connector = localConnectors.get(sourceNodeNum);

            if (connector == null) {
                connector = new LocalConnector(sourceNodeNum, msgQTable);
                localConnectors.put(sourceNodeNum, connector);
            }

            return connector;
        }
    }

    /**
     * Returns broadcast connector
     * 
     * @param sourceNodeNum
     * @return a broadcast connector
     */
    public static AbstractConnector getBroadcastConnector(int sourceNodeNum) {
        synchronized (broadcastConnectors) {
            AbstractConnector connector = broadcastConnectors
                    .get(sourceNodeNum);

            if (connector == null) {
                String connectorClassName = Property
                        .get("xdb.broadcast.connector");
                if (connectorClassName == null) {
                    return null;
                } else {
                    try {
                        Class broadcastClass = Class
                                .forName(connectorClassName);
                        connector = (AbstractConnector) broadcastClass
                                .getConstructor(new Class[] { int.class })
                                .newInstance(
                                        new Object[] { new Integer(
                                                sourceNodeNum) });
                        broadcastConnectors.put(new Integer(sourceNodeNum),
                                connector);
                    } catch (Throwable t) {
                        logger.catching(t);
                        return null;
                    }
                }
            }

            return connector;
        }
    }
}