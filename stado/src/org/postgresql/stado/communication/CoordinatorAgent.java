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

import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Properties;

import org.apache.log4j.Level;
import org.postgresql.stado.common.util.Property;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.communication.message.NodeMessage;
import org.postgresql.stado.engine.CoordinatorPools;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.MetaData;
import org.postgresql.stado.metadata.Node;
import org.postgresql.stado.metadata.SysAgent;
import org.postgresql.stado.metadata.SysDatabase;


/**
 * CoordinatorAgent is used by the main process to connect to Nodes
 * of the system. It contains an array of the connection objects
 * Main purpose of the CoordinatorAgent is to conduct NodeMessages between
 * processes running inside Coordinator and other Nodes of the system.
 *
 * @see org.postgresql.stado.communication.AbstractConnector
 * @see org.postgresql.stado.communication.message.NodeMessage
 *
 *  
 * @version 1.0
 */
public class CoordinatorAgent extends AbstractAgent {

    private static final XLogger logger = XLogger
            .getLogger(CoordinatorAgent.class);

    private static final XLogger serverLogger = XLogger.getLogger("Server");

    /**
     * The instance of the CoordinatorAgent. We are using single-instance model.
     */
    private static CoordinatorAgent agent = null;

    /**
     * The array of connections to the Nodes. Every AbstractConnector holds a
     * channel to some Node. There is exactly one connector to every Node and it
     * is reused by all acting components of the central instance. Other side of
     * the channel is served by the NodeAgent
     *
     * @see org.postgresql.stado.communication.NodeAgent.
     */
    private AbstractConnector[] nodeConnectors;

    private AbstractConnector broadcastConnector = null;

    /**
     * Constructor
     */
    private CoordinatorAgent() {
        // Force loading SysAgents and validity check
        SysAgent.getAgent(0);
    }

    /**
     * Coordinator always have nodeID 0
     *
     * @see org.postgresql.stado.communication.AbstractAgent#getNodeID()
     * @return
     */
    @Override
    public int getNodeID() {
        return 0;
    }

    /**
     * Initializes array of Connectors to Nodes. Adds itself as listener to
     * every Connector.
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    @Override
    protected synchronized void init() throws XDBServerException {
        final String method = "init";
        logger.entering(method);
        try {

            try {
                Collection nodeList = MetaData.getMetaData().getNodes();
                nodeConnectors = new AbstractConnector[nodeList.size()];
                for (Iterator it = nodeList.iterator(); it.hasNext();) {
                    Node aNode = (Node) it.next();
                    AbstractConnector connector = ConnectorFactory
                            .getConnector(0, aNode.getNodeid());
                    nodeConnectors[aNode.getNodeid() - 1] = connector;
                    connector.addMessageListener(this);
                    connector.start();
                }
                broadcastConnector = ConnectorFactory.getBroadcastConnector(0);
                if (broadcastConnector != null) {
                    broadcastConnector.addMessageListener(this);
                    broadcastConnector.start();
                }
            } catch (Exception e) {
                logger.catching(e);
                XDBServerException ex = new XDBServerException(
                        "Exception during Agent initialization", e);
                logger.throwing(ex);
                throw ex;
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Destroy Connectors and close the agent
     */
    @Override
    protected synchronized void close() {
        final String method = "close";
        logger.entering(method);
        try {

            if (broadcastConnector != null) {
                broadcastConnector.destroy();
                broadcastConnector.removeMessageListener(this);
            }
            for (AbstractConnector element : nodeConnectors) {
                element.destroy();
                element.removeMessageListener(this);
            }

            nodeConnectors = null;
            serverLogger.info("*** Coordinator agent is stopped");

        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @param target
     * @return
     */

    @Override
    public synchronized AbstractConnector getConnector(Integer target) {
        final String method = "getConnector";
        logger.entering(method, new Object[] { target });
        AbstractConnector connector = null;
        try {

            if (target == null) {
                try {
                    connector = broadcastConnector;
                } catch (Exception ex) {
                }
            } else {
                int idx = target.intValue();

                if (idx > 0 && idx <= nodeConnectors.length) {
                    connector = nodeConnectors[idx - 1];
                }
            }

            return connector;

        } finally {
            logger.exiting(method, connector);
        }
    }

    /**
     * If NodeMessage has type <code>MSG_NODE_UP</code> source node is marked
     * as "connected" in internal table and message with connection details is
     * sent back.
     *
     * @see org.postgresql.stado.communication.AbstractAgent#beforeProcessMessage(org.postgresql.stado.communication.message.NodeMessage)
     * @param message
     * @return
     */
    @Override
    public boolean beforeProcessMessage(NodeMessage message) {
        final String method = "beforeProcessMessage";
        logger.entering(method, new Object[] { message });
        try {

            switch (message.getMessageType()) {
            case NodeMessage.MSG_NODE_UP:
                try {
                    serverLogger.log(Level.INFO,
                            "Coordinator: Node %0% is connected",
                            new Object[] { new Integer(message
                                    .getSourceNodeID()) });
                    NodeMessage msg = reply(message,
                            NodeMessage.MSG_INIT_PROPERTIES);
                    Properties props = Property.getProperties();
                    Enumeration propNames = props.propertyNames();
                    while (propNames.hasMoreElements()) {
                        String name = (String) propNames.nextElement();
                        String value = props.getProperty(name);
                        if (!msg.canAddRows()) {
                            sendMessage(msg);
                            msg = reply(message,
                                    NodeMessage.MSG_INIT_PROPERTIES);
                        }
                        msg.addRowData(name + "=" + value);
                    }
                    sendMessage(msg);
                    sendMessage(reply(message, NodeMessage.MSG_NODE_UP_ACK));
                        for (DBNode dbNode : SysAgent.getAgent(
                                message.getSourceNodeID()).setConnected(true)) {
                            connectTo(dbNode);
                    }
                } catch (Exception ex) {
                    logger.catching(ex);
                }
                return true;
            case NodeMessage.MSG_INIT_FROM_SYS_ACK:
                try {
                    Node node = MetaData.getMetaData().getNode(
                            message.getSourceNodeID());
                    String dbName = message.getDatabase();
                    node.getDBNode(dbName).setOnline();
                    if (MetaData.getMetaData().getSysDatabase(dbName)
                            .isOnline()) {
                        serverLogger.log(Level.INFO,
                                "*** Database %0% is now online",
                                new Object[] { message.getDatabase() });
                    }
                } catch (Exception ex) {
                    logger.catching(ex);
                }
                return true;
            case NodeMessage.MSG_SHUTDOWN_FROM_SYS_ACK:
                try {
                    String dbName = message.getDatabase();
                    if (dbName == null) {
                            SysAgent.getAgent(message.getSourceNodeID())
                                    .setConnected(false);
                    } else {
                            Node node = MetaData.getMetaData().getNode(
                                    message.getSourceNodeID());
                        DBNode dbNode = node.getDBNode(dbName);
                        if (dbNode != null) {
                            dbNode.setOffline();
                        }
                        if (!MetaData.getMetaData().getSysDatabase(
                                message.getDatabase()).isOnline()) {
                            serverLogger.log(Level.INFO,
                                    "*** Database %0% is now offline",
                                    new Object[] { message.getDatabase() });
                        }
                    }
                } catch (Exception ex) {
                    logger.catching(ex);
                }
                return true;
            default:
                return super.beforeProcessMessage(message);
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Init database connection pools and load metadata
     * @param dbName the name of the database
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    public void initDatabase(String dbName) throws XDBServerException {
        SysDatabase sysdb = MetaData.getMetaData().getSysDatabase(dbName);
        CoordinatorPools.initPool(dbName, sysdb.getCoordinatorNodeID());
        connectToDatabase(sysdb);
    }

    /**

     * Init database connection pools on Nodes

     * @param database the name of the database

     * @throws org.postgresql.stado.exception.XDBServerException

     */
    public void connectToDatabase(SysDatabase database)
            throws XDBServerException {
        Iterator it = database.start().iterator();
        while (it.hasNext()) {
            connectTo((DBNode) it.next());
        }
    }

    /**
     * Close database connection pools on Nodes
     * @param database the name of the database
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    public void shutdownDatabase(SysDatabase database)
            throws XDBServerException {
        disconnectFrom(database.stop());
    }

    /**
     * Connection details for specified database are sent to specified Node
     *
     * @param dbNode
     */
    private void connectTo(DBNode dbNode) {
        final String method = "connectTo";
        logger.entering(method, new Object[] { dbNode });
        try {

            Node node = dbNode.getNode();
            SysDatabase database = dbNode.getDatabase();
            NodeMessage initMsg = NodeMessage
                    .getNodeMessage(NodeMessage.MSG_INIT_FROM_SYS);
            initMsg.setTargetNodeID(new Integer(node.getNodeid()));
            initMsg.setSourceNodeID(getNodeID());
            initMsg.setDatabase(database.getDbname());
            initMsg.setJdbcDriver(node.getSJdbcDriver());
            initMsg.setJdbcString(dbNode.getJdbcString());
            initMsg.setJdbcUser(node.getJdbcUser());
            initMsg.setJdbcPassword(node.getJdbcPassword());
            initMsg.setMaxConns(dbNode.getPoolSize());
            initMsg.setTimeOut(dbNode.getPoolTimeout());
            sendMessage(initMsg);

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Close database connection pools on specified Nodes
     * @param nodeList
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    public void disconnectFrom(Collection<DBNode> nodeList) throws XDBServerException {
        for (DBNode dbNode : nodeList) {
            disconnectFrom(dbNode);
        }
    }

    /**
     * Close JDBC pool on the node
     *
     * @param dbNode
     */
    private void disconnectFrom(DBNode dbNode) {
        final String method = "disconnectFrom";
        logger.entering(method, new Object[] { dbNode });
        try {

            Node node = dbNode.getNode();
            SysDatabase database = dbNode.getDatabase();
            NodeMessage shutdownMsg = NodeMessage
                    .getNodeMessage(NodeMessage.MSG_SHUTDOWN_FROM_SYS);
            shutdownMsg.setTargetNodeID(new Integer(node.getNodeid()));
            shutdownMsg.setSourceNodeID(getNodeID());
            shutdownMsg.setDatabase(database.getDbname());
            sendMessage(shutdownMsg);

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Restart or shutdown Nodes
     * @param restart if <code>TRUE</code> do restart if <code>FALSE</code> do
     * shutdown
     */
    public void resetNodes(boolean restart) {
        final String method = "resetNodes";
        logger.entering(method, new Object[] { restart ? Boolean.TRUE
                : Boolean.FALSE });
        try {

            for (Node node : MetaData.getMetaData().getNodes()) {
            Integer target = node.getNodeid();
            if (!restart
                || !(getConnector(target) instanceof LocalConnector)) {
            NodeMessage reset = NodeMessage
                    .getNodeMessage(NodeMessage.MSG_SHUTDOWN_FROM_SYS);
            reset.setTargetNodeID(target);
            reset.setSourceNodeID(getNodeID());
            reset.setSqlCommand(restart ? "RESTART" : "SHUTDOWN");
            sendMessage(reset);
            }
         }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Returns instance of the CoordinatorAgent. Creates new if it doesn't
     * exist. NOTE: Is not thread-safe
     * @return
     */
    public static final CoordinatorAgent getInstance() {
        if (agent == null) {
            agent = new CoordinatorAgent();
            try {
                agent.init();
            } catch (XDBServerException e) {
                logger.catching(e);
                agent = null;
                throw e;
            }
        }

        return agent;
    }

    /**
     * NOTE: Is not thread-safe
     */
    public static final void releaseInstance() {
        if (agent != null) {
            agent.close();
            agent = null;
        }
    }
}
