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

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Level;
import org.postgresql.stado.common.util.Property;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.communication.message.NodeMessage;
import org.postgresql.stado.engine.JDBCPool;
import org.postgresql.stado.engine.NodeThread;
import org.postgresql.stado.engine.NodeThreadPool;
import org.postgresql.stado.engine.loader.LoaderConnectionPool;
import org.postgresql.stado.exception.XDBBaseException;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.exception.XDBUnexpectedMessageException;
import org.postgresql.stado.exception.XDBWrappedException;
import org.postgresql.stado.parser.handler.IdentifierHandler;


/**
 * NodeAgent is interface between specific node and all the system. It contains
 * permanent Connector objects to CoordinatorAgent
 * Main purpose of the NodeAgent is to conduct NodeMessages between NodeThreads 
 * and other parts of the system.
 * 
 * @see org.postgresql.stado.communication.AbstractConnector
 * @see org.postgresql.stado.communication.CoordinatorAgent
 * @see org.postgresql.stado.communication.message.NodeMessage
 * 
 *  
 * @version 1.0
 */
public class NodeAgent extends AbstractAgent {

    private static final XLogger logger = XLogger.getLogger(NodeAgent.class);

    private static final XLogger serverLogger = XLogger.getLogger("Server");

    /**
     * The instances of the NodeAgent, which are running within the Java VM.
     */
    private static final HashMap<Integer, NodeAgent> localNodeAgents = new HashMap<Integer, NodeAgent>();

    private int nodeID;

    private HashMap<String, JDBCPool> connectionPools = new HashMap<String, JDBCPool>();

    private AtomicReference<NodeThreadPool> threadPool = new AtomicReference<NodeThreadPool>(
            null);

    /**
     * Connector to the CoordinatorAgent.
     * 
     * @see org.postgresql.stado.communication.CoordinatorAgent
     */
    private AbstractConnector[] connectors;

    private AbstractConnector broadcastConnector;

    private ProcessAnalizer analizer = new ProcessAnalizer();

    private Executor executor;

    /**
     * Constructor
     * @param nodeID 
     */
    private NodeAgent(int nodeID) {
        this.nodeID = nodeID;
        executor = Executors.newCachedThreadPool();
    }

    /**
     * 
     * @see org.postgresql.stado.communication.AbstractAgent#getNodeID()
     * @return 
     */
    @Override
    public int getNodeID() {
        return nodeID;
    }

    /**
     * Initializes Connector to CoordinatorAgent. Adds itself as listener to it.
     */
    @Override
    protected void init() {
        final String method = "init";
        logger.entering(method);
        try {

            connectors = new AbstractConnector[1];
            connectors[0] = ConnectorFactory.getConnector(nodeID, 0);
            connectors[0].addMessageListener(this);
            connectors[0].start();
            NodeMessage message = NodeMessage
                    .getNodeMessage(NodeMessage.MSG_NODE_UP);
            message.setSourceNodeID(nodeID);
            message.setTargetNodeID(new Integer(0));
            sendMessage(message);

        } catch (XDBServerException ex) {
            logger.catching(ex);
            // FATAL: can not initialize Agent properly, perhaps invalid config
            // or duplicate instance
            System.exit(-1);
        } finally {
            logger.exiting(method);
        }
    }

    private void finishInit() {
        final String method = "finishInit";
        logger.entering(method);
        try {

            AbstractConnector connector = connectors[0];
            connectors = new AbstractConnector[Props.XDB_NODECOUNT + 1];
            connectors[0] = connector;

            broadcastConnector = ConnectorFactory
                    .getBroadcastConnector(getNodeID());
            if (broadcastConnector != null) {
                broadcastConnector.addMessageListener(this);
                broadcastConnector.start();
            }
            NodeThreadPool aThreadPool = new NodeThreadPool(nodeID, Property
                    .getInt("xdb.node." + nodeID + ".threads.pool.initsize",
                            Props.XDB_DEFAULT_THREADS_POOL_INITSIZE), Property
                    .getInt("xdb.node." + nodeID + ".threads.pool.maxsize",
                            Props.XDB_DEFAULT_THREADS_POOL_MAXSIZE));
            aThreadPool.setGetTimeout(Property.getInt("xdb.node." + nodeID
                    + ".threads.pool.timeout",
                    Props.XDB_DEFAULT_THREADS_POOL_TIMEOUT));
            aThreadPool.setMaxLifetime(Property 
                    .getInt("xdb.node." + nodeID + ".threads.pool.max_lifetime",
                            Props.XDB_DEFAULT_THREADS_POOL_MAX_LIFETIME));
            aThreadPool.setReleaseTimeout(Property
                    .getInt("xdb.node." + nodeID + ".threads.pool.idle",
                            Props.XDB_DEFAULT_THREADS_POOL_IDLE));
            aThreadPool.initBuffer();
            aThreadPool.setCleanupAgent(analizer);
            serverLogger.log(Level.INFO, "Node %0%: thread pool is ready",
                    new Object[] { new Integer(nodeID) });
            // Some kind of swap: get previous value to aThreadPool
            aThreadPool = threadPool.getAndSet(aThreadPool);
            if (aThreadPool != null) {
                logger.warn("Node Thread Pool was initialized twice on node "
                        + nodeID);
                aThreadPool.destroy();
            }

        } catch (XDBServerException ex) {
            logger.catching(ex);
            serverLogger
                    .fatal("Can not initialize Agent properly, perhaps invalid config or duplicate instance");
            System.exit(-1);
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Destroys Connectors.
     */
    @Override
    protected void close() {
        NodeThreadPool aThreadPool = threadPool.getAndSet(null);
        if (aThreadPool != null) {
            aThreadPool.destroy();
        }
        synchronized (connectionPools) {
            Iterator<JDBCPool> it = connectionPools.values().iterator();
            while (it.hasNext()) {
                JDBCPool pool = it.next();
                if (pool != null) {
                    pool.destroy();
                }
            }
            connectionPools.clear();
        }
        if (connectors != null) {
            for (int i = 0; i < connectors.length; i++) {
                if (connectors[i] != null) {
                    connectors[i].destroy();
                    connectors[i].removeMessageListener(this);
                    connectors[i] = null;
                }
            }
            connectors = null;
        }
        if (broadcastConnector != null) {
            broadcastConnector.destroy();
            broadcastConnector.removeMessageListener(this);
            broadcastConnector = null;
        }
        serverLogger.log(Level.INFO, "*** Node agent %0% is stopped",
                new Object[] { new Integer(nodeID) });
    }

    /**
     * 
     * @see org.postgresql.stado.communication.AbstractAgent#getConnector(java.lang.Integer)
     * @param target 
     * @return 
     */
    @Override
    public AbstractConnector getConnector(Integer target) {
        final String method = "getConnector";
        logger.entering(method, new Object[] { target });
        AbstractConnector connector = null;
        try {

            if (target == null) {
                connector = broadcastConnector;
            } else {
                if (connectors == null) {
                    throw new XDBServerException("Node Agent " + nodeID
                            + " is not initialized");
                }
                if (target.intValue() >= connectors.length) {
                    throw new XDBServerException("Invalid Node Number: "
                            + target);
                }
                connector = connectors[target.intValue()];
                if (connector == null) {
                    connector = ConnectorFactory.getConnector(nodeID, target);
                    if (connector != null) {
                        connector.addMessageListener(this);
                        connector.start();
                    }
                    connectors[target.intValue()] = connector;
                }
            }
            return connector;

        } finally {
            logger.exiting(method, connector);
        }
    }

    /**
     * This method consumes following message types: MSG_INIT_FROM_SYS - Message
     * carrying DB connection details. Connection pool is initialized.
     * MSG_CONNECTION_BEGIN - starts new session and NodeThread is got from pool
     * and bound to it. MSG_CONNECTION_END - closes the session and returns
     * connection to pool
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
            case NodeMessage.MSG_INIT_PROPERTIES:
                Property.addLines(message.getRowData());
                return true;
            case NodeMessage.MSG_NODE_UP_ACK:
                finishInit();
                serverLogger.log(Level.INFO,
                        "Node %0%: Connection to Coordinator is completed",
                        new Object[] { new Integer(nodeID) });
                return true;
            case NodeMessage.MSG_INIT_FROM_SYS:
                executor.execute(new PoolInitializer(message));
                return true;
            case NodeMessage.MSG_SHUTDOWN_FROM_SYS:
                try {
                    NodeMessage msg;
                    if (message.getDatabase() == null) {
                        // Reset message, shutdown all connection pools
                        synchronized (connectionPools) {
                            Iterator<JDBCPool> it = connectionPools.values()
                                    .iterator();
                            while (it.hasNext()) {
                                JDBCPool pool = it.next();
                                if (pool != null) {
                                    pool.destroy();
                                }
                            }
                            connectionPools.clear();
                        }
                        // TODO clear processes as well
                        if ("RESTART".equals(message.getSqlCommand())) {
                            sendMessage(reply(message, NodeMessage.MSG_NODE_UP));
                        } else {
                            sendMessage(reply(message,
                                    NodeMessage.MSG_SHUTDOWN_FROM_SYS_ACK));
                            NodeAgent.releaseNodeAgent(nodeID);
                        }
                    } else {
                        synchronized (connectionPools) {
                            JDBCPool pool = connectionPools.remove(message
                                    .getDatabase());
                            if (pool != null) {
                                pool.destroy();
                            }
                        }
                        LoaderConnectionPool.getConnectionPool().removeDatabase(message.getDatabase());
                        msg = reply(message,
                                NodeMessage.MSG_SHUTDOWN_FROM_SYS_ACK);
                        msg.setDatabase(message.getDatabase());
                        sendMessage(msg);
                    }
                } catch (Exception ex) {
                    sendMessage(reply(message, NodeMessage.MSG_ABORT));
                }
                return true;
            case NodeMessage.MSG_CONNECTION_BEGIN:
                executor.execute(new ThreadInitializer(message));
                return true;

            case NodeMessage.MSG_CONNECTION_END:
                // NodeThread.reset() can take long so put this to separate
                // thread
                executor.execute(new ThreadFinalizer(message));
                return true;

            case NodeMessage.MSG_START_LOADERS:
                String dbHost = message.getJdbcString();
                String database = message.getDatabase();
                String dbUser = message.getJdbcUser();
                String dbPassword = message.getJdbcPassword();
                String tableName = message.getTargetTable();
                String address = message.getSqlCommand();
                try {
                    Class loaderClass = Class
                            .forName("org.postgresql.stado.Util.loader.SocketWriter");
                    Constructor loaderConstructor = loaderClass
                            .getConstructor(new Class[] { int.class,
                                    String.class, String.class, String.class,
                                    String.class, String.class, String.class });
                    loaderConstructor.newInstance(new Object[] {
                            new Integer(nodeID), dbHost, database, dbUser,
                            dbPassword, tableName, address });
                } catch (Throwable t) {
                    logger.catching(t);
                }
                sendMessage(reply(message, NodeMessage.MSG_START_LOADERS_ACK));

                return true;

            default:
                return super.beforeProcessMessage(message);
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Returns instance of the NodeAgent with specified number. Creates new if
     * it doesn't exist.
     * @param nodeID 
     * @return 
     */
    public static NodeAgent getNodeAgent(int nodeID) {
        Integer key = new Integer(nodeID);
        NodeAgent nodeAgent = null;
        synchronized (localNodeAgents) {
            nodeAgent = localNodeAgents.get(key);

            if (nodeAgent == null) {
                nodeAgent = new NodeAgent(nodeID);
                localNodeAgents.put(key, nodeAgent);
                nodeAgent.init();
            }
        }

        return nodeAgent;
    }

    /**
     * Releases instance of the NodeAgent with specified number.
     * @param nodeID 
     */
    public static void releaseNodeAgent(int nodeID) {
        Integer key = new Integer(nodeID);
        synchronized (localNodeAgents) {
            NodeAgent nodeAgent = localNodeAgents.remove(key);

            if (nodeAgent != null) {
                nodeAgent.close();
            }

            if (localNodeAgents.isEmpty()) {
                System.exit(0);
            }
        }
    }

    /**
     * Asynchronous initializer of connection pool
     * 
     *  
     */
    private class PoolInitializer implements Runnable {
        private NodeMessage initMessage;

        /**
         * 
         * @param initMessage 
         */
        public PoolInitializer(NodeMessage initMessage) {
            this.initMessage = initMessage;
        }

        public void run() {
            final String method = "run";
            logger.entering(method);

            int attempts = Property.getInt("xdb.jdbc.pool.retry", -1);
            if (attempts < 0) {
                attempts = 0;
            }
            boolean logProblem = true;
            final String dbName = initMessage.getDatabase();
            while (attempts-- >= 0) {
                try {
                    JDBCPool pool = null;
                    synchronized (connectionPools) {
                        pool = connectionPools.get(dbName);
                        if (pool == null) {
                            pool = new JDBCPool(
                                    initMessage.getJdbcDriver(),
                                    initMessage.getJdbcString(),
                                    initMessage.getJdbcUser(),
                                    initMessage.getJdbcPassword(),
                                    Property
                                            .getInt(
                                                    "xdb.jdbc.pool.initsize",
                                                    Props.XDB_DEFAULT_THREADS_POOL_INITSIZE),
                                    Property
                                            .getInt(
                                                    "xdb.jdbc.pool.maxsize",
                                                    Props.XDB_DEFAULT_THREADS_POOL_MAXSIZE));
                            pool.setGetTimeout(Property.getInt(
                                    "xdb.jdbc.pool.timeout",
                                    Props.XDB_DEFAULT_THREADS_POOL_TIMEOUT));
                            pool.setMaxLifetime(Property.getInt(
                                    "xdb.jdbc.pool.max_lifetime",
                                    Props.XDB_DEFAULT_THREADS_POOL_MAX_LIFETIME));
                            pool.setReleaseTimeout(Property.getInt(
                                    "xdb.jdbc.pool.idle",
                                    Props.XDB_DEFAULT_THREADS_POOL_IDLE));
                            pool.setCleanupAgent(analizer);
                            connectionPools
                                    .put(initMessage.getDatabase(), pool);
                        }
                        pool.initBuffer();
                    }

                    Connection oConn = pool.getConnection();
                    try {
                        ResultSet rs;
                        if (Props.XDB_TEMP_TABLE_SELECT == null
                                || Props.XDB_TEMP_TABLE_SELECT.trim().length() == 0) {
                            rs = oConn.getMetaData().getTables(null, null,
                                    Props.XDB_TEMPTABLEPREFIX + "%", null);
                        } else {
                            Statement select = oConn.createStatement();
                            rs = select
                                    .executeQuery(Props.XDB_TEMP_TABLE_SELECT);
                        }
                        Statement drop = oConn.createStatement();
                        try {
                            while (rs.next()) {
                                try {
                                    String dropCommand = "DROP TABLE "
                                             + IdentifierHandler.quote(rs.getString("TABLE_NAME"));
                                    logger.info(dropCommand);
                                    drop.executeUpdate(dropCommand);
                                    oConn.commit();
                                } catch (SQLException se1) {
                                    logger.catching(se1);
                                    oConn.rollback();
                                }
                            }
                        } finally {
                            rs.close();
                        }
                    } catch (SQLException se) {
                        logger.catching(se);
                    } finally {
                        pool.releaseConnection(oConn);
                    }
                    serverLogger
                            .log(
                                    Level.INFO,
                                    "Node %0%: connection pool for database %1% is ready",
                                    new Object[] { new Integer(nodeID), dbName });
                    for (int i = 0; i < 5 && threadPool.get() == null; i++) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ie) {
                            // ignore
                        }
                    }
                    if (threadPool.get() == null) {
                        serverLogger
                                .error("Failed to initialize thread pool on node "
                                        + nodeID);
                        break;
                    }
                    NodeMessage msg = reply(initMessage,
                            NodeMessage.MSG_INIT_FROM_SYS_ACK);
                    msg.setDatabase(dbName);
                    sendMessage(msg);
                    return;
                } catch (XDBServerException ex) {
                    if (logProblem) {
                        logProblem = false;
                        serverLogger
                                .log(
                                        Level.INFO,
                                        "Node %0%: failed to create connection pool for database %1% (%2%)",
                                        new Object[] { new Integer(nodeID),
                                                dbName, ex.getMessage() });
                    }
                    if (attempts >= 0) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            // Ignore
                        }
                    } else {
                        serverLogger
                                .error("Failed to initialize connection pool for "
                                        + dbName + " on node " + nodeID);
                    }
                } finally {
                    logger.exiting(method);
                }
            }
        }
    }

    /**
     * Asynchronous initializer of NodeThread from pool
     * 
     *  
     */
    private class ThreadInitializer implements Runnable {
        private NodeMessage initMessage;

        /**
         * 
         * @param initMessage 
         */
        public ThreadInitializer(NodeMessage initMessage) {
            this.initMessage = initMessage;
        }

        /**
         * 
         * @param nt 
         * @param pool 
         */
        private void addProcess(NodeThread nt, NodeThreadPool pool) {
            IMessageListener oldProcess = NodeAgent.this.addProcess(initMessage
                    .getSessionID(), nt);
            if (oldProcess instanceof NodeThread) {
                NodeThread oldNT = (NodeThread) oldProcess;
                try {
                    synchronized (oldNT) {
                        if (pool != null && pool.isOut(oldNT)) {
                            oldNT.reset(null, null);
                            pool.releaseNodeThread(oldNT);
                        }
                        oldNT.notifyAll();
                    }
                } catch (Throwable t) {
                    logger.catching(t);
                    if (pool != null) {
                        pool.destroyObject(oldNT, true);
                    }
                }
            }
        }

        public void run() {
            final String method = "run";
            logger.entering(method);
            NodeThread nt = null;
            NodeThreadPool aThreadPool = threadPool.get();
            if (aThreadPool == null) {
                NodeMessage abort = reply(initMessage, NodeMessage.MSG_ABORT);
                abort.setCause(new XDBUnexpectedMessageException(getNodeID(),
                        "Node Agent is not initialized", initMessage));
                sendMessage(abort);
                return;
            }
            try {
                JDBCPool pool = null;

                synchronized (connectionPools) {
                    pool = connectionPools.get(initMessage.getDatabase());
                }
                if (pool == null) {
                    XDBBaseException ex = new XDBUnexpectedMessageException(
                            getNodeID(), "Not connected to database: "
                                    + initMessage.getDatabase(), initMessage);
                    logger.throwing(ex);
                    throw ex;
                }
                nt = aThreadPool.getNodeThread();
                logger.debug("Session " + initMessage.getSessionID()
                        + " - get pooled thread " + nt);
                nt.reset(initMessage.getSessionID(), pool);
                addProcess(nt, aThreadPool);
                sendMessage(reply(initMessage,
                        NodeMessage.MSG_CONNECTION_BEGIN_ACK));
            } catch (Throwable e) {
                logger.catching(e);
                NodeMessage abort = reply(initMessage, NodeMessage.MSG_ABORT);
                if (e instanceof XDBBaseException) {
                    abort.setCause((XDBBaseException) e);
                } else {
                    abort.setCause(new XDBWrappedException(nodeID, e));
                }
                sendMessage(abort);
                if (nt != null) {
                    try {
                        nt.reset(null, null);
                        aThreadPool.releaseObject(nt);
                    } catch (Exception ex) {
                        aThreadPool.destroyObject(nt, true);
                    }
                }
            } finally {
                logger.exiting(method);
            }
        }
    }

    private class ThreadFinalizer implements Runnable {
        private NodeMessage message;

        private NodeThread nt;

        /**
         * 
         * @param message 
         */
        public ThreadFinalizer(NodeMessage message) {
            this.message = message;
        }

        public void run() {
            final String method = "run";
            logger.entering(method);
            try {
                IMessageListener listener = removeProcess(message
                        .getSessionID());
                // Report success, because listener is removed and will be later
                // returned
                // to pool or thrown out if reset() fails
                sendMessage(reply(message, NodeMessage.MSG_CONNECTION_END_ACK));
                if (listener instanceof NodeThread) {
                    nt = (NodeThread) listener;
                } else {
                    logger.warn("A listener is not NodeThread but " + listener);
                    return;
                }

                NodeThreadPool aThreadPool = threadPool.get();
                logger.debug("Session " + message.getSessionID()
                        + " - release thread " + nt);
                try {
                    synchronized (nt) {
                        if (aThreadPool != null && aThreadPool.isOut(nt)) {
                            nt.reset(null, null);
                            aThreadPool.releaseObject(nt);
                        }
                        nt.notifyAll();
                    }
                } catch (Exception ex) {
                    if (aThreadPool != null) {
                        aThreadPool.destroyObject(nt, true);
                    }
                }
            } finally {
                logger.exiting(method);
            }
        }
    }
}