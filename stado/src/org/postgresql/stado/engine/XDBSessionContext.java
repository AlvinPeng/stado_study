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
package org.postgresql.stado.engine;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.naming.Context;  
import javax.naming.NamingException;  
import javax.naming.ldap.InitialLdapContext;  
import javax.naming.ldap.LdapContext;  

import org.postgresql.stado.common.CommandLog;
import org.postgresql.stado.common.util.ParseCmdLine;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.communication.CoordinatorAgent;
import org.postgresql.stado.engine.io.MessageTypes;
import org.postgresql.stado.engine.io.ResponseMessage;
import org.postgresql.stado.engine.loader.LoaderConnectionPool;
import org.postgresql.stado.exception.XDBSecurityException;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.MetaData;
import org.postgresql.stado.metadata.Node;
import org.postgresql.stado.metadata.NodeDBConnectionInfo;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysLogin;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.SysUser;
import org.postgresql.stado.metadata.scheduler.ILockCost;
import org.postgresql.stado.metadata.scheduler.ITransaction;
import org.postgresql.stado.metadata.scheduler.Scheduler;
import org.postgresql.stado.optimizer.QueryTree;
import org.postgresql.stado.parser.Parser;
import org.postgresql.stado.parser.SqlCreateUser;
import org.postgresql.stado.parser.SqlGrant;
import org.postgresql.stado.parser.SqlModifyTable;


/**
 * An XDBSessionContext maintains state information for client sessions.
 * XDBSessionContext's lifetime, use cases, etc. 1. Client contacts the server,
 * socket connection is established. Session can not be used for any purpose
 * until user is not authorized against some database. 2. User logs in on some
 * database. Session is available for use. Between 2 and 2' Session is fully
 * functional. 2' User is logged off from database. All queries are interrupted,
 * all resources are freed. After this session can be closed 1' Session is
 * closed, socket connection is broken.
 * 
 * Monitors: sessions: Access to the list of active sessions, if session is
 * created, destroyed or ExecutableRequest are enumerated this: list of
 * ExecutableRequests of the current session executorAccess: access to
 * MultinodeExecutor instance database.getScheduler(): access to executables and
 * currentRequest
 */
public class XDBSessionContext implements MessageTypes, ITransaction {
    private static final XLogger logger = XLogger
            .getLogger(XDBSessionContext.class);

    // A logger to trace session instance
    private XLogger logSession;

    private static final Collection<XDBSessionContext> sessions = new HashSet<XDBSessionContext>();

    private static volatile int lastSessionID = 0;

    /** whether or not the user has made changes to their 
     * connection/environment */
    private boolean usedSet = false;
    
    /**
     * Global statement Id
     */
    private long statementId;

    /**
     * @param protocol
     * @return
     */
    public static final XDBSessionContext createSession()
            throws XDBServerException {
        XDBSessionContext session = new XDBSessionContext();
        synchronized (sessions) {
            if (sessions.size() < Props.XDB_MAXCONNECTIONS) {
                sessions.add(session);
                return session;
            } else {
                throw new XDBServerException("Too many connections");
            }
        }
    }

    /**
     * @throws org.postgresql.stado.exception.XDBSecurityException
     */
    public ExecutionResult shutdownDatabase() throws XDBSecurityException {
        checkPrivileged();

        Collection<XDBSessionContext> toClose = new LinkedList<XDBSessionContext>();
        synchronized (sessions) {
            for (XDBSessionContext other : sessions) {
                if (other.getSysDatabase() != getSysDatabase() || this == other) {
                    continue;
                }
                SysUser otherUser = other.getCurrentUser();

                if (otherUser == null || otherUser == getCurrentUser()
                        || otherUser.getUserClass() != SysLogin.USER_CLASS_DBA) {
                    toClose.add(other);
                } else {
                    XDBServerException se = new XDBServerException(
                            "Can not get exclusive access to database "
                                    + getDBName()
                                    + ": there is active DBA connection");
                    logger.throwing(se);
                    throw se;
                }
            }
        }
        for (XDBSessionContext session : toClose) {
            try {
                session.close();
            } catch (IOException ioe) {
                logger.catching(ioe);
            }
        }
        // after logout this.database will be set to null
        SysDatabase db = database;
        logout();
        CommandLog.cmdLogger.info(sessionID + " - Stop database: " + db);
        CoordinatorAgent.getInstance().shutdownDatabase(db);
        CoordinatorPools.destoryPool(db.getDbname());
        LoaderConnectionPool.getConnectionPool().removeDatabase(db.getDbname());
        return ExecutionResult
                .createSuccessResult(ExecutionResult.COMMAND_STOP_DATABASE);
    }

    /**
     * @throws org.postgresql.stado.exception.XDBSecurityException
     */
    public void shutdown() throws XDBSecurityException {
        synchronized (sessions) {
            for (XDBSessionContext session : sessions) {
                if (this != session) {
                    throw new XDBServerException("There are active connections");
                }
            }
        }
        XLogger.getLogger("Server").info("*** Server is going down");
        for (SysDatabase database : MetaData.getMetaData().getSysDatabases()) {
            // Zahid: We want to proceed with the shutdown process if the virtual xdb admin db is running
            if (database.isStarted() && !database.getDbname().equalsIgnoreCase(Props.XDB_ADMIN_DATABASE)) {
                throw new XDBServerException("Database " + database.getDbname()
                        + " is running");
            }
        }
        // Stop listening
        CoordinatorAgent.getInstance().resetNodes(false);

        int waitFor = 60; // Up to one minute by default
        for (int i = 0; i < waitFor; i++) {
            boolean allDown = true;
            for (Node node : MetaData.getMetaData().getNodes()) {
                allDown = allDown && !node.isUp();
            }
            if (allDown) {
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // Ignore exception
            }
        }

        CoordinatorAgent.releaseInstance();
        XLogger.getLogger("Server").info("*** Server is down, exiting...");
        System.exit(0);
    }


    /**
     * constructs the session context
     * 
     * @param protocol
     */
    private XDBSessionContext() {
        sessionID = ++lastSessionID;
        tempTableMap = new Hashtable<String, String>();
        savepointTable = new HashMap<String, String>();

        requestId = 0;

        logSession = XLogger.getLogger(XDBSessionContext.class.getName() + "."
                + sessionID);
        logSession.debug("Session object is created");
    }

    /**
     * close this session
     * 
     * @throws java.io.IOException
     */
    public ExecutionResult close() throws IOException {
        logSession.debug("Closing session");

        logout();
        CoordinatorAgent.getInstance().removeProcess(new Integer(sessionID));
        logSession.debug("Session is closed");

        // BUILD_CUT_START - just for debugging, not production
        logger.debug("Parse Time: " + Parser.parseTimer.getDuration());
        // logger.debug ("Analyze Time: (contains parse time): " +
        // RequestAnalyzer.analyzeTimer.getDuration());
        logger.debug("Execution Time: "
                + ExecutableRequest.requestTimer.getDuration());
        logger.debug("Batch Execution Time: "
                + ExecutableRequest.batchTimer.getDuration());
        // BUILD_CUT_END
        return ExecutionResult
                .createSuccessResult(ExecutionResult.COMMAND_LOGOUT);
    }


    /*
     * Client connection section start
     */
    /*
     * Nothing is modified during Session's lifetime No concurrent access No
     * need to synchronize access
     */
    private int sessionID;

    /**
     * returns the seesion id for this session
     * 
     * @return
     */
    public int getSessionID() {
        return sessionID;
    }

    /** get the clients socket channel to use for this session */
    /*
     * Client connection section end
     */
    /*
     * Database Connection section start
     */
    /*
     * Nothing is modified during Session's lifetime No concurrent access No
     * need to synchronize access
     */
    private String dbName;

    private SysDatabase database;

    private String userName;

    private SysUser currentUser;

    private volatile boolean connected = false;

    private String mode = null;

    private Properties extraParams;

    /**
     * @return
     */

    public String getDBName() {
        return dbName;
    }

    /**
     * @return the current database
     */
    public SysDatabase getSysDatabase() {
        return database;
    }

    /**
     * @return
     */

    public SysUser getCurrentUser() {
        return currentUser;
    }

    /**
     * @throws org.postgresql.stado.exception.XDBSecurityException
     */

    private void checkPrivileged() throws XDBSecurityException {
        if (currentUser.getUserClass() != SysLogin.USER_CLASS_DBA) {
            throw new XDBSecurityException(
                    "Only DBA user can perform this task");
        }
    }

    public void useDB(String dbName, String mode) {
        if (dbName.equals(this.dbName)) {
            return;
        }
        if (connected) {
            logout();
        }
        this.dbName = dbName;
        if (CONNECTION_MODE_CREATE.equals(mode)) {
            this.mode = CONNECTION_MODE_CREATE;
        } else if (CONNECTION_MODE_ADMIN.equals(mode)) {
            this.mode = CONNECTION_MODE_ADMIN;
        } else if (CONNECTION_MODE_PERSISTENT.equals(mode)) {
            this.mode = CONNECTION_MODE_PERSISTENT;
        } else {
            this.mode = CONNECTION_MODE_NORMAL;
        }
        if (CONNECTION_MODE_CREATE != this.mode) {
            database = MetaData.getMetaData().getSysDatabase(dbName);
            if (CONNECTION_MODE_ADMIN != this.mode && !database.isStarted()) {
                throw new XDBServerException("Database " + dbName
                        + " is not started");
            }
        }
        checkDbUser();
    }

    public void setUser(String userName) {
        if (userName.equals(this.userName)) {
            return;
        }
        if (connected) {
            logout();
        }
        this.userName = userName;
        checkDbUser();
    }

    /*
     * Functions to support parameters passed with the startup message. We used
     * to just ignore them, but JDBC driver expects they are returned as
     * ParameterStatus messages. That may be useful for other clients. Later on,
     * we may want to use these parameters within the session.
     */
    public void setExtraParameter(String key, String value) {
        if (extraParams == null) {
            if (value == null)
                return;
            else
                extraParams = new Properties();
        }
        if (value == null)
            extraParams.remove(key);
        else
            extraParams.setProperty(key, value);
    }

    public String getExtraParameter(String key) {
        return extraParams == null ? null : extraParams.getProperty(key);
    }

    public Set<String> getExtraParamNames() {
        return extraParams == null ? null : extraParams.stringPropertyNames();
    }

    private void checkDbUser() {
        if (database != null && userName != null) {
            if (mode == CONNECTION_MODE_ADMIN && !database.isOnline()) {
                database.admin();
            }
            currentUser = database.getSysUser(userName);
        }
    }

    public ExecutionResult login(String connUser, String connDatabase, 
            String mode, String password) throws XDBSecurityException,
            XDBServerException {

        SysLogin login;
        SqlCreateUser createUser = null;
        boolean failed = false;
        String exceptionMsg = "Invalid LDAP configuration or login";
        
        if (connUser == null) {
            throw new XDBServerException("User name is not specified");
        }
        
        if (connDatabase == null) {
            throw new XDBServerException("Database name is not specified");
        }
        
        logSession.debug("User " + userName + " is logging in to database "
                + dbName);
        
        if (mode == null) {
            mode = MessageTypes.CONNECTION_MODE_NORMAL;
        }
                
        // Check authentication method, see if LDAP chosen
        if (Props.XDB_AUTHENTICATION.equalsIgnoreCase("ldap")) {

            String securityPrincipal;
            String securityCredentials;
            
            LdapContext ctx = null;
        
            if (Props.XDB_LDAP_PROVIDER_URL == null ||  Props.XDB_LDAP_PROVIDER_URL.length() == 0) {
                throw new XDBServerException("Property xdb.ldap.provider_url must be set to authenticate");
            }
            
            if (Props.XDB_LDAP_SECURITY_PRINCIPAL != null || Props.XDB_LDAP_SECURITY_PRINCIPAL.length() > 0) {
                securityPrincipal = Props.XDB_LDAP_SECURITY_PRINCIPAL.replace("{username}",connUser);
            } else {
                securityPrincipal = connUser;
            }
            
            if (Props.XDB_LDAP_SECURITY_CREDENTIALS != null || Props.XDB_LDAP_SECURITY_CREDENTIALS.length() > 0) {
                securityCredentials = Props.XDB_LDAP_SECURITY_CREDENTIALS.replace("{password}",password);
            } else {
                securityCredentials = password;
            }
            
            // Set up the environment for creating the initial context
            Hashtable<String, String> env = new Hashtable<String, String>();

            env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
            env.put(Context.PROVIDER_URL, Props.XDB_LDAP_PROVIDER_URL);
            // 
            env.put(Context.SECURITY_AUTHENTICATION, Props.XDB_LDAP_SECURITY_AUTHENTICATION);         
            env.put(Context.SECURITY_PRINCIPAL, securityPrincipal); 
            env.put(Context.SECURITY_CREDENTIALS, securityCredentials);

            // Create the initial context
            try {
                ctx = new InitialLdapContext(env, null);
            } catch (NamingException ne) {
                failed = true;
            }

             // If it failed, try again without the domain
            if (ctx == null || failed) {
                
                failed = false;               

                connUser = ParseCmdLine.stripDomain(connUser);

                if (Props.XDB_LDAP_SECURITY_PRINCIPAL != null || Props.XDB_LDAP_SECURITY_PRINCIPAL.length() > 0) {
                    securityPrincipal = Props.XDB_LDAP_SECURITY_PRINCIPAL.replace("{username}",connUser);
                } else {
                    securityPrincipal = connUser;
                }                   

                env.put(Context.SECURITY_PRINCIPAL, securityPrincipal);  

                try {
                    ctx = new InitialLdapContext(env, null);
                }
                catch (NamingException ne) {
                    failed = true;
                    exceptionMsg = ne.getLocalizedMessage();
                }
            }
            
            if (ctx == null || failed) {    
                
                // It could be that they are trying with a local
                // admin user, allow us to fallback on that.
                // The password cannot be empty in that case                
                if (password != null && password.length() > 0) {
                    
                    login = null;
                    
                    try {
                        login = MetaData.getMetaData().getSysLogin(connUser);
                    } catch (Exception e) {
                        // ignore
                    }
                    if (login != null && login.isDBA()) {
                        login.checkPassword(password);
                        setUser(connUser);
                        useDB(connDatabase, MessageTypes.CONNECTION_MODE_NORMAL);
                        return login(login);
                    }
                }
                           
                throw new XDBServerException(exceptionMsg);                
            }
 
            try {
                ctx.close();
            } catch (NamingException ne) { 
                throw new XDBServerException(ne.getLocalizedMessage());   
            }
            
            // For purposes of dealing with the database, strip the domain
            // info if we have not already
            connUser = ParseCmdLine.stripDomain(connUser);
            
            // Try and login. If the user does not exist, try and create it
            login = null;
            try {
                login = MetaData.getMetaData().getSysLogin(connUser);

            } catch (XDBServerException xe) {
                             
                /* Create it in the database */
                createUser = new SqlCreateUser(this.getSysDatabase(), 
                        connUser, "", SysLogin.getUserClass(Props.XDB_AUTHENTICATION_NEW_USER_CLASS));
                try {
                    createUser.execute(Engine.getInstance());
                } catch (Exception e) {
                    throw new XDBServerException(e.getLocalizedMessage());
                }
                
                setUser(connUser);     
                useDB(connDatabase, mode);
                login = MetaData.getMetaData().getSysLogin(connUser);
                         
                if (Props.XDB_AUTHENTICATION_NEW_USER_GRANT) {
                    SqlGrant grant = new SqlGrant(this);
                    grant.addGrantee(connUser);
                    grant.setGrant(true);
                    grant.setForceGrant(true);

                    if (Props.XDB_AUTHENTICATION_NEW_USER_GRANT_SELECT) {
                        grant.setHasSelect(true);
                    }
                    if (Props.XDB_AUTHENTICATION_NEW_USER_GRANT_INSERT) {
                        grant.setHasInsert(true);
                    }                    
                    if (Props.XDB_AUTHENTICATION_NEW_USER_GRANT_UPDATE) {
                        grant.setHasUpdate(true);
                    }
                    if (Props.XDB_AUTHENTICATION_NEW_USER_GRANT_DELETE) {
                        grant.setHasDelete(true);
                    }
                    if (Props.XDB_AUTHENTICATION_NEW_USER_GRANT_TABLES != null) {
                        grant.setTableListFromString(Props.XDB_AUTHENTICATION_NEW_USER_GRANT_TABLES);
                    }

                    try {
                        grant.execute(Engine.getInstance());
                    } catch (Exception e) {
                        throw new XDBServerException(e.getLocalizedMessage());
                    }
                }
            }
            
            if (createUser == null) {
                // We did not need to create it, just set up connection          
                setUser(connUser);
                useDB(connDatabase, mode);
            }
            
        } else {
            // Regular authentication case
            setUser(connUser);
            useDB(connDatabase, mode);
        
            login = MetaData.getMetaData().getSysLogin(userName);
            login.checkPassword(password);
        }
        return login(login);        
    }

    public ExecutionResult login(SysLogin login) throws XDBSecurityException,
    XDBServerException {
    	return login(login, false);
    }
    
    public ExecutionResult login(SysLogin login, boolean isSpatial) throws XDBSecurityException,
            XDBServerException {
        if (CONNECTION_MODE_CREATE == mode) {
            logSession.debug("This is createdb request");
            if (dbName == null) {
                throw new XDBServerException("Database name is not specified");
            }
            database = MetaData.getMetaData().createDatabase(dbName, login, isSpatial);
            logSession.debug("Temp database and user have been created");
        }
        if (database == null) {
            throw new XDBServerException("Database name is not specified");
        }
        if (CONNECTION_MODE_ADMIN == mode) {
            if (login.getUserClass() != SysLogin.USER_CLASS_DBA) {
                throw new XDBSecurityException(
                        "Non-privileged user can not perform privileged task");
            }
            database.admin();
        }
        logSession.debug("Starting client session for " + login);
        currentUser = database.getSysUser(login.getName());
        connected = true;
        return ExecutionResult
                .createSuccessResult(ExecutionResult.COMMAND_LOGIN);
    }

    /**
     * We are serving a client request here, since they are serialized, we
     * suppose that client will keep silence till we finish. Nothing is thrown
     * from here, we will catch an suppress any error
     */
    public void logout() {
        logSession.debug("User " + currentUser + " is logging out");
        try {
            if (!connected) {
                logSession.debug("Ooops, not connected !");
                // Not connected - nothing to do
                return;
            }
            connected = false;
            // If createdb failed or connection broken
            if (mode == CONNECTION_MODE_CREATE) {
                logSession.debug("We were creating new database, cleaning up");
                MetaData.getMetaData().dropTempDatabase(dbName);
            }
            logSession.debug("Cleaning up ExecutableRequests");
            // If something is currently running try and kill it
            synchronized (this) {
                for (ExecutableRequest request : executables) {
                    request.cancel();
                }
                executables.clear();
                notifyAll();
            }
            logSession.debug("Releasing query responses and temp tables...");
            Engine engine = Engine.getInstance();
            try {
                if (!isInTransaction) {
                    engine.beginTransaction(this, database.getDBNodeList());
                }
                // Clear Query responses
                for (ServerStatement statement : cursors.values()) {
                    try {
                        statement.close();
                    } catch (Exception ex) {
                        logger.catching(ex);
                    }
                }
                cursors.clear();
                // Clear temp tables
                Collection<String> tempTables = getTempTableNames();
                for (String table : tempTables) {
                    database.dropSysTable(table);
                }
                engine.dropNodeTempTables(tempTables, database.getDBNodeList(),
                        this);

                engine.commitTransaction(this, database.getDBNodeList());
            } catch (Throwable t) {
                logger.catching(t);
            }
            logSession.debug("...done");
            // Force and release all the lock
            synchronized (database.getScheduler()) {
                logSession.debug("Releasing locks from LM, if still held");
                database.getLm().notifyTransactionEnd(this);
            }
        } finally {
            sessions.remove(this);
            clearCoordinatorConnection();
            releaseExecutor();
            currentUser = null;
            userName = null;
            database = null;
            dbName = null;
            logSession.debug("Logout is completed");
        }
    }

    /*
     * Database Connection section end
     */
    /*
     * Batch section start
     */
    /*
     * Batch on nodes is set up in Executor's thread, so all calls are
     * serialized. No need to syncronize. user batch is set up by single
     * ClientRequest. No need to syncronize, since user requests are serialized.
     * BatchGroups is valid during single request execution, no need to
     * synchronize too.
     *
     * If we are doing a batch on Nodes connections are persistent.
     */
    private boolean isInBatch = false;

    /**
     * @param value
     */

    void setInBatch(boolean value) {
        isInBatch = value;
        batchGroup = null;
        dirtyWriteTables = null;
        dirtyReadTables = null;
    }

    private BatchInsertGroup batchGroup = null;

    private HashSet<SysTable> dirtyWriteTables = null;

    private HashSet<SysTable> dirtyReadTables = null;

    /**
     * Put specified statement into current or new batch group and returns the
     * group
     * 
     * @param insert
     *                the statement
     * @return the batch group
     * @see org.postgresql.stado.engine.BatchInsertGroup
     */
    public BatchInsertGroup getInsertGroup(SqlModifyTable insert) {
        if (dirtyReadTables != null
                && dirtyReadTables.contains(insert.getTargetTable())) {
            batchGroup = null;
            dirtyReadTables = null;
            dirtyWriteTables = null;
        }
        if (dirtyWriteTables != null) {
            for (SysTable table : insert.getReadTables()) {
                if (dirtyWriteTables.contains(table)) {
                    batchGroup = null;
                    dirtyReadTables = null;
                    dirtyWriteTables = null;
                    break;
                }
            }
        }
        if (batchGroup == null) {
            batchGroup = new BatchInsertGroup(insert);
        } else {
            batchGroup.addMember(insert);
        }
        return batchGroup;
    }

    /**
     * @param insert
     */

    public void closeInsertGroup(SqlModifyTable insert) {
        if (batchGroup != null) {
            if (dirtyReadTables == null) {
                dirtyReadTables = new HashSet<SysTable>(insert.getReadTables());
            } else {
                dirtyReadTables.addAll(insert.getReadTables());
            }
            if (dirtyWriteTables == null) {
                dirtyWriteTables = new HashSet<SysTable>();
            }
            dirtyWriteTables.add(insert.getTargetTable());
        }
    }

    /*
     * Batch section end
     */
    /*
     * Coordinator connection section start
     */
    /*
     * Accessed only from execution thread. No need to synchronize
     */
    private Connection coordConnection;

    /**
     * @return
     */

    public Connection getAndSetCoordinatorConnection() {
        if (coordConnection == null) {
            coordConnection = CoordinatorPools.getConnection(dbName);
        }
        return coordConnection;
    }

    /**
     * @return
     */

    public Connection resetCoordinatorConnection() {
        if (coordConnection != null && !isPersistent()) {
            CoordinatorPools.destroyConnection(dbName, coordConnection);

            coordConnection = CoordinatorPools.getConnection(dbName);
        }
        return coordConnection;
    }

    /**
     * "Clears" the coordinator connection if session is not persistent
     */
    private void clearCoordinatorConnection() {
        if (coordConnection != null) {
            try {
                coordConnection.rollback();
                CoordinatorPools.releaseConnection(dbName, coordConnection);
            } catch (Exception e) {
                logger.catching(e);
                CoordinatorPools.destroyConnection(dbName, coordConnection);
            }

            coordConnection = null;
        }
    }

    /*
     * Coordinator connection section end
     */
    /*
     * Multinode Executor connection section start
     */
    /*
     * Request ID is accessed only from executor thread and does not need to be
     * synchronized. MultinodeExecutor is also accessed by kill(), it should be
     * synchronized. Kill executes only method koll() on ME, it is thread-safe
     */
    /**
     * For allowing messaging to keep requests straight.
     */
    private int requestId;

    private Object executorAccess = new Object();

    private MultinodeExecutor lastExecutor = null;

    /**
     * @return
     */

    public int getRequestId() {
        return requestId == Integer.MAX_VALUE ? requestId = 0 : ++requestId;
    }

    private void releaseExecutor() {
        synchronized (executorAccess) {
            if (lastExecutor != null) {
                logSession.debug("Releasing MultinodeExecutor");
                lastExecutor.releaseNodeThreads();
                lastExecutor = null;
            }
        }
    }

    /**
     * @param nodeList
     * @return
     */

    public MultinodeExecutor getMultinodeExecutor(Collection<DBNode> nodeList) {
        synchronized (executorAccess) {
            if (lastExecutor == null) {
                logSession.debug("Creating new MultinodeExecutor");
                lastExecutor = new MultinodeExecutor(nodeList, this);
            } else {
                // add any additional missing nodes if necessary.
                lastExecutor.addNeededNodes(nodeList);
            }
            return lastExecutor;
        }
    }

    /*
     * Multinode Executor connection section end
     */
    /*
     * Transactions section start
     */
    /*
     * Transactions and subTransactions are only accessed from executor thread
     * No need to synchronize
     */

    /** if we are in the middle of a subtransaction */
    private HashMap<String, String> savepointTable; // boolean

    // isInSubTransaction;

    /** if we are in the middle of a transaction */
    private boolean isInTransaction;

    private int isolationLevel = Connection.TRANSACTION_READ_COMMITTED;

    /**
     * @param savepointName
     */

    void setSavepoint(String savepointName) {
        savepointTable.put(savepointName, savepointName);
    }

    /**
     * @param savepointName
     */

    void clearSavepoint(String savepointName) {
        savepointTable.remove(savepointName);
    }

    /**
     * @return
     */

    public boolean isInSubTransaction() {
        return savepointTable.size() != 0;
    }

    /**
     * Note if we are in a transaction or not
     * 
     * @param flag
     */
    void setInTransaction(boolean flag) {
        if (!flag) {
            // clear savepoints after commit or rollback
            savepointTable.clear();
            if (database != null) {
                logSession
                        .debug("Transaction is completed, releasing all the locks");
                synchronized (database.getScheduler()) {
                    database.getLm().notifyTransactionEnd(this);
                }
            }
        }
        isInTransaction = flag;
    }

    /**
     * @return
     */

    public boolean isInTransaction() {
        return isInTransaction;
    }

    /**
     * @return
     */

    public int getTransactionIsolation() {
        return isolationLevel;
    }

    /**
     * @param newLevel
     * @return
     */

    public int setTransactionIsolation(int newLevel) {
        if (newLevel != isolationLevel) {
            switch (newLevel) {
            case Connection.TRANSACTION_READ_UNCOMMITTED:
            case Connection.TRANSACTION_READ_COMMITTED:
                isolationLevel = Connection.TRANSACTION_READ_COMMITTED;
            case Connection.TRANSACTION_REPEATABLE_READ:
            case Connection.TRANSACTION_SERIALIZABLE:
            default:
                isolationLevel = Connection.TRANSACTION_SERIALIZABLE;
            }
        }
        return isolationLevel;
    }

    /*
     * Transactions section end
     */
    /*
     * Temp Tables section start
     */
    /*
     * Temp tebles are created and deleted while request is executed (in
     * executor thread), but this info is needed while statement is parsed (in
     * client thread). Use thread-safe storage to synchronize access
     */

    // List of temp table
    private Hashtable<String, String> tempTableMap;

    /**
     * @param referenceName
     * @param tableName
     */

    public void registerTempTableWithSession(String referenceName,
            String tableName) {
        tempTableMap.put(referenceName.toUpperCase(), tableName);
    }

    /**
     * @param referenceName
     */
    public void deregisterTempTableWithSession(String referenceName) {
        tempTableMap.remove(referenceName.toUpperCase());
    }

    /**
     * Return real (backend) name of local temp table
     * 
     * @param referenceName
     *                the local name
     * @return the real name
     */
    public String getTempTableName(String referenceName) {
        return tempTableMap.get(referenceName.toUpperCase());
    }

    /**
     * @return
     */

    public Collection<String> getTempTableNames() {
        return new ArrayList<String>(tempTableMap.values());
    }

    /*
     * Temp Tables section end
     */
    /*
     * Request Execution section start
     */
    private LinkedList<ExecutableRequest> executables = new LinkedList<ExecutableRequest>();

    /**
     * @return
     */
    public Map<ExecutableRequest, XDBSessionContext> getRequests() {
        Map<ExecutableRequest, XDBSessionContext> requests = new HashMap<ExecutableRequest, XDBSessionContext>();
        synchronized (sessions) {
            for (XDBSessionContext client : sessions) {
                if (currentUser.getUserClass() == SysLogin.USER_CLASS_DBA
                        || currentUser == client.currentUser) {
                    synchronized (client) {
                        for (ExecutableRequest request : client.executables) {
                            requests.put(request, client);
                        }
                    }
                }
            }
        }
        return requests;
    }

    // Below goes experimental and draft code to support server-side prepared
    // statements
    // Request is parsed outside and registered with session using
    // createStatement, then cursor name amd values for parameters are assigned
    // using bindStatement and executed using executeRequest
    private Map<String, ServerStatement> statements = new HashMap<String, ServerStatement>();

    private Map<String, ServerStatement> cursors = new HashMap<String, ServerStatement>();
    
    private Map<String, DeclaredCursor> userCursors = new HashMap<String, DeclaredCursor>();
    
    public void createCursor(String cursorName, QueryTree cursorTree, boolean isScrollable, boolean isHoldable) {
    	userCursors.put(cursorName, new DeclaredCursor(cursorName, cursorTree, isScrollable, isHoldable));
    }

    public DeclaredCursor getCursor(String cursorName) {
    	return userCursors.get(cursorName);
    }
    
    public void createStatement(String statementID, ExecutableRequest request) {
        closeStatement(statementID);
        statements.put(statementID, new ServerStatement(statementID, request, this));
    }

    public void setParameterTypes(String statementID, int[] parameterTypes) {
        ServerStatement statement = statements.get(statementID);
        if (statement == null) {
            throw new XDBServerException("Statement does not exist");
        }
        statement.setParameterTypes(parameterTypes);
    }

    public void bindStatement(String statementID, String cursorID,
            String[] paramValues) {
        closeCursor(cursorID);
        ServerStatement statement = statements.get(statementID);
        if (statement == null) {
            throw new XDBServerException("Statement does not exist");
        }
        ServerStatement previous = cursors.put(cursorID, statement);
        if (previous != null) {
            previous.close();
        }
        statement.bind(cursorID, paramValues);
        statement.setBindStepComplete(true);
    }
    
    public void clearAllCursors() {
        cursors.clear();
    }

    public ResponseMessage describeStatement(String statementID, String cursorID) {
        ServerStatement statement = null;
        if (cursorID != null) {
            statement = cursors.get(cursorID);
        }
        if (statement == null && statementID != null) {
            statement = statements.get(statementID);
        }
        if (statement == null) {
            throw new XDBServerException("Statement or cursor does not exist");
        }
        return statement.describe();
    }

    public int[] getParameterTypes(String statementID) {
        ServerStatement statement = statements.get(statementID);
        if (statement == null) {
            throw new XDBServerException("Statement or cursor does not exist");
        }
        return statement.getParameterTypes();
    }

    public ExecutionResult executeRequest(String cursorID) {
        ServerStatement statement = cursors.get(cursorID);
        if (statement == null) {
            throw new XDBServerException("Cursor does not exist");
        }
        return statement.execute();
    }

    public void createResultSet(String cursorID, ExecutionResult result) {
        ServerStatement previous = cursors.put(cursorID, new ServerStatement(
                cursorID, result, this));
        if (previous != null) {
            previous.close();
        }
    }

    public ResultSet getResultSet(String cursorID) {
        ServerStatement statement = cursors.get(cursorID);
        if (statement == null) {
            throw new XDBServerException("Cursor does not exist");
        }
        return statement.getResultSet();
    }

    public void closeStatement(String statementID) {
        ServerStatement statement = statements.remove(statementID);
        if (statement != null && statement != cursors.get(statement.getCursorID())) {
            statement.close();
        }
    }

    public void closeCursor(String cursorID) {
        ServerStatement statement = cursors.remove(cursorID);
        if (statement != null && statement != statements.get(statement.getStatementID())) {
            statement.close();
        }
        tryToReleaseConnections();
    }

    /**
     * This method is blocking - it returns only if request is successfully
     * completed or rejected with error. Request handling sequence: 1. Request
     * is enqueued. Execution is blocked until the request is first in the
     * queue. 2. Request is scheduled. It is registered with the Scheduler and
     * compete for limited resources. Scheduler may block current thread if no
     * free slots to execute query. If the request is scheduled and queue has
     * been empty it is released for execution immediately. 3. XDBSessionContext
     * tries and acquires locks needed by call to LockManager, then reports
     * result back to Scheduler. There are three possible results: a.
     * LockManager succeeded and locks are acquired. Request is executed. Go to
     * Step 4. b. LockManager failed (there are conflicting locks) and no locks
     * are acquired. Request is resubmitted to scheduler and get blocked until a
     * concurrent request is completed. Repeat Step 3. c. Exception is thrown
     * (deadlock is detected) and no locks are acquired. Clean up is performed
     * and exception is thrown out. 4. Locks are released by call to
     * LockManager. It is possible that LockManager does not release the locks
     * immediately. If there is active transaction LockManager will held the
     * locks until transaction is committed or rolled back. 5. Request us
     * removed from scheduler.
     * 
     * @param executableRequest
     * @return the execution result
     */
    ExecutionResult executeRequest(ExecutableRequest executableRequest) {
        synchronized (this) {
            // Add to local queue
            executables.addLast(executableRequest);
            // Wait while the request becomes first
            while (!executables.isEmpty()
                    && executables.getFirst() != executableRequest
                    && !executableRequest.cancelled()) {
                try {
                    wait();
                } catch (InterruptedException ie) {
                    // ignore
                }
            }
            if (executableRequest.cancelled()) {
                executables.remove(executableRequest);
                notifyAll();
                return ExecutionResult
                        .createErrorResult(new XDBServerException(
                                "Request has been cancelled"));
            }
        }
        // We are here, this means scheduler allow the request being executed
        ExecutionResult result = null;
        // Make a copy - the field may be set to null if session is closed
        SysDatabase database = this.database;
        try {
            Scheduler scheduler = database.getScheduler();
            scheduler.addRequest(executableRequest.getCost(), this);
            ILockCost lockCost = executableRequest.getCost().getSqlObject();
            try {
                while (result == null) {
                    // Register the request with scheduler
                    // try and place locks, acquire other resources
                    boolean locked = true;
                    if (lockCost != null) {
                        if (lockCost.needCoordinatorConnection()) {
                            try {
                                getAndSetCoordinatorConnection();
                            } catch (Exception e) {
                                locked = false;
                            }
                        }
                        if (locked) {
                            locked = database.getLm().getLock(
                                    lockCost.getLockSpecs(), this, lockCost.isReadOnly());
                        }
                    }
                    if (locked) {
                        result = executableRequest.execute(
                                Engine.getInstance(), this);
                        // remove locks
                        // This result is not stored in the ServerStatement yet,
                        // so it won't be found by hasActiveResultSet()
                        // Check it right here
                        // TODO merge ExecutableRequest and ServerStatement
                        // and register result with the statement as soon as
                        // possible
                        if (result == null || !result.hasResultSet()) {
                            tryToReleaseConnections();
                        }
                        database.getLm().releaseLock(this);
                    } else {
                        // remove locks
                        tryToReleaseConnections();
                        scheduler.holdRequest(executableRequest.getCost());
                        if (executableRequest.cancelled()) {
                            database.getLm().notifyRefusedRequest(this);
                            return ExecutionResult
                                    .createErrorResult(new XDBServerException(
                                            "Request has been cancelled"));
                        }
                    }
                    if (result == null) {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException ie) {
                        }
                    }
                }
            } finally {
                // Unregister the request with scheduler
                scheduler.removeRequest(executableRequest.getCost());
            }
        } catch (Exception e) {
            result = ExecutionResult.createErrorResult(e);
        } finally {
            synchronized (this) {
                executables.remove(executableRequest);
                notifyAll();
            }
        }
        return result;
    }

    private boolean forcePersistent = false;

    /**
     * gets if the session should be persistent
     * 
     * @return
     */
    public boolean isPersistent() {
        return forcePersistent || isInTransaction || savepointTable.size() != 0
                || isInBatch || hasActiveResultSets()
                || tempTableMap.size() != 0 || hasPreparedStatements()
                || (usedSet && Props.XDB_PERSIST_ON_SET)
                || this.mode == CONNECTION_MODE_PERSISTENT;
    }

    /**
     * @param persistent
     */

    void setPersistent(boolean persistent) {
        forcePersistent = persistent;
    }

    /**
     * releases connections back to the pool
     */
    private void tryToReleaseConnections() {
        // if the state is not persistent release the connections
        if (!isPersistent()) {
            // Release MultinodeExecutor and its NodeThreads
            releaseExecutor();
            // Take care of coordinator, too.
            clearCoordinatorConnection();
        }
    }

    /**
     * @return
     */

    boolean hasActiveResultSets() {
        for (ServerStatement statement : cursors.values()) {
            if (statement.hasResultSet()) {
                return true;
            }
        }
        return false;
    }

    /*
     * Request Execution section end
     */
    /*
     * Kill section start
     */
    /**
     * Kill current request
     */
    public void kill() {
        /*
         * Interrupt current work
         */
        synchronized (executorAccess) {
            if (lastExecutor != null) {
                lastExecutor.kill();
            }
        }
        /*
         * If command is in queue mark it as killed
         */
        synchronized (this) {
            if (!executables.isEmpty()) {
                kill(executables.getFirst());
            }
        }
    }

    /**
     * @param execRequest
     */
    public synchronized void kill(ExecutableRequest execRequest) {
        execRequest.cancel();
        notifyAll();
    }

    /*
     * Kill section end
     */

    /**
     * @param nodes
     * @throws org.postgresql.stado.exception.XDBSecurityException
     */
    public ExecutionResult createDatabase(String[] nodes)
            throws XDBSecurityException {
        checkPrivileged();
        // Check is current database new
        if (database.getDbid() == -1 && database.getDBNodeList().isEmpty()) {
            HashSet<Integer> nodeList = new HashSet<Integer>();
            for (String element : nodes) {
                nodeList.add(new Integer(element));
            }
            database.createDBNodes(nodeList);
        } else {
            // silently ignore
        }
        return ExecutionResult
                .createSuccessResult(ExecutionResult.COMMAND_CREATE_DATABASE);
    }

    /**
     * @throws org.postgresql.stado.exception.XDBSecurityException
     */
    public ExecutionResult persistDatabase() throws XDBSecurityException {
        checkPrivileged();
        int dbid = MetaData.getMetaData().makeDbPersistent(database);
        CoordinatorPools.initPool(database.getDbname(), database.getCoordinatorNodeID());
        CoordinatorAgent.getInstance().connectToDatabase(database);
        return ExecutionResult
                .createGeneratorRangeResult(ExecutionResult.COMMAND_PERSIST_DATABASE, dbid);
    }

    /**
     * @param waitFor
     * @throws XDBSecurityException
     */
    public ExecutionResult startDatabase(int waitFor)
            throws XDBSecurityException {
        
        checkPrivileged();
        CommandLog.cmdLogger.info(sessionID + " - Start database: " + database);
        CoordinatorPools.initPool(dbName, database.getCoordinatorNodeID());
        CoordinatorAgent.getInstance().connectToDatabase(database);
        if (waitFor > 0 && !database.waitForOnline((long) waitFor * 1000)) {
            XDBServerException ex = new XDBServerException(
                    "Failed to bring database online in " + waitFor
                            + " seconds");
            logger.throwing(ex);
            throw ex;
        }
        return ExecutionResult
                .createSuccessResult(ExecutionResult.COMMAND_START_DATABASE);
    }

    /**
     * Returns connection infos for nodes of current database
     * 
     * @param nodes
     *                the numbers of interested nodes, all if empty
     * @return the array of NodeDBConnectionInfo objects
     * @throws XDBSecurityException
     * @see org.postgresql.stado.metadata.NodeDBConnectionInfo
     */
    public NodeDBConnectionInfo[] getConnectionInfos(String[] nodes)
            throws XDBSecurityException {
        NodeDBConnectionInfo[] connectionInfos = null;
        if (nodes == null || nodes.length == 0) {
            if (database == null || database.getDbid() == -1) {
                // Client is running dropdb -f
                Collection<Node> nodeList = MetaData.getMetaData().getNodes();
                connectionInfos = new NodeDBConnectionInfo[nodeList.size()];
                int i = 0;
                for (Node node : nodeList) {
                    connectionInfos[i++] = node.getNodeDBConnectionInfo(dbName);
                }
            } else {
                Collection<DBNode> dbNodes = database.getDBNodeList();
                connectionInfos = new NodeDBConnectionInfo[dbNodes.size()];
                int i = 0;
                for (DBNode node : dbNodes) {
                    connectionInfos[i++] = node.getNodeDBConnectionInfo();
                }
            }
        } else {
            connectionInfos = new NodeDBConnectionInfo[nodes.length];
            for (int i = 0; i < nodes.length; i++) {
                int nodeID = Integer.parseInt(nodes[i]);
                Node node = MetaData.getMetaData().getNode(nodeID);
                connectionInfos[i] = node.getNodeDBConnectionInfo(dbName);
            }
        }
        return connectionInfos;
    }

    /**
     * @throws SQLException
     */
    public ExecutionResult dropDatabase() throws SQLException {
        checkPrivileged();
        if (database.getDbid() != -1) {
            // This is not a temp database which would be dropped at client
            // logout
            MetaData.getMetaData().dropDatabase(dbName);
        }
        return ExecutionResult
                .createSuccessResult(ExecutionResult.COMMAND_DROP_DATABASE);
    }

    private Map<String, Collection<DBNode>> preparedCommands = new HashMap<String, Collection<DBNode>>();

    /**
     * @param commandID
     * @param execNodeList
     * @return
     */
    public String registerPreparedStatement(String commandID,
            Collection<DBNode> execNodeList) {
        int modifier = 0;
        String id = commandID;
        synchronized (preparedCommands) {
            while (preparedCommands.containsKey(id)) {
                id = commandID + modifier++;
            }
            preparedCommands.put(id, execNodeList);
        }
        return id;
    }

    /**
     * @param id
     */
    public void unregisterPreparedStatement(String commandID) {
        synchronized (preparedCommands) {
            preparedCommands.remove(commandID);
        }
    }

    public Collection<DBNode> getPreparedStatementNodes(String commandID) {
        synchronized (preparedCommands) {
            return preparedCommands.get(commandID);
        }
    }

    public boolean hasPreparedStatements() {
        synchronized (preparedCommands) {
            return !preparedCommands.isEmpty();
        }
    }

    /**
     * Set the current statement id.
     * 
     * @param value
     */
    public void setStatementId(long value) {
        statementId = value;
    }

    /**
     * @return current statement id
     */
    public long getStatementId() {
        return statementId;
    }
    
    /**
     * The client used SET, we need to persist the connection.
     */
    public void setUsedSet() {
        usedSet = true;
    }
}
