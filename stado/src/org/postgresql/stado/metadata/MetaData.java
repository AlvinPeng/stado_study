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
 * MetaData.java
 */

/**
 * Creating the object of MetaData can throw a XDBServerException.
 * The exception thrown can be of the nature HIGH/MEDIUM/LOW
 *
 */
package org.postgresql.stado.metadata;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import org.postgresql.stado.common.util.ParseCmdLine;
import org.postgresql.stado.common.util.Property;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBServerException;



public class MetaData {
    private static final XLogger logger = XLogger.getLogger(MetaData.class);

    // To make it easier in rewriting to support multiple databases,
    // and to smooth the way for potentially breaking out MetaData so
    // that we have an instance for each database we support,
    // database is created. This saves us from having to rewrite
    // a lot of calls in several different modules.
    public static final int INDEX_TYPE_PRIMARY_KEY = 1;

    public static final int INDEX_TYPE_UNIQUE = 2;

    public static final int INDEX_TYPE_SINGLE = 3;

    public static final int INDEX_TYPE_FIRST_IN_COMPOSITE = 4;

    public static final int INDEX_TYPE_NOT_FIRST_IN_COMPOSITE = 5;

    public static final int INDEX_TYPE_NONE = 6;

    // We now support multiple databases.
    // All of them are indexed by the database id
    private HashMap<String, SysDatabase> sysDatabaseList = new HashMap<String, SysDatabase>();

    // This is the full list of system nodes
    private Hashtable<Integer, Node> nodeList = new Hashtable<Integer, Node>();

    private Hashtable<String, SysTablespace> tablespaces = new Hashtable<String, SysTablespace>();

    private Hashtable<String, SysLogin> sysLoginList = new Hashtable<String, SysLogin>();

    // The driver information for connecting to the Metadata database
    private String metadataJdbcDriver;

    private String metadataJdbc;

    private String metadataHost;

    private int metadataPort;

    private String metadataDatabase;

    private String metadataJdbcUser;

    private String metadataJdbcPassword;

    // A single connection to the metadata database
    private Connection oConn;

    private Object connectionMutex = new Object();

    // used for preventing multiple threads
    // from accessing the connection during a
    // transaction
    private Thread transactionThread = null;

    private static MetaData metaData = null;

    /* Metadata constructor */
    private MetaData() {
    }

    /**
     * Static method that returns single metadata object.
     * @throws org.postgresql.stado.exception.XDBServerException 
     * @return the MetaData object that contains all metadata
     */
    public static MetaData getMetaData() throws XDBServerException {
        final String method = "getMetaData";
        logger.entering(method);
        try {

            // Ensure Metadata is initialized only once
            // Synchronization can be removed if MetaData.getMetaData()
            // called at XDBServer.main() before any thread started
            synchronized (MetaData.class) {
                if (metaData == null) {
                    metaData = new MetaData();
                    metaData.loadMetaData();
                }
                return metaData;
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * 
     * @return a collection of defined nodes to system
     */
    public synchronized Collection<Node> getNodes() {
        final String method = "getNodes";
        logger.entering(method);
        try {

            return nodeList.values();

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * 
     * @return a collection of databases defined in the cluster
     */
    public synchronized Collection<SysDatabase> getSysDatabases() {
        final String method = "getSysDatabases";
        logger.entering(method);
        try {

            return new ArrayList<SysDatabase>(sysDatabaseList.values());

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * 
     * @param nodeID the node id number to get
     * @throws org.postgresql.stado.exception.XDBServerException 
     * @return Node object for the node id
     */
    public synchronized Node getNode(int nodeID) throws XDBServerException {
        final String method = "getNode";
        logger.entering(method, new Object[] { new Integer(nodeID) });
        try {

            Node node = nodeList.get(nodeID);
            if (node == null) {
                XDBServerException ex = new XDBServerException(
                        "Node "
                                + nodeID
                                + " has not been registered. Please, check your config file.");
                logger.throwing(ex);
                throw ex;
            }
            return node;

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * 
     * @param dbName the database name to find
     * @throws org.postgresql.stado.exception.XDBServerException 
     * @return the SysDatabase object that corresonds to the database name
     */
    public synchronized SysDatabase getSysDatabase(String dbName)
            throws XDBServerException {
        final String method = "getSysDatabase";
        logger.entering(method, new Object[] { dbName });
        try {

            SysDatabase database = sysDatabaseList.get(dbName);
            if (database == null) {
                XDBServerException ex = new XDBServerException("Database "
                        + dbName + " has not been registered");
                logger.throwing(ex);
                throw ex;
            }
            return database;

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * 
     * @return the collection of defined tablespaces
     */
    public Collection<SysTablespace> getTablespaces() {
        return tablespaces.values();
    }

    /**
     * 
     * @param name 
     * @return whether or not this is a tablespace name
     */
    public boolean hasTablespace(String name) {
        return name != null
                && tablespaces.containsKey(name);
    }

    /**
     * 
     * @param name name of the tablespace to get
     * @return the tablespace
     */
    public SysTablespace getTablespace(String name) {
        SysTablespace tablespace = null;
        if (name != null) {
            tablespace = tablespaces.get(name);
        }
        if (tablespace == null) {
            throw new XDBServerException("Tablespace " + name + " is not found");
        }
        return tablespace;
    }

    /**
     * 
     * @param name name of the tablespace to add
     */    
    void addTablespace(SysTablespace tablespace) {
        String key = tablespace.getTablespaceName();
        if (tablespaces.containsKey(key)) {
            throw new XDBServerException("Tablespace " + key
                    + " already exists");
        }
        tablespaces.put(key, tablespace);
    }

    void removeTablespace(SysTablespace tablespace) {
        String key = tablespace.getTablespaceName();
        tablespaces.remove(key);
    }

    /**
     * This function is responsible for loading all the information about user
     * tables, columns, indexes etc.
     *
     * It keeps it in memory for faster query processing
     */
    private void loadMetaData() throws XDBServerException {
        final String method = "loadMetaData";
        logger.entering(method);
        try {

            // init config - move this out?
            initConfigValues();

            // init connection
            initConnection();

            // populate info
            readMetaDataInfo();
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Initialize metadata database and connection related configuration values
     */
    private void initConfigValues() throws XDBServerException {
        final String method = "initConfigValues";
        logger.entering(method);
        try {

            metadataJdbcDriver = Property.get("xdb.metadata.jdbcdriver",
                    Props.XDB_DEFAULT_JDBCDRIVER);
            metadataHost = Property.get("xdb.metadata.dbhost", "localhost");
            metadataPort = Property.getInt("xdb.metadata.dbport",
                    Props.XDB_DEFAULT_DBPORT);
            metadataDatabase = Property.get("xdb.metadata.database");
            if (metadataDatabase == null) {
                throw new XDBServerException(
                        "Metadata database is not specified");
            }
            metadataJdbcUser = Property.get("xdb.metadata.dbusername",
                    Props.XDB_DEFAULT_DBUSER);
            if (metadataJdbcUser == null) {
                throw new XDBServerException(
                        "User name for metadata database is not specified");
            }
            metadataJdbcPassword = Property.get("xdb.metadata.dbpassword",
                    Props.XDB_DEFAULT_DBPASSWORD);
            if (metadataJdbcPassword == null) {
                throw new XDBServerException(
                        "Password for metadata database is not specified");
            }
            metadataJdbc = Property.get("xdb.metadata.jdbcstring",
                    Props.XDB_DEFAULT_JDBCSTRING);

            // In case the user used a template, substitute any values
            HashMap<String, String> valueMap = new HashMap<String, String>();
            valueMap.put("dbhost", metadataHost);
            valueMap.put("dbport", "" + metadataPort);
            valueMap.put("database", metadataDatabase);
            valueMap.put("dbusername", metadataJdbcUser);
            valueMap.put("dbpassword", metadataJdbcPassword);

            metadataJdbc = ParseCmdLine.substitute(metadataJdbc, valueMap);

            for (int i = 1; i <= Props.XDB_NODECOUNT; i++) {
                String base = "xdb.node." + i + ".";
                String aDriver = Property.get(base + "jdbcdriver",
                        Props.XDB_DEFAULT_JDBCDRIVER);
                String aJdbcString = Property.get(base + "jdbcstring",
                        Props.XDB_DEFAULT_JDBCSTRING);
                String aHost = Property.get(base + "host", "localhost");
                String aDbHost = Property.get(base + "dbhost", aHost);
                int aDbPort = Property.getInt(base + "dbport",
                        Props.XDB_DEFAULT_DBPORT);
                String aJdbcUser = Property.get(base + "dbusername",
                        Props.XDB_DEFAULT_DBUSER);
                String aJdbcPassword = Property.get(base + "dbpassword",
                        Props.XDB_DEFAULT_DBPASSWORD);
                nodeList.put(i, new Node(i, aDriver.trim(), aJdbcString.trim(),
                        aDbHost.trim(), aDbPort, aJdbcUser, aJdbcPassword));
            }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Checks if we are able to find the driver
     *
     * @param driverString
     *                The driver class name which we want to use
     * @throws XDBServerException
     */
    private void checkDriver(String driverString) throws XDBServerException {
        final String method = "checkDriver";
        logger.entering(method, new Object[] { driverString });
        try {

            try {
                Class.forName(driverString);
            } catch (ClassNotFoundException cnfe) {
                String errorMessage = ErrorMessageRepository.CLASS_PATH_ERROR
                        + " (" + driverString + " )";
                XDBServerException ex = new XDBServerException(errorMessage,
                        cnfe, ErrorMessageRepository.CLASS_PATH_ERROR_CODE);
                logger.throwing(ex);
                throw ex;
            }

        } finally {
            logger.exiting(method);
        }
    }


    /**
     * Initializes the connection, 
     * checks whether the driver is avaialable,
     * creates a connection to the Metadata database
     *
     * @throws XDBServerException
     */
    private void initConnection() throws XDBServerException {
        final String method = "initConnection";
        logger.entering(method);
        try {

            checkDriver(metadataJdbcDriver);
            try {

                oConn = DriverManager.getConnection(metadataJdbc,
                        metadataJdbcUser, metadataJdbcPassword);
                // always use explicit transactions
                oConn.setAutoCommit(false);

            } catch (SQLException se) {
                logger.catching(se);
                String errorMessage = ErrorMessageRepository.CANNOT_INITIALIZE_NODE_CONNECTION
                        + " ( "
                        + metadataJdbc
                        + " Password :"
                        + metadataJdbcPassword
                        + "  User :"
                        + metadataJdbcUser
                        + " )";
                XDBServerException ex = new XDBServerException(
                        errorMessage,
                        se,
                        ErrorMessageRepository.CANNOT_INITIALIZE_NODE_CONNECTION_CODE);
                logger.throwing(ex);
                throw ex;
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Reads the metadata data information and stores in memory
     *
     */
    private void readMetaDataInfo() throws XDBServerException {
        final String method = "readMetaDataInfo";
        logger.entering(method);
        try {

            ResultSet aDBRS;

            try {
                aDBRS = executeQuery("SELECT * from xsysusers");
                while (aDBRS.next()) {
                    int loginID = aDBRS.getInt("userid");
                    String userName = aDBRS.getString("username").trim();
                    String userPwd = aDBRS.getString("userpwd").trim();
                    String userClass = aDBRS.getString("usertype").trim();
                    SysLogin aSysLogin = new SysLogin(loginID, userName,
                            userPwd, userClass);

                    // TODO configure if user name is case sensitive
                    sysLoginList.put(userName, aSysLogin);
                }
            } catch (SQLException se) {
                logger.catching(se);
                String errorMessage = se.getMessage() 
                		+ "\nQUERY: SELECT * from xsysusers";
                XDBServerException ex = new XDBServerException(errorMessage,
                        se, ErrorMessageRepository.SQL_EXEC_FAILURE_CODE);
                logger.throwing(ex);
                throw ex;
            }

            // Add dummy xdbadmin database for utilities.
            sysDatabaseList.put(Props.XDB_ADMIN_DATABASE, SysDatabase
                    .getAdminDatabase());

            try {
                aDBRS = executeQuery("SELECT * from xsysdatabases");
                while (aDBRS.next()) {
                    SysDatabase aSysDatabase = new SysDatabase(aDBRS
                            .getInt("dbid"), aDBRS.getString("dbname"),
                            aDBRS.getBoolean("isSpatial"));

                    // Add it to the database list.
                    sysDatabaseList.put(aSysDatabase.getDbname().trim(),
                            aSysDatabase);
                }
            } catch (SQLException se) {
                logger.catching(se);
                String errorMessage = se.getMessage()
                        + "\nQUERY: SELECT dbname from xsysdatabases";
                XDBServerException ex = new XDBServerException(errorMessage,
                        se, ErrorMessageRepository.SQL_EXEC_FAILURE_CODE);
                logger.throwing(ex);
                throw ex;
            }

            beginTransaction();
            try {
                PreparedStatement ps = null;
                aDBRS = executeQuery("SELECT * from xsystablespaces");
                while (aDBRS.next()) {
                    int tablespaceID = aDBRS.getInt("tablespaceid");
                    String tablespaceName = aDBRS.getString("tablespacename");
                    int ownerID = aDBRS.getInt("ownerid");
                    Map<Integer, String> locations = new HashMap<Integer, String>();
                    if (ps == null) {
                        ps = prepareStatement("select nodeid, filepath from xsystablespacelocs where tablespaceid = ?");
                    }
                    ps.setInt(1, tablespaceID);
                    ResultSet locsRS = ps.executeQuery();
                    while (locsRS.next()) {
                        locations.put(new Integer(locsRS.getInt(1)), locsRS
                                .getString(2));
                    }
                    locsRS.close();
                    SysTablespace aSysTablespace = new SysTablespace(
                            tablespaceID, tablespaceName, ownerID, locations);
                    // Add it to the list.
                    tablespaces.put(tablespaceName, aSysTablespace);
                }
                commitTransaction(null);
            } catch (SQLException se) {
                rollbackTransaction();
                logger.catching(se);
                String errorMessage = "Can not load tablespaces";
                XDBServerException ex = new XDBServerException(errorMessage,
                        se, ErrorMessageRepository.SQL_EXEC_FAILURE_CODE);
                logger.throwing(ex);
                throw ex;
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * 
     * @param login the login to create 
     */
    public synchronized void insertLogin(SysLogin login) {
        // TODO configure if user name is case sensitive
        sysLoginList.put(login.getName(), login);
        for (SysDatabase database : sysDatabaseList.values()) {
            database
                    .insertUser(new SysUser(login.getLoginID(), login, database));
        }
    }

    /**
     * 
     * @param userName 
     * @return 
     */
    public synchronized SysLogin getSysLogin(String userName) {
        // TODO configure if user name is case sensitive
        SysLogin aSysLogin = sysLoginList.get(userName);
        if (aSysLogin == null) {
            XDBServerException ex = new XDBServerException("User " + userName
                    + " has not been registered");
            logger.throwing(ex);
            throw ex;
        }
        return aSysLogin;
    }

    /**
     * 
     * @return collection of known SysLogins
     */
    public synchronized Collection<SysLogin> getSysLogins() {
        return new ArrayList<SysLogin>(sysLoginList.values());
    }

    /**
     * Removes the login from the metadata database
     * @param login SysLogin to remove
     */
    public synchronized void removeLogin(SysLogin login) {
        // TODO configure if user name is case sensitive
        sysLoginList.remove(login.getName());
        for (SysDatabase database : sysDatabaseList.values()) {
            database.removeUser(login.getName());
        }
    }

    /**
     * 
     * @param dbName 
     * @param owner 
     * @throws org.postgresql.stado.exception.XDBServerException 
     * @return the newly defined SysDatabase object
     */
    public synchronized SysDatabase createDatabase(String dbName, SysLogin owner, boolean isSpatial)
            throws XDBServerException {
        final String method = "createDatabase";
        logger.entering(method, new Object[] { dbName });
        try {

            // Make sure that it is not a duplicate database
            if (sysDatabaseList.containsKey(dbName)) {
                String errorMessage = ErrorMessageRepository.DUP_DATABASE_ERROR
                        + " " + dbName;
                throw new XDBServerException(errorMessage,
                        XDBServerException.SEVERITY_LOW,
                        ErrorMessageRepository.DUP_DATABASE_ERROR_CODE);
            }

            SysDatabase db = new SysDatabase(-1, dbName, isSpatial);
            for (SysLogin login : sysLoginList.values()) {
                SysUser user = new SysUser(login.getLoginID(), login, db);
                db.insertUser(user);
            }
            // just add a placeholder
            sysDatabaseList.put(dbName, null);
            return db;

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * @param dbName
     */
    public synchronized void dropTempDatabase(String dbName) {
        // Ensure that database is "temp"
        if (sysDatabaseList.containsKey(dbName)
                && sysDatabaseList.get(dbName) == null) {
            sysDatabaseList.remove(dbName);
        }
    }

    /**
     * 
     * @param database 
     * @throws org.postgresql.stado.exception.XDBServerException 
     */
    public synchronized int makeDbPersistent(SysDatabase database)
            throws XDBServerException {
        final String method = "makeDbPersistent";
        logger.entering(method, new Object[] { database });
        try {

            // Make sure that it is not a persistent database
            if (database.getDbid() != -1) {
                String errorMessage = ErrorMessageRepository.DUP_DATABASE_ERROR
                        + " " + database.getDbname();
                throw new XDBServerException(errorMessage,
                        XDBServerException.SEVERITY_LOW,
                        ErrorMessageRepository.DUP_DATABASE_ERROR_CODE);
            }
            if (database.getDBNodeList().isEmpty()) {
                throw new XDBServerException(
                        "No nodes are defined in database "
                                + database.getDbname());
            }
            ResultSet rs;
            String commandStr = null;
            beginTransaction();
            try {

                // Insert database
                commandStr = "SELECT max(dbid) FROM xsysdatabases";
                rs = executeQuery(commandStr);
                try {
                    rs.next();
                    database.setDbid(rs.getInt(1) + 1);
                } finally {
                    rs.close();
                }
                commandStr = "INSERT INTO xsysdatabases"
                        + " (dbid, dbname, isspatial) VALUES (" + database.getDbid() + ","
                        + "'" + database.getDbname() + "', " + database.isSpatial() + ")";
                if (executeUpdate(commandStr) == 0) {
                    XDBServerException ex = new XDBServerException(
                            "Failed to insert row into \"xsysdatabases\"");
                    logger.throwing(ex);
                    throw ex;
                }

                // Insert nodes
                makeDBNodesPersistent(database.getDBNodeList());

                commitTransaction(null);
            } catch (Exception e) {
                logger.catching(e);
                rollbackTransaction();
                String errorMessage = e.getMessage()
                        + "\nQUERY: " + commandStr;
                XDBServerException ex = new XDBServerException(errorMessage, e,
                        ErrorMessageRepository.SQL_EXEC_FAILURE_CODE);
                logger.throwing(ex);
                throw ex;
            }
            sysDatabaseList.put(database.getDbname(), database);
            return database.getDbid();
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * 
     * @param nodeList 
     * @throws java.sql.SQLException 
     */
    public synchronized void makeDBNodesPersistent(Collection<DBNode> nodeList)
            throws SQLException {
        final String method = "makeDBNodesPersistent";
        logger.entering(method, new Object[] { nodeList });
        try {

            String commandStr = null;
            ResultSet rs;
            int dbnodeid;
            rs = executeQuery("SELECT max(dbnodeid) FROM xsysdbnodes");
            try {
                rs.next();
                dbnodeid = rs.getInt(1) + 1;
            } finally {
                rs.close();
            }

            for (DBNode dbnode : nodeList) {
                commandStr = "INSERT INTO xsysdbnodes"
                        + " (dbnodeid, dbid, nodeid)" + " VALUES" + " ("
                        + dbnodeid++ + ", " + dbnode.getDatabase().getDbid()
                        + ", " + dbnode.getNodeId() + ")";
                executeUpdate(commandStr);
            }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Drops the database metadata info from the metadata database
     *
     * @param dbName database name to drop
     * @throws java.sql.SQLException 
     */
    public synchronized void dropDatabase(String dbName) throws SQLException {
        String commandStr = "";
        // prepare to acqure locks on database and the nodes
        synchronized (getStartupLock()) {
            SysDatabase database = getSysDatabase(dbName);
            if (database.isStarted()) {
                throw new SQLException(
                        "Database is running, please stop it first");
            }
            synchronized (database) {
                // Loop through, delete all table info, one by one
                beginTransaction();
                try {
                    // xsyschecks
                    commandStr = "DELETE FROM xsyschecks WHERE constid IN (SELECT constid FROM xsysconstraints WHERE tableid IN (SELECT tableid FROM xsystables WHERE dbid = "
                            + database.getDbid() + "))";
                    executeUpdate(commandStr);
                    // xsysviewscolumns
                    commandStr = "DELETE FROM xsysviewdeps WHERE viewid IN (SELECT viewid FROM xsysviews WHERE dbid = "
                            + database.getDbid() + ")";
                    executeUpdate(commandStr);
                    commandStr = "DELETE FROM xsysviewscolumns WHERE viewid IN (SELECT viewid FROM xsysviews WHERE dbid = "
                            + database.getDbid() + ")";
                    executeUpdate(commandStr);
                    // xsysviews
                    commandStr = "DELETE FROM xsysviews WHERE dbid = "
                            + database.getDbid();
                    executeUpdate(commandStr);
                    // xsystabprivs
                    commandStr = "DELETE FROM xsystabprivs WHERE tableid IN (SELECT tableid FROM xsystables WHERE dbid = "
                            + database.getDbid() + ")";
                    executeUpdate(commandStr);
                    // xsysforeignkeys
                    commandStr = "DELETE FROM xsysforeignkeys WHERE refid IN (SELECT refid FROM xsysreferences WHERE constid IN (SELECT constid FROM xsysconstraints WHERE tableid IN (SELECT tableid FROM xsystables WHERE dbid = "
                            + database.getDbid() + ")))";
                    executeUpdate(commandStr);
                    // xsysreferences
                    commandStr = "DELETE FROM xsysreferences WHERE constid IN (SELECT constid FROM xsysconstraints WHERE tableid IN (SELECT tableid FROM xsystables WHERE dbid = "
                            + database.getDbid() + "))";
                    executeUpdate(commandStr);
                    // xsysconstraints
                    commandStr = "DELETE FROM xsysconstraints WHERE tableid IN (SELECT tableid FROM xsystables WHERE dbid = "
                            + database.getDbid() + ")";
                    executeUpdate(commandStr);
                    // xsysindexkeys
                    commandStr = "DELETE FROM xsysindexkeys WHERE idxid IN (SELECT idxid FROM xsysindexes WHERE tableid IN (SELECT tableid FROM xsystables WHERE dbid = "
                            + database.getDbid() + "))";
                    executeUpdate(commandStr);
                    // xsysindexes
                    commandStr = "DELETE FROM xsysindexes WHERE tableid IN (SELECT tableid FROM xsystables WHERE dbid = "
                            + database.getDbid() + ")";
                    executeUpdate(commandStr);
                    // xsyscolumns
                    commandStr = "DELETE FROM xsyscolumns WHERE tableid IN (SELECT tableid FROM xsystables WHERE dbid = "
                            + database.getDbid() + ")";
                    executeUpdate(commandStr);
                    // xsystabparthash
                    commandStr = "DELETE FROM xsystabparthash WHERE tableid IN (SELECT tableid FROM xsystables WHERE dbid = "
                            + database.getDbid() + ")";
                    executeUpdate(commandStr);
                    // xsystabparts
                    commandStr = "DELETE FROM xsystabparts WHERE tableid IN (SELECT tableid FROM xsystables WHERE dbid = "
                            + database.getDbid() + ")";
                    executeUpdate(commandStr);
                    // xsystables
                    commandStr = "DELETE FROM xsystables WHERE dbid = "
                            + database.getDbid();
                    executeUpdate(commandStr);
                    // xsysdbnodes
                    commandStr = "DELETE FROM xsysdbnodes WHERE dbid = "
                            + database.getDbid();
                    executeUpdate(commandStr);
                    // xsysdatabases
                    commandStr = "DELETE FROM xsysdatabases WHERE dbid = "
                            + database.getDbid();
                    executeUpdate(commandStr);

                    commitTransaction(null);
                } catch (Exception se) {
                    logger.catching(se);
                    rollbackTransaction();
                    String errorMessage = se.getMessage()
                            + "\nQUERY: " + commandStr;
                    XDBServerException ex = new XDBServerException(
                            errorMessage, se,
                            ErrorMessageRepository.SQL_EXEC_FAILURE_CODE);
                    logger.throwing(ex);
                    throw ex;
                }
                // To clear references from Nodes to database we are deleting
                for (Node node : getNodes()) {
                    node.removeDBNode(dbName);
                }
            }
            sysDatabaseList.remove(dbName);
            getStartupLock().notifyAll();
        }
    }

    /**
     * Begins a transaction 
     * @throws org.postgresql.stado.exception.XDBServerException 
     */
    public void beginTransaction() throws XDBServerException {
        final String method = "beginTransaction";
        logger.entering(method);
        try {

            synchronized (connectionMutex) {
                if (transactionThread == Thread.currentThread()) {
                    // we are already in transaction
                    return;
                }

                // check if someone else is trying to modify MetaData
                while (transactionThread != null) {
                    try {
                        connectionMutex.wait();
                    } catch (InterruptedException e) {
                    }
                }

                transactionThread = Thread.currentThread();
            }
            try {
                if (oConn == null || oConn.isClosed()) {
                    initConnection();
                }
            } catch (SQLException e) {
                logger.catching(e);
                oConn = null;
                XDBServerException ex = new XDBServerException(
                        "Failed to check is connection closed", e);
                logger.throwing(ex);
                throw ex;
            }

        } finally {
            logger.exiting(method);
        }
    }
    
    /**
     * Commits a transation
     * @param refresher 
     * @throws org.postgresql.stado.exception.XDBServerException 
     */
    public void commitTransaction(IMetaDataUpdate refresher)
            throws XDBServerException {
        final String method = "commitTransaction";
        logger.entering(method, new Object[] { refresher });
        try {

            synchronized (connectionMutex) {
                if (transactionThread != Thread.currentThread()) {
                    XDBServerException ex = new XDBServerException(
                            "Illegal attempt to commit transaction. Current thread: "
                                    + Thread.currentThread()
                                    + ", transaction was started by thread: "
                                    + transactionThread);
                    logger.throwing(ex);
                    throw ex;
                }
                oConn.commit();
                if (refresher != null) {
                    refresher.refresh();
                }
                transactionThread = null;
                connectionMutex.notifyAll();
            }

        } catch (Exception e) {
            logger.catching(e);
            XDBServerException ex = new XDBServerException(
                    "Failed to commit transaction", e);
            logger.throwing(ex);
            throw ex;
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Rollsback transaction
     * @throws org.postgresql.stado.exception.XDBServerException 
     */
    public void rollbackTransaction() throws XDBServerException {
        final String method = "rollbackTransaction";
        logger.entering(method);
        try {

            synchronized (connectionMutex) {
                if (transactionThread != Thread.currentThread()) {
                    XDBServerException ex = new XDBServerException(
                            "Illegal attempt to rollback transaction. Current thread: "
                                    + Thread.currentThread()
                                    + ", transaction was started by thread: "
                                    + transactionThread);
                    logger.throwing(ex);
                    throw ex;
                }
                try {
                    oConn.rollback();
                } catch (Exception e) {
                    logger.catching(e);
                    XDBServerException ex = new XDBServerException(
                            "Failed to rollback transaction", e);
                    logger.throwing(ex);
                    throw ex;
                } finally {
                    // If it is valid attempt to rollback transaction, ensure it
                    // is
                    // properly closed
                    transactionThread = null;
                    connectionMutex.notifyAll();
                }
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Executes specified query on the meta data database
     *
     * @param sql the statement to execute
     * @return ResultSet
     */
    public ResultSet executeQuery(String sql) throws XDBServerException {
        final String method = "executeQuery";
        logger.entering(method, new Object[] { sql });
        ResultSet rs = null;
        try {
            synchronized (connectionMutex) {
                /*
                 * boolean autoTransaction = (transactionThread == null); if
                 * (autoTransaction) { beginTransaction(); }
                 */
                try {
                    Statement stmt = oConn.createStatement();
                    rs = stmt.executeQuery(sql);
                    /*
                     * if (autoTransaction) { commitTransaction(null); }
                     */
                    return rs;
                } catch (SQLException e) {
                    /*
                     * logger.catching(e); if (autoTransaction) {
                     * rollbackTransaction(); }
                     */
                    XDBServerException ex = new XDBServerException(
                            ErrorMessageRepository.SQL_STATEMENT_CREATE_FAILURE,
                            e, ErrorMessageRepository.SQL_EXEC_FAILURE_CODE);
                    logger.throwing(ex);
                    throw ex;
                }
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Executes specified SQL statement
     * @param sql statement to execute 
     * @return the number of rows affected
     */
    public int executeUpdate(String sql) {
        final String method = "executeUpdate";
        logger.entering(method, new Object[] { sql });
        try {

            synchronized (connectionMutex) {
                boolean autoTransaction = transactionThread == null;
                if (autoTransaction) {
                    beginTransaction();
                }
                try {
                    Statement statement = oConn.createStatement();
                    int rowsAffected = statement.executeUpdate(sql);
                    if (autoTransaction) {
                        commitTransaction(null);
                    }
                    return rowsAffected;
                } catch (SQLException e) {
                    logger.catching(e);
                    if (autoTransaction) {
                        rollbackTransaction();
                    }
                    XDBServerException ex = new XDBServerException(
                            "Metadata update failed: " + e.getMessage());
                    logger.throwing(ex);
                    throw ex;
                }
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Executes specified SQL statement
     * @param sql statement to execute 
     * @return the result set of the generated keys
     */
    public ResultSet executeUpdateReturning(String sql) {
        final String method = "executeUpdateReturning";
        logger.entering(method, new Object[] { sql });
        try {

            synchronized (connectionMutex) {
                boolean autoTransaction = transactionThread == null;
                if (autoTransaction) {
                    beginTransaction();
                }
                try {
                    Statement statement = oConn.createStatement();
                    ResultSet rs = statement.executeQuery(sql);
                    if (autoTransaction) {
                        commitTransaction(null);
                    }
                    return rs;
                } catch (SQLException e) {
                    logger.catching(e);
                    if (autoTransaction) {
                        rollbackTransaction();
                    }
                    XDBServerException ex = new XDBServerException(
                            "Metadata update failed: " + e.getMessage());
                    logger.throwing(ex);
                    throw ex;
                }
            }

        } finally {
            logger.exiting(method);
        }
    }

    /*
     * Monitor to synchronize starting of databases and nodes
     */
    private Object startupLock = new Object();

    Object getStartupLock() {
        return startupLock;
    }

    /**
     * @param sql
     * @return the PreparedStatement
     */
    public PreparedStatement prepareStatement(String sql)
            throws XDBServerException {
        final String method = "prepareStatement";
        logger.entering(method, new Object[] {});
        try {

            synchronized (connectionMutex) {
                if (Thread.currentThread() == transactionThread) {
                    try {
                        return oConn.prepareStatement(sql);
                    } catch (SQLException e) {
                        logger.catching(e);
                        XDBServerException ex = new XDBServerException(
                                e.getMessage() + "\nQUERY: " + sql, e,
                                ErrorMessageRepository.SQL_EXEC_FAILURE_CODE);
                        logger.throwing(ex);
                        throw ex;
                    }
                }
            }
            return null;

        } finally {
            logger.exiting(method);
        }
    }

    private static NodeDBConnectionInfo connectionInfo;

    /**
     * @return the connection info for the metadata database
     * @see org.postgresql.stado.metadata.NodeDBConnectionInfo
     */
    public static final NodeDBConnectionInfo getMetadataDBConnectionInfo() {
        if (connectionInfo == null) {
            MetaData instance;
            if (metaData == null) {
                // Create temp instance and load only config values,
                // it is possible database does not exist yet
                instance = new MetaData();
                instance.initConfigValues();
            } else {
                // point to existing instance
                instance = metaData;
            }
            java.util.Properties props = new java.util.Properties();
            String defaultPrefix = "xdb.default.custom.";
            String nodePrefix = "xdb.node.metadata.custom.";
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
            return new NodeDBConnectionInfo(0, instance.metadataHost,
                    instance.metadataPort, instance.metadataDatabase,
                    instance.metadataJdbcUser, instance.metadataJdbcPassword,
                    props);
        }
        return connectionInfo;
    }

    /**
     * @param databases
     * @return the connection info for specified databases
     * @see org.postgresql.stado.metadata.NodeDBConnectionInfo
     */
    public NodeDBConnectionInfo[] getNodeDBConnectionInfos(
            Collection<String> databases) {
        Collection<NodeDBConnectionInfo> connectionInfos = new ArrayList<NodeDBConnectionInfo>();
        for (String string : databases) {
            SysDatabase db = metaData.getSysDatabase(string);
            // Load from master database
            db.admin();
            for (DBNode dbNode : db.getDBNodeList()) {
                connectionInfos.add(dbNode.getNodeDBConnectionInfo());
            }
        }
        return connectionInfos.toArray(new NodeDBConnectionInfo[connectionInfos
                .size()]);
    }
}
