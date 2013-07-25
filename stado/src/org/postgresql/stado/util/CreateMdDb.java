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
/****************************************************************************
 *
 * createmddb 
 *          [-u initial user -p initial password]
 *          [-i param.init]
 *          [-m]
 *
 *
 * nodelist is comma separated, eg, 1,2,4
 * -m sets it to "manual" mode - it will not physically create the database,
 * but will just try and create the metadata database schema.
 *
 * Note we do not specify the database name. We use the one specified
 * in the xdb.config file.
 *
 ****************************************************************************/
package org.postgresql.stado.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Vector;

import org.postgresql.stado.common.util.ParseCmdLine;
import org.postgresql.stado.common.util.Property;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.MetaData;
import org.postgresql.stado.metadata.NodeDBConnectionInfo;
import org.postgresql.stado.metadata.SysLogin;


/**
 * 
 */
public class CreateMdDb {
    private static String osUsername;

    private static String osPassword;

    private static String fileListString;

    private static Vector<String> fileList;

    private static boolean isManual = false;

    private static HashMap<String, String> valueMap = new HashMap<String, String>();

    private final static String createStatements[] = {
            "create table xsystablespaces (" + " tablespaceid serial,"
                    + " tablespacename varchar(255) not null,"
                    + " ownerid int not null," + " primary key(tablespaceid)"
                    + ")",
            "create unique index idx_xsystablespaces_1"
                    + " on xsystablespaces (tablespacename)",
            "create table xsystablespacelocs ("
                    + " tablespacelocid int not null,"
                    + " tablespaceid int not null,"
                    + " filepath varchar(1024) not null,"
                    + " nodeid int not null," + " primary key(tablespacelocid)"
                    + ")",
            "create unique index idx_xsystablespacelocs_1"
                    + " on xsystablespacelocs (tablespaceid, nodeid)",
            "alter table xsystablespacelocs"
                    + " add foreign key (tablespaceid) references xsystablespaces (tablespaceid)",

            "create table xsysusers (" + " userid int not null,"
                    + " username char(30) not null,"
                    + " userpwd char(32) not null,"
                    + " usertype char(8) not null," + " primary key (userid) "
                    + ")",
            "create unique index idx_xsysusers_1" + " on xsysusers (username)",

            "create table xsysdatabases (" + " dbid int not null,"
                    + " dbname varchar(128) not null,"
                    + " isspatial boolean not null, "
                    + " primary key (dbid)" + ")",
            "create unique index idx_xsysdatabases_1"
                    + " on xsysdatabases (dbname)",

            "create table xsysdbnodes (" + " dbnodeid int not null,"
                    + " dbid int not null," + " nodeid int not null,"
                    + " primary key (dbid, nodeid)" + ")",
            "create unique index idx_dbnodes1 on xsysdbnodes (dbnodeid)",
            "alter table xsysdbnodes"
                    + " add foreign key (dbid) references xsysdatabases (dbid)",
            "create table xsystables (" + " tableid serial,"
                    + " dbid integer not null,"
                    + " tablename char(128) not null,"
                    + " numrows bigint not null,"
                    + " partscheme smallint not null, "
                    + " partcol char(128),    " + " parthash int,"
                    + " owner int," + " parentid int," + " tablespaceid int,"
                    + " clusteridx varchar(80)," + " primary key (tableid)"
                    + ")"

            ,
            "alter table xsystables"
                    + " add foreign key (dbid) references xsysdatabases (dbid)",
            "alter table xsystables"
                    + " add foreign key (parentid) references xsystables (tableid)",
            "alter table xsystables"
                    + " add foreign key (tablespaceid) references xsystablespaces (tablespaceid)",
            "create table xsystabparts (" + " partid int not null,"
                    + " tableid integer not null," + " dbid integer not null,"
                    + " nodeid int not null," + " primary key (partid)" + ")",
            "alter table xsystabparts"
                    + " add foreign key (tableid) references xsystables (tableid)",
            "alter table xsystabparts"
                    + " add foreign key (dbid, nodeid) references xsysdbnodes (dbid, nodeid)",

            "create table xsystabparthash (" + " parthashid int not null,"
                    + " tableid integer not null," + " dbid integer not null,"
                    + " hashvalue integer not null," + " nodeid int not null,"
                    + " primary key (parthashid)" + ")",
            "alter table xsystabparthash"
                    + " add foreign key (tableid) references xsystables (tableid)",
            "alter table xsystabparthash"
                    + " add foreign key (dbid, nodeid) references xsysdbnodes (dbid, nodeid)",
            "create table xsyscolumns (" + " colid serial,"
                    + " tableid int not null," + " colseq smallint not null,"
                    + " colname varchar(128) not null,"
                    + " coltype smallint not null," + " collength int,"
                    + " colscale smallint," + " colprecision smallint,"
                    + " isnullable smallint not null," + " isserial smallint,"
                    + " defaultexpr varchar(255)," + " checkexpr varchar(255),"
                    + " selectivity float," + " nativecoldef varchar(255), "
                    + " primary key (colid)" + ")",
            "alter table xsyscolumns"
                    + " add foreign key (tableid) references xsystables (tableid)",
            "create unique index idx_xsyscolumns_1"
                    + " on xsyscolumns (tableid, colseq)",
            "create table xsysindexes (" + " idxid int not null,"
                    + " idxname varchar(80) not null,"
                    + " tableid int not null," + " keycnt smallint not null,"
                    + " idxtype char(1),   " + " tablespaceid int,"
                    + " usingtype varchar(80) ," + " wherepred varchar(1024) ,"
                    + " issyscreated smallint not null ,"
                    + " primary key (idxid)" + ")",
            "alter table xsysindexes"
                    + " add foreign key (tableid) references xsystables (tableid)"
            // ,
            // "create index idx_xsysindexes_1" +
            // " on xsysindexes (idxname)"
            ,
            "alter table xsysindexes"
                    + " add foreign key (tablespaceid) references xsystablespaces (tablespaceid)",

            "create table xsysindexkeys (" + " idxkeyid int not null,"
                    + " idxid int not null," + " idxkeyseq int not null,"
                    + " idxascdesc smallint not null , "
                    + " colid int not null," + " coloperator varchar(80),"
                    + " primary key (idxkeyid)" + ")",
            "alter table xsysindexkeys"
                    + " add foreign key (idxid) references xsysindexes (idxid)",
            "alter table xsysindexkeys"
                    + " add foreign key (colid) references xsyscolumns (colid)",
            "create unique index idx_xsysindexkeys_1"
                    + " on xsysindexkeys (idxid, idxkeyseq)",
            "create table xsysconstraints (" + " constid int not null,"
                    + " constname varchar(128),  " + " tableid int not null,"
                    + " consttype char(1) not null, " + " idxid int,"
                    + " issoft smallint not null ," + " primary key (constid)"
                    + ")",

            "alter table xsysconstraints"
                    + " add foreign key (tableid) references xsystables (tableid)",
            "alter table xsysconstraints"
                    + " add foreign key (idxid) references xsysindexes (idxid)",
            "create table xsysreferences (" + " refid int not null,"
                    + " constid int not null," + " reftableid int not null,"
                    + " refidxid int not null,  " + " primary key (refid)"
                    + ")",
            "alter table xsysreferences"
                    + " add foreign key (constid) references xsysconstraints (constid)",
            "alter table xsysreferences"
                    + " add foreign key (reftableid) references xsystables (tableid)",
            "alter table xsysreferences"
                    + " add foreign key (refidxid) references xsysindexes (idxid)",

            "create table xsysforeignkeys (" + " fkeyid int not null,"
                    + " refid int not null," + " fkeyseq int not null,"
                    + " colid int not null," + " refcolid int not null,"
                    + " primary key (fkeyid)" + ")",
            "alter table xsysforeignkeys"
                    + " add foreign key (refid) references xsysreferences (refid)",
            "alter table xsysforeignkeys"
                    + " add foreign key (colid) references xsyscolumns (colid)",
            "alter table xsysforeignkeys"
                    + " add foreign key (refcolid) references xsyscolumns (colid)",
            "create unique index idx_xsysforeignkeys_1"
                    + " on xsysforeignkeys (refid, fkeyseq)",
            "create table xsystabprivs (" + " privid int not null,"
                    + " userid int," + " tableid int not null,"
                    + " selectpriv char(1) not null,"
                    + " insertpriv char(1) not null,"
                    + " updatepriv char(1) not null,"
                    + " deletepriv char(1) not null,"
                    + " referencespriv char(1) not null,"
                    + " indexpriv char(1) not null,"
                    + " alterpriv char(1) not null," + " primary key (privid)"
                    + ")",
            "alter table xsystabprivs"
                    + " add foreign key (userid) references xsysusers (userid)",
            "alter table xsystabprivs"
                    + " add foreign key (tableid) references xsystables (tableid)",
            "create unique index idx_xsystabprivs_1"
                    + " on xsystabprivs (userid, tableid)",
            "alter table xsystables"
                    + " add foreign key (owner) references xsysusers (userid)",
            "create table xsysviews (" + " viewid int not null,"
                    + " dbid int not null," + " viewname varchar(255),"
                    + " viewtext varchar(7500)," + " primary key(viewid))",
            "create unique index idx_xsysviews_1" + " on xsysviews (viewid)",
            "create table xsysviewscolumns (" + "viewcolid int not null, "
                    + "viewid int not null, " + "viewcolseqno int not null,"
                    + "viewcolumn varchar(255),"
                    + "coltype smallint not null, " + "collength int, "
                    + "colscale smallint, " + "colprecision smallint, "
                    + "primary key (viewcolid))",
            "create table xsysviewdeps   (" + "viewid int not null, "
                    + "columnid int not null, " + "tableid int not null) ",

            "create unique index idx_sysviewscols_1 on xsysviewscolumns (viewid, viewcolseqno)",
            "alter table xsysviewscolumns add foreign key (viewid) references xsysviews (viewid)",
            "alter table xsysviewdeps add foreign key (viewid) references xsysviews (viewid)",
            "alter table xsysviews"
                    + " add foreign key (dbid) references xsysdatabases (dbid)",
            "create table xsyschecks (" + " checkid int not null,"
                    + " constid int not null," + " seqno int not null,"
                    + " checkstmt varchar(8000)," + " primary key (checkid))",
            "create unique index idx_xsyschecks_1"
                    + " on xsyschecks (constid, seqno)",
            "alter table xsyschecks"
                    + " add foreign key (constid) references xsysconstraints (constid)" };

    private static Connection conn;

    // -------------------------------------------------------------------------
    /**
     * 
     * @param args
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        fileList = new Vector<String>();

        try {

            processArgs(args);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.err.println("Parameters: " + " [-i initscript]"
                    + " [-m] [-u username] [-p password]");
            System.exit(1);
        }
        NodeDBConnectionInfo connectionInfo = MetaData
                .getMetadataDBConnectionInfo();
        DbGateway aGwy = new DbGateway();

        // Don't bother creating on nodes if we are in "manual" mode
        if (!isManual) {
            aGwy.createDbOnNodes(valueMap,
                    new NodeDBConnectionInfo[] { connectionInfo });
        } else {
            // Fill valueMap to be able to get connection
            aGwy.populateValueMap(connectionInfo, valueMap);
        }

        // Now we initialize the database if the user specified an
        // initialization
        // script.
        if (fileList.size() > 0 && !isManual) {
            try {
                aGwy.wait(1000);
            } catch (Exception e) {
            }
            for (int i = 0; i < fileList.size(); i++) {
                String currFile = (String) fileList.elementAt(i);

                valueMap.put("inputfile", currFile);
                try {
                    aGwy.execScriptOnNodes(valueMap,
                            new NodeDBConnectionInfo[] { connectionInfo });
                } catch (Exception e) {
                    System.out.println("Error executing script: "
                            + e.getMessage());
                    // continue
                    // try and execute on all, even if it failed on one.
                }
            }
        }

        conn = getConnection(aGwy);

        // Now, try and load up schema

        createSchema(aGwy);

        createInitialUser(osUsername, osPassword);

        // Due to Runtime.exec issues on Windows, explicitly exit
        System.exit(0);
    }

    private static void checkDriver(String driverString) {
        try {
            Class.forName(driverString);
        } catch (ClassNotFoundException cnfe) {
            throw new XDBServerException("Couln't find driver class: "
                    + driverString);
        }
    }

    /**
     * Gets and initializes connection to metadata database
     * 
     * @param aGateway
     * @return
     */
    private static Connection getConnection(DbGateway aGateway) {
        Connection connection = null;

        checkDriver(Property.get("xdb.metadata.jdbcdriver",
                Props.XDB_DEFAULT_JDBCDRIVER));

        String jdbcURL = Property.get("xdb.metadata.jdbcstring",
                Props.XDB_DEFAULT_JDBCSTRING);
        jdbcURL = ParseCmdLine.substitute(jdbcURL, valueMap);

        try {
            // First try just based on the jdbcURL
            connection = DriverManager.getConnection(jdbcURL);
            connection.setAutoCommit(false);
        } catch (SQLException se) {
            try {
                // Now use jdbcuser and jdbcpassword
                connection = DriverManager.getConnection(jdbcURL, Property.get(
                        "xdb.metadata.dbusername", Props.XDB_DEFAULT_DBUSER),
                        Property.get("xdb.metadata.dbpassword",
                                Props.XDB_DEFAULT_DBPASSWORD));
                connection.setAutoCommit(false);
            } catch (SQLException se2) {

                throw new XDBServerException(
                        "Could not connect to the database server", se2);
            }
        }
        if (connection == null) {
            throw new XDBServerException(
                    "Could not initialize connection.: Please check if the underlying"
                            + "database is running on the System");
        }

        return connection;

    }

    /**
     * Create database schema
     * 
     * @param aGwy
     */
    private static void createSchema(DbGateway aGwy) {
        int i = 0;
        try {
            java.sql.Statement stmt = conn.createStatement();
            for (i = 0; i < createStatements.length; i++) {
                stmt.executeUpdate(createStatements[i]);
                System.out.println("Executed Statement: "
                        + createStatements[i].toString());
            }
            conn.commit();
        } catch (java.sql.SQLException sqlex) {
            System.err.println("Could not execute: statement : "
                    + createStatements[i]);
            System.err.println(sqlex.getErrorCode() + " :  "
                    + sqlex.getMessage());
            try {
                conn.rollback();
            } catch (SQLException ex) {
                sqlex.setNextException(ex);
                throw new XDBServerException(
                        "The Rollback  also failed - Please check your connection after execution"
                                + "failuer", sqlex);
            }
        }
    }

    private static void createInitialUser(String userName, String password) {
        if (userName == null) {
            userName = System.getProperty("user.name");
            // Prompt for user name
            System.out.print("Enter username [" + userName + "]: ");
            // This reader should not be closed as it closes underlying stream
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    System.in));
            try {
                String newName = reader.readLine().trim();
                if (newName.length() > 0) {
                    userName = newName;
                }
            } catch (IOException ioe) {
                ioe.printStackTrace();
                throw new XDBServerException(
                        "I/O error while reading user input", ioe);
            }
        }
        while (password == null) {
            try {
                password = PasswordPrompt.getPassword("Enter new password for "
                        + userName + ": ");
                String confirm = PasswordPrompt
                        .getPassword("Confirm password for " + userName + ":  ");
                if (!password.equals(confirm)) {
                    password = null;
                    System.out.print("Passwords do not match");
                }
            } catch (IOException ioe) {
                ioe.printStackTrace();
                throw new XDBServerException(
                        "I/O error while reading user input", ioe);
            }
        }
        try {
            int userid;
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt
                    .executeQuery("SELECT max(userid) FROM xsysusers");
            try {
                rs.next();
                userid = rs.getInt(1) + 1;
            } finally {
                rs.close();
            }

            PreparedStatement ps = conn
                    .prepareStatement("INSERT INTO xsysusers"
                            + " (userid, username, userpwd, usertype)"
                            + " VALUES (?, ?, ?, ?)");
            ps.setInt(1, userid);
            ps.setString(2, userName);
            ps.setString(3, SysLogin.encryptPassword(password));
            ps.setString(4, SysLogin.USER_CLASS_DBA_STR);
            if (ps.executeUpdate() != 1) {
                XDBServerException ex = new XDBServerException(
                        "Failed to insert row into \"xsysusers\"");
                throw ex;
            }
            conn.commit();
            System.out.println("User " + userName + " is created");

        } catch (SQLException se) {
            System.err.println("Failed to create initial user");
            se.printStackTrace();
        }
    }

    // Parses arguments and ensures that they are valid
    // -------------------------------------------------------------------------
    /**
     * 
     * @param args
     */
    public static void processArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].compareTo("-i") == 0) {
                fileListString = args[++i];
            } else if (args[i].compareTo("-u") == 0) {
                osUsername = args[++i];
                valueMap.put("osusername", osUsername);
            } else if (args[i].compareTo("-p") == 0) {
                osPassword = args[++i];
                valueMap.put("ospassword", osPassword);
            } else if (args[i].compareTo("-m") == 0) {
                isManual = true;
            } else {
                throw new XDBServerException("unknown argument " + args[i]);
            }
        }
        // Now do some checking

        String tempStr = fileListString;
        int lastPos = 0;
        String currFile;
        if (tempStr != null) {
            while (!isManual && lastPos < tempStr.length()) {
                int pos = tempStr.indexOf(",", lastPos);
                if (pos >= 0) {
                    currFile = tempStr.substring(lastPos, pos);
                } else {
                    currFile = tempStr.substring(lastPos);
                }
                // Make sure it exists
                File aFile = new File(currFile);
                if (!aFile.exists()) {
                    throw new XDBServerException("File " + currFile
                            + " not found.");
                }
                fileList.addElement(currFile);
                if (pos < 0) {
                    break;
                }
                lastPos = pos + 1;
            }
        }
    }
}
