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
 * CreateDb.java
 *
 *  
 */

/****************************************************************************
 *
 * createdb -d dbname -u dbmser -p dbmpassword
 *          -n nodelist
 *          -o owner
 *          -m
 *
 * nodelist is comma separated, eg, 1,2,4
 * -m sets it to "manual" mode - it will make entries in the MetaData db
 *    but will not try and actually create the database on the nodes.
 *
 ****************************************************************************/
package org.postgresql.stado.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.postgresql.stado.common.util.Props;


/**
 * 
 * 
 */
public class CreateDb {

    /**
     * 
     * 
     * 
     * @param errorMsg
     * 
     */
    private static void terminate(String errorMsg) {
        System.err.println(errorMsg);
        System.err
                .println("Parameters:  <connect> -d <database> [-o <owner>] -n <nodelist> [-m]\n"
                        + "\twhere <connect> is -j jdbc:postgresql://<host>:<port>/"
                        + Props.XDB_ADMIN_DATABASE
                        + "?user=<username>&password=<password>\n"
                        + "\tor [-h <host>] [-s <port>] -u <user> [-p <password>]\n"
                        + "\t-h <host> : Host name or IP address where XDBServer is running. Default is localhost\n"
                        + "\t-s <port> : XDBServer's port. Default is 6453\n"
                        + "\t-u <user>, -p <password> : Login to the database\n"
                        + "\t-d <database> : Name of database to create.\n"
                        + "\t-o <owner> : Name of database owner.\n"
                        + "\t-n <nodelist> : Comma or space separated list of numbers of nodes where the database will be created. Nodes must be up and running.\n"
                        + "\t-m : Manual mode. Do not create node databases. Databases must exist if -m is specified.\n"
                // + "\t-i <initscript> : Script to run against newly created
                // node database. Has no effect in manual mode."
                );
        System.exit(1);
    }

    /**
     * 
     * @param args
     */

    public static void main(String[] args) {
        Connection con = null;
        Statement stmt = null;

        try {
            Map<String, List<String>> m = ParseArgs.parse(args, "jhsdoupnm");
            String databaseToCreate = ParseArgs.getStrArg(m, "-d");
            StringBuffer nodesCmd = new StringBuffer();
            List<String> nodeList = m.get("-n");

            if (nodeList == null) {
                terminate("No nodes specified");
            }

            for (String nodes : nodeList) {
                StringTokenizer st = new StringTokenizer(nodes, ",");

                while (st.hasMoreTokens()) {
                    String nodeID = st.nextToken();
                    // check integer format
                    try {
                        Integer.parseInt(nodeID);
                    } catch (NumberFormatException nfe) {
                        terminate("The node id should be an integer.");
                    }

                    if (!nodesCmd.toString().equals("")) {
                        nodesCmd.append(",");
                    }

                    nodesCmd.append(nodeID);
                }
            }

            if (nodesCmd.length() == 0) {
                terminate("No Nodes specified");
            }

            String owner = ParseArgs.getStrArg(m, "-o");
            boolean manual = m.containsKey("-m");

            con = Util.connect(m, true);
            stmt = con.createStatement();
            String createDBstmt = "CREATE DATABASE " + databaseToCreate;

            // If no owner is specified, the current user is used as the owner
            if (owner != null) {
                createDBstmt += " OWNER " + owner;
            }

            if (manual) {
                createDBstmt += " MANUAL";
            }

            createDBstmt += " ON NODES " + nodesCmd.toString();
            stmt.execute(createDBstmt);

        } catch (Exception e) {
            terminate(e.getMessage());
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }

                if (con != null) {
                    con.close();
                }
            } catch (SQLException sqle) {
            }
        }

        System.out.println("OK");
        System.exit(0);
    }
}
