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
 *
 */
package org.postgresql.stado.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import org.postgresql.stado.common.util.Props;


/**
 *
 */
public class XdbShutdown {
    /**
     *
     * @param errorMsg
     */
    private static void terminate(String errorMsg) {
        System.err.println(errorMsg);
        System.err
                .println("Parameters: <connect> [-f]\n"
                        + "\twhere <connect> is -j jdbc:postgresql://<host>:<port>/"
                        + Props.XDB_ADMIN_DATABASE
                        + "?user=<username>&password=<password>\n"
                        + "\tor [-h <host>] [-s <port>] -u <user> [-p <password>]\n"
                        + "\t-h <host> : Host name or IP address where XDBServer is running. Default is localhost\n"
                        + "\t-s <port> : XDBServer's port. Default is 6453\n"
                        + "\t-u <user>, -p <password> : Login to the server\n"
                        + "\t-f : Force mode. Try and shutdown server even if databases are online.");
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
            Map<String, List<String>> m = ParseArgs.parse(args, "jhsupf");
            boolean force = m.containsKey("-f");
            con = Util.connect(m, true);
            stmt = con.createStatement();
            String shutdownStmt = "SHUTDOWN";

            if (force) {
                shutdownStmt += " FORCE";
            }

            try {
                stmt.execute(shutdownStmt);
            } catch (SQLException e) {
                // I/O Error is expected response - server shuts down and closes connection
                if (!"08006".equals(e.getSQLState())) {
                    throw e;
                }
            }

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

        System.out.println("Server is down.");
        System.exit(0);
    }
}
