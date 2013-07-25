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
 * execdb.java
 *
 *  
 *
 * usage: 
 *  gs-execdb -d dbname -c command -u dbusername -p dbpassword -n nodelist
 */

package org.postgresql.stado.util;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

/**
 * 
 * 
 */
public class ExecDb {

    private static Connection oConn;

    private static String commandString;

    /**
     * 
     * @param errorMsg
     */
    private static void terminate(String errorMsg) {
        System.err.println(errorMsg);
        System.err
                .println("Parameters:  <connect> -c <command>\n"
                        + "\twhere <connect> is -j jdbc:postgresql://<host>:<port>/<database>?user=<username>&password=<password>\n"
                        + "\tor [-h <host>] [-s <port>] -d <database> -u <user> [-p <password>]\n"
                        + "\t-h <host> : Host name or IP address where XDBServer is running. Default is localhost\n"
                        + "\t-s <port> : XDBServer's port. Default is 6453\n"
                        + "\t-d <database> : Name of target database.\n"
                        + "\t-u <user>, -p <password> : Login to the database\n"
                        + "\t-c <command> : Command to be executed.");
        System.exit(1);
    }

    /**
     * @param args
     *                the command line arguments
     */
    public static void main(String[] args) {
        try {
            Map<String, List<String>> m = ParseArgs.parse(args, "jhsdupc");
            commandString = ParseArgs.getStrArg(m, "-c");
            if (commandString == null || commandString.length() == 0) {
                terminate("No command specified");
            }
            oConn = Util.connect(m, false);
        } catch (Exception e) {
            terminate(e.getMessage());
        }

        try {
            Statement stmt = oConn.createStatement();
            String command = "EXEC DIRECT ON ALL '" + commandString + "'";
            PrintWriter writer = new PrintWriter(System.out);
            boolean resultType = stmt.execute(command);
            for (;;) {
                if (resultType) {
                    Util.dumpRsTable(stmt.getResultSet(), writer, "|");
                } else {
                    int updCount = stmt.getUpdateCount();
                    if (updCount == -1) {
                        break;
                    } else if (updCount == Statement.SUCCESS_NO_INFO) {
                        writer.println("OK");
                    } else {
                        writer.println(updCount + " row(s) affected");
                    }
                }
                resultType = stmt.getMoreResults();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }
}
