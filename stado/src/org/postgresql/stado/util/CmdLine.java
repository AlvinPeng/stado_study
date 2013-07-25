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
package org.postgresql.stado.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import org.postgresql.stado.misc.Timer;

import jline.ConsoleReader;


class CmdLine {
    static boolean echoMode = false;

    static String inFileName = null;

    static boolean formatOutput = true;

    static String delim = "|";

    static boolean printTrailingDelimiter;

    static boolean useTimer = false;

    private static void printUsageAndExit() {
        System.out
                .println("Parameters:  <connect> [-e] [-t] [-f inputfile]\n"
                        + "\twhere <connect> is -j jdbc:postgresql://<host>:<port>/<database>?user=<username>&password=<password>\n"
                        + "\tor [-h <host>] [-s <port>] -d <database> -u <user> [-p <password>]\n"
                        + "\t-h <host> : Host name or IP address where XDBServer is running. Default is localhost\n"
                        + "\t-s <port> : XDBServer's port. Default is 6453\n"
                        + "\t-d <database> : Name of database to connect to.\n"
                        + "\t-u <user>, -p <password> : Login to the database\n"
                        + "\t-e : echo mode. Echos any statements as it executes them\n"
                        + "\t-t : has effect of SET OUPUT NORMAL\n"
                        + "\t-f : input file to be executed, instead of interactive mode"
                        + "\t-b : buffer command line history"
                        + "\t-a : print trailing delimiter"
                        + "\t-z : print query time");
        System.exit(1);
    }

    /**
     *
     * @param args
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        Map<String, List<String>> m = null;
        boolean use_history = true;
        try {
            m = ParseArgs.parse(args, "jhsdupetfbaz");
        } catch (Exception e) {
            printUsageAndExit();
        }

        useTimer = m.containsKey("-z");
        echoMode = m.containsKey("-e");
        use_history = m.containsKey("-b");
        if (m.containsKey("-t")) {
            formatOutput = false;
            delim = ";";
        }
        inFileName = ParseArgs.getStrArg(m, "f");
        printTrailingDelimiter = m.containsKey("-a");

        String currentLine = "";
        String command = "";
        boolean batchFlag = false; // determines whether or not we are in batch
        // mode
        BufferedReader in = null;
        Timer aTimer = new Timer();
        String strTime = "";
        ConsoleReader reader = null;
        if (inFileName == null && use_history) {
            // in = new BufferedReader(new InputStreamReader(System.in));
            try {
                System.out.flush();
                reader = new ConsoleReader();

            } catch (NoClassDefFoundError e1) {
                use_history = false;
                System.out
                        .println("HISTORY COMMAND IS OFF (jline.jar is not found)");
                System.out.flush();
            }

        }

        try {
            Connection con = Util.connect(m);

            if (con == null) {
                throw new SQLException("Can not connect to server");
            }
            Statement stmt = con.createStatement();

            currentLine = "";

            PrintWriter out = new PrintWriter(System.out);

            /*
             * SET OUTPUT NORMAL SET OUTPUT TABLE If set to TABLE, will pad
             * leading spaces as follow: -> center: column headers ->
             * right-justified: numeric values -> left-justified: other data
             * types
             */

            /*
             * DatabaseMetaData aDMD = con.getMetaData(); ResultSet aRS =
             * aDMD.getColumns("DEVDB4",null,"nation",null); Util.dumpRs(aRS,
             * out, ";"); aRS.close();
             */

            if (inFileName != null) {
                FileReader fr = new FileReader(inFileName);
                in = new BufferedReader(fr);
            } else {
                if (use_history) {
                    reader = new ConsoleReader();
                } else {
                    in = new BufferedReader(new InputStreamReader(System.in), 65536);
                }
            }

            while (inFileName == null || inFileName != null
                    && currentLine != null) {
                command = "";
                currentLine = "";
                System.out.println("");

                // Wait until they have entered a ";"
                // Note, we assume it ends in ";".
                // Need to change this if in middle of line.
                while (currentLine != null
                        && (currentLine.indexOf(";") < 0
                                || currentLine.startsWith("#") || currentLine
                                .startsWith("--"))) {
                    // don't display prompt if taking input from file
                    if (inFileName == null) {
                        System.out.print("Stado -> ");
                        if (use_history) {
                            currentLine = reader.readLine();
                        } else {
                            currentLine = in.readLine();
                        }
                    } else {
                        currentLine = in.readLine();
                    }

                    // Allow # and -- as start of line comment characters
                    if (currentLine != null) {
                        if (currentLine.startsWith("#")
                                || currentLine.startsWith("--")) {
                            continue;
                        }
                    }

                    if (command.length() > 0) {
                        command += " ";
                    }

                    command += currentLine;
                }

                if (currentLine == null) {
                    break;
                }
                command = command.trim();

                if (command.length() >= 4
                        && command.toUpperCase().trim().substring(0, 4)
                                .compareTo("EXIT") == 0) {
                    break;
                }

                // Echo the command if necessary
                if (echoMode) {
                    System.out.println(command);
                }

                try {
                    if (command.toUpperCase().matches(
                            "SET[ ]*OUTPUT[ ]*TABLE.*")) {
                        formatOutput = true;
                        delim = "|";
                    } else if (command.toUpperCase().matches(
                            "SET[ ]*OUTPUT[ ]*NORMAL.*")) {
                        formatOutput = false;
                        delim = ";";
                    } else if (command.toUpperCase()
                            .matches("BEGIN[ ]*BATCH.*")) {
                        batchFlag = true;
                    } else if (command.toUpperCase().matches(
                            "EXECUTE[ ]*BATCH.*")) {
                        batchFlag = false;
                        if (useTimer) {
                            aTimer = new Timer();
                        }
                        int[] batchResults = stmt.executeBatch();
                        if (useTimer) {
                            aTimer.stopTimer();
                        }
                        System.out.println("Batch Results:");
                        for (int i = 0; i < batchResults.length; i++) {
                            System.out.println(i + 1 + ": " + batchResults[i]);
                        }
                        if (useTimer) {
                            System.out.println(" Response time: "
                                    + aTimer.getDuration());
                        }
                    } else if (batchFlag) {
                        stmt.addBatch(command);
                    } else if (command.toUpperCase().matches("BEGIN.*")) {
                        con.setAutoCommit(false);
                    } else if (command.toUpperCase().matches("COMMIT.*")
                            || command.toUpperCase().matches("END.*")) {
                        con.commit();
                        con.setAutoCommit(true);
                    } else if (command.toUpperCase().matches("ROLLBACK.*")) {
                        con.rollback();
                        con.setAutoCommit(true);
                    } else {
                        if (useTimer) {
                            aTimer = new Timer();
                        }

                        stmt.setFetchSize(1000);
                        boolean nextIsResultSet = stmt.execute(command);

                        if (useTimer) {
                            aTimer.stopTimer();
                            strTime = " Response time: " + aTimer.getDuration();
                            aTimer.startTimer();
                        }
                        boolean suppressTrailingOK = false;
                        while (true) {
                            if (nextIsResultSet) {
                                ResultSet rs = stmt.getResultSet();
                                try {
                                    if (formatOutput) {
                                        Util.dumpRsTable(rs, out, delim);
                                    } else {
                                        Util.dumpRs(rs, out, delim,
                                                printTrailingDelimiter);
                                    }
                                } finally {
                                    rs.close();
                                }
                            } else {
                                int numRows = stmt.getUpdateCount();
                                if (numRows > 0) {
                                    System.out.println(numRows
                                            + " row(s) affected");
                                } else {
                                    if (numRows == 0 || !suppressTrailingOK) {
                                        System.out.println("OK");
                                    }
                                    if (numRows < 0) {
                                        break;
                                    }
                                }
                            }
                            nextIsResultSet = stmt.getMoreResults();
                            suppressTrailingOK = true;
                        }

                        if (useTimer) {
                            aTimer.stopTimer();
                            System.out.println(strTime + "  Total time: "
                                    + aTimer.getDuration());
                        }
                    }
                } catch (SQLException e) {
                    // ignore the exception in case it's socket reset error
                    // ("08006") that is expected when
                    // the Stado Server goes down
                    if (!e.getSQLState().equals("08006")) {
                        System.out.println("SQLException: " + e.getMessage());
                        SQLException next = e.getNextException();
                        while (next != null) {
                            System.out.println("Next SQLException: "
                                    + next.getMessage());
                            next = next.getNextException();
                        }
                    } else {
                        System.out.println("Connection error.");
                    }
                } catch (Exception e) {
                    System.out.println("Exception: " + e.getMessage());
                }
            }

            // disconnect from server
            con.close();

        } catch (Exception e) {
            System.out.println("Exception: " + e.getMessage());
            e.printStackTrace();
        }
        // System.out.println ("Exiting cmdline ...");
    } // method main

} // class cmdline

