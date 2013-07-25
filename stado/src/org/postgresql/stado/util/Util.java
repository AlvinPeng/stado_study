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

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.engine.io.DataTypes;
import org.postgresql.stado.engine.io.MessageTypes;


/**
 * Some convenient utility methods
 *
 *
 */
public class Util implements MessageTypes {

    private static final int SAMPLE_SIZE = 100; // perhaps make this

    // configurable?

    /**
     * Print the given string to the out stream specified. If the input string
     * is shorter than the defined columnLength, pre-pad the string with white
     * spaces.
     *
     * @param out
     * @param str
     * @param columnLength
     * @param delimiter
     * @param padType
     *                1) right-justified; 2)left-justified; 3) centered;
     *                otherwise it's no padding
     */
    private static void padPrint(PrintWriter out, String str, int columnLength,
            String delimiter, int padType) {
        int diff = columnLength - str.length();
        while (padType == 1 && diff-- > 0) {
            out.print(" ");
        }
        int remain = 0;
        if (diff > 0 && padType == 3) {
            int half = diff / 2;
            if (half == 0) {
                half = 1;
            }
            remain = diff - half;
            while (half-- > 0) {
                out.print(" ");
            }
        }

        out.print(" ");
        out.print(str);

        while (padType == 2 && diff-- > 0) {
            out.print(" ");
        }

        while (padType == 3 && remain-- > 0) {
            out.print(" ");
        }

        out.print(" ");
        out.print(delimiter);

    }

    /**
     *
     * @param out
     * @param length
     * @param delimiter
     */
    private static void printTableLine(PrintWriter out, int length,
            String delimiter) {
        out.print("+");
        while (length-- > 0) {
            out.print("-");
        }
        out.println("+");
    }

    /**
     *
     * @param rs
     * @param out
     * @param delimiter
     * @throws java.sql.SQLException
     */
    public static void dumpRsTable(ResultSet rs, PrintWriter out,
            String delimiter) throws SQLException {
        if (!rs.next()) {
            out.println("no rows to display");
            out.flush();
            return;
        }

        int lineLength = 0; // estimate line length

        java.sql.ResultSetMetaData meta = rs.getMetaData();
        int c = meta.getColumnCount();

        // estimate the length of each column by looping thru the first few
        // records.
        ArrayList<List<String>> firstRows = new ArrayList<List<String>>(
                SAMPLE_SIZE);
        int[] columnSize = new int[c];
        int count = 0;
        String tmpStr = null;
        do {
            ArrayList<String> list = new ArrayList<String>(c);
            for (int i = 0; i < c; i++) {
                String value = rs.getString(i + 1);
                tmpStr = value == null ? "" : value.toString();
                columnSize[i] = Math.max(columnSize[i], tmpStr.length());
                list.add(tmpStr);
            }
            firstRows.add(list);
        } while (++count < SAMPLE_SIZE && rs.next());

        int[] types = new int[c];

        // you can have the header names longer than the data itself
        for (int i = 0; i < c; i++) {
            types[i] = meta.getColumnType(i + 1);
            // out.print(meta.getColumnName(i+1).toUpperCase() + delimiter);
            columnSize[i] = Math.max(columnSize[i], meta.getColumnLabel(i + 1)
                    .length());
            lineLength += columnSize[i] + 2; // add 2 since we pad data in
            // column
        }
        lineLength += c - 1;

        printTableLine(out, lineLength, delimiter);
        out.print(delimiter);

        // now print the column header
        for (int i = 0; i < c; i++) {
            padPrint(out, meta.getColumnLabel(i + 1), columnSize[i], delimiter, 3);
        }
        out.println();

        printTableLine(out, lineLength, delimiter);

        // out.println();

        int rowsCnt = firstRows.size();
        for (int idx = 0; idx < rowsCnt; idx++) {

            // printTableLine(out, lineLength, delimiter);
            out.print(delimiter);

            List<String> list = firstRows.get(idx);
            for (int i = 0; i < c; i++) {
                padPrint(out, list.get(i), columnSize[i], delimiter, (DataTypes
                        .isNumeric(types[i]) ? 1 : 2));
            }
            out.println();
        }
        out.flush();

        // write rows
        while (rs.next()) {

            // printTableLine(out, lineLength, delimiter);
            out.print(delimiter);

            for (int i = 0; i < c; i++) {
                String value = rs.getString(i + 1);
                padPrint(out, value == null ? "" : value.toString(),
                        columnSize[i], delimiter, (DataTypes
                                .isNumeric(types[i]) ? 1 : 2));
            }
            out.println();
            out.flush();
            rowsCnt++;
        }
        printTableLine(out, lineLength, delimiter);
        out.println(rowsCnt + " row(s).");
        out.flush();
    }

    /**
     *
     * @param rs
     * @param out
     * @param delimiter
     * @param printTrailingDelimiter
     * @throws java.sql.SQLException
     */
    public static void dumpRs(ResultSet rs, PrintWriter out, String delimiter,
            boolean printTrailingDelimiter) throws SQLException {

        if (!rs.next()) {
            out.println("no rows to display");
            out.flush();
            return;
        }

        java.sql.ResultSetMetaData meta = rs.getMetaData();
        int c = meta.getColumnCount();
        int types[] = new int[c];

        for (int i = 0; i < c; i++) {
            types[i] = meta.getColumnType(i + 1);
            out.print(meta.getColumnLabel(i + 1).toUpperCase() + delimiter);
            // out.print(meta.getColumnName(i+1) + " - " +
            // meta.getColumnClassName(i+1) + delimiter);
        }

        out.println();
        int rowsCnt = 0;

        // java.text.DecimalFormat aDF = new java.text.DecimalFormat();
        // aDF.setGroupingUsed(false);

        // write rows
        do {
            for (int i = 0; i < c; i++) {
                String value = rs.getString(i + 1);
                out.print((value == null ? "null" : value.toString().trim()));
                if (printTrailingDelimiter || i < c - 1) {
                    out.print(delimiter);
                }
            }
            out.println();
            out.flush();
            rowsCnt++;
        } while (rs.next());
        out.println(rowsCnt + " rows dumped");
        out.flush();
    }

    public static Connection connect(Map<String, List<String>> parameters)
            throws SQLException {
        return connect(parameters, false);
    }

    /**
     *
     * @param parameters
     * @param mode
     * @throws java.sql.SQLException
     * @return
     */
    public static Connection connect(Map<String, List<String>> parameters,
            boolean adminMode) throws SQLException {
        String url;
        // Alternates to URL
        String host = "localhost";
        int port = 6453;
        String database = null;
        String username = null;
        String password = null;

        Connection con = null;

        url = ParseArgs.getStrArg(parameters, "j");
        if (url == null) {
            String s = ParseArgs.getStrArg(parameters, "h");
            if (s != null) {
                host = s;
            }
            s = ParseArgs.getStrArg(parameters, "s");
            if (s != null) {
                try {
                    port = Integer.parseInt(s);
                } catch (NumberFormatException ignore) {
                }
            }
            if (!adminMode) {
                database = ParseArgs.getStrArg(parameters, "d");
                if (database == null) {
                    System.out.println("Please, specify database");
                    throw new SQLException("Database name is not specified");
                }
            }
            username = ParseArgs.getStrArg(parameters, "u");
            if (username == null) {
                System.out.println("Please, specify user name");
                throw new SQLException("User name is not specified");
            }
            password = ParseArgs.getStrArg(parameters, "p");
            url = "jdbc:postgresql://" + host + ":" + port + "/"
                    + (adminMode ? Props.XDB_ADMIN_DATABASE : database);
        }

        try {
            Class.forName("org.postgresql.driver.Driver");
        } catch (ClassNotFoundException cnfe) {
            throw new SQLException("JDBC driver is not available");
        }
        if (username == null) {
            con = DriverManager.getConnection(url);
        } else if (password == null) {
            for (int i = 0; i < 3; i++) {
                try {
                    try {
                        password = PasswordPrompt.getPassword("password: ");
                    } catch (IOException ioe) {
                        throw new SQLException("Can not get password: "
                                + ioe.getMessage());
                    }
                    con = DriverManager.getConnection(url, username, password);
                    break;
                } catch (SQLException e) {
                    if (!"Invalid login".equals(e.getMessage())) {
                        throw e;
                    }
                }
            }
        } else {
            con = DriverManager.getConnection(url, username, password);
        }
        if (con == null) {
            throw new SQLException("Can not connect to server");
        }
        return con;
    }

}
