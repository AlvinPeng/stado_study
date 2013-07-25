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
 * xdbimpex.java
 *
 *
 */

package org.postgresql.stado.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.StringTokenizer;

import org.postgresql.stado.common.util.OutputFormatter;


/*
 * XDBImport -f formatfile
 */
public class XdbImpEx {
    private String dbTable;

    private String dbDriverClass;

    /*
     * private String dbDriver; private String dbHost; private String dbUser;
     * private String dbPass;
     */
    private String jdbcUrl;

    private Statement dbStatement;

    private Connection dbConnection;

    static String sFieldSep;

    static String nullValue;

    static String sTerminator;

    private StringBuffer buffer = null;

    private String sImportFile;

    private String sExportFile;

    private String sErrorFile;

    private String sExtractQuery;

    private String formatFileName;

    private String sPosFormatted;

    private String quoted;

    private int startingLine = 1;

    private int endLine = 0;

    private int sMaxErrors = 1;

    private int sCommitInt = 1000;

    private int notCommitted = 0;

    private int numErrors = 0;

    private int rowsProcessed = 0;

    private int notifyInterval = 1000000;

    private boolean overWrite = false;

    private boolean lock = false;

    private boolean silent = false;

    private boolean ignore = false;

    // instead of outputting nothing for null, output a space.
    static boolean trimTrailingSpaces = false;

    // private int DISPLAY_COUNT = 1000;

    private static String stmtTerminator = "";

    private String lockClass;

    /** Input stream for reading import file */
    private BufferedReader in = null;

    /** Output stream for writing exported file */
    private OutputStream outStream = null;

    /** Output stream for writing data failed to execute */
    private BufferedWriter errorStream = null;

    // private int numCols;
    // private String[] colNames;
    // private int[] colTypes;
    private ColumnDef[] columns;

    static boolean hasTrailingDelimiter = false;

    /**
     * Main method.
     *
     * @param argv
     */
    public static void main(String argv[]) {
        new XdbImpEx(argv);
    }

    /**
     * Constructor
     *
     * @param argv
     */
    public XdbImpEx(String argv[]) {

        parseCommandLine(argv);
        if (notifyInterval == 0) {
            notifyInterval = sCommitInt;
        }
        if (notifyInterval == 0) {
            notifyInterval = 1000;
        }

        if (formatFileName != null) {
            try {
                loadFormatFile(new File(formatFileName));
            } catch (IOException e) {
                exitWithError(e.toString());
            }
        }
        checkInputParameters();
        instantiateDrivers();
        try {
            openConnections();
            if (sImportFile != null) {
                importData();
            } else if (sExportFile != null) {
                exportData();
            }
            closeConnections();
        } catch (SQLException e) {
            exitWithError("SQL Error: " + e.getMessage());
        }
    }

    /**
     * Parse the command line inputs and save them
     *
     * @param argv
     */
    private void parseCommandLine(String argv[]) {
        String sArg = "";
        String sNextArg = null;
        char cOption;

        try {
            for (int i = 0; i < argv.length; i++) {
                sArg = argv[i];
                cOption = sArg.charAt(1);
                switch (cOption) {
                case 'a':
                case 'w':
                case 'g':
                case 'h':
                case 'P':
                case 'T':
                case 'l':
                    break;
                default:
                    if (sArg.charAt(0) != '-') {
                        badArgs();
                    }
                    if (sArg.length() > 2) {
                        sNextArg = sArg.substring(2);
                    } else {
                        sNextArg = argv[++i];
                    }
                }
                switch (cOption) {
                case 'a':
                    hasTrailingDelimiter = true;
                    break;

                case 'b':
                    lockClass = sNextArg;
                    break;

                case 'c':
                    sCommitInt = Integer.parseInt(sNextArg);
                    break;

                case 'n':
                    notifyInterval = Integer.parseInt(sNextArg);
                    break;

                case 'd':
                    sFieldSep = sNextArg;
                    break;

                case 'e':
                    endLine = Integer.parseInt(sNextArg);
                    break;

                case 'f':
                    formatFileName = sNextArg;
                    break;

                case 'g':
                    ignore = true;
                    break;

                case 'h':
                    silent = true;
                    break;

                case 'i':
                    sImportFile = sNextArg;
                    break;

                case 'j':
                    jdbcUrl = sNextArg;
                    break;

                case 'l':
                    lock = true;
                    break;

                case 'm':
                    sMaxErrors = Integer.parseInt(sNextArg);
                    break;

                case 'p':
                    sPosFormatted = sNextArg;
                    break;

                case 'q':
                    quoted = sNextArg;
                    break;

                case 'r':
                    sErrorFile = sNextArg;
                    break;

                case 's':
                    startingLine = Integer.parseInt(sNextArg);
                    break;

                case 't':
                    dbTable = sNextArg;
                    break;

                case 'w':
                    overWrite = true;
                    break;

                case 'x':
                    sExportFile = sNextArg;
                    break;

                case 'y':
                    sExtractQuery = sNextArg;
                    break;

                case 'z':
                    sTerminator = sNextArg;
                    break;

                case 'C':
                    dbDriverClass = sNextArg;
                    break;

                case 'N':
                    nullValue = sNextArg;
                    break;

                case 'T':
                    trimTrailingSpaces = true;
                    break;

                // don't enable this yet
                // case 'P':
                // parallelMode = true;
                // break;

                /*
                 * case 'H': dbHost = sNextArg; break;
                 *
                 * case 'R': dbDriver = sNextArg; break;
                 *
                 * case 'D': dbName = sNextArg; break;
                 *
                 * case 'U': dbUser = sNextArg; break;
                 *
                 * case 'P': dbPass = sNextArg; break;
                 */
                default:
                    // System.out.println ("Poorly formed command line
                    // arguments.");
                    System.err.println("Poorly formed command line arguments.");
                    badArgs();

                }
            }
        } catch (Exception e) {
            // System.out.println ("Error processing command line arguments.");
            System.err.println("Error processing command line arguments.");
            badArgs();
        }
    }

    /**
     * Check for the input paramters
     *
     */
    private void checkInputParameters() {
        if (sFieldSep == null) {
            sFieldSep = "\t";
        }

        if (nullValue == null) {
            nullValue = "\\N";
        }

        /*
         * if (dbHost == null) { System.out.println ("Error: no database host
         * specified"); badArgs(); }
         *
         * if (dbDriver == null) { dbDriver = "jdbc:xdb"; System.out.println
         * ("No database driver specified."+ " Using default jdbc:xdb "); }
         *
         * if (dbUser == null) { System.out.println ("Error: no database user
         * name specified"); badArgs(); }
         *
         * if (dbPass == null) { System.out.println ("Error: no database user
         * password specified"); badArgs(); }
         *
         * if (dbName == null) { System.out.println ("Error: no database name
         * specified"); badArgs(); }
         */

        if (dbDriverClass == null) {
            dbDriverClass = "org.postgresql.driver.Driver";
            // System.out.println ("No database driver class specified ");
        }

        if (dbTable == null && sExtractQuery == null) {
            // System.out.println ("Error: no database table specified");
            System.err.println("Error: no database table specified");
            badArgs();
        }

        // if (sImportFile == null && sExportFile == null)
        // {
        // System.out.println();
        // System.out.println ("Error: no import/ export file specified");
        // badArgs();
        // }

        if (sImportFile != null && sExportFile != null) {
            // System.out.println();
            System.err.println();
            // System.out.println
            System.err
                    .println("import/ export operations are mutually exclusive");
            badArgs();
        }

        if (sExtractQuery != null && dbTable != null) {
            // System.out.println();
            // System.out.println("Extract query and table options"+
            // " are mutually exclusive ");
            System.err.println();
            System.err.println("Extract query and table options"
                    + " are mutually exclusive ");

            badArgs();
        }

        if (lock) {
            if (lockClass == null) {
                exitWithError("Error: Lock Class should be provided for "
                        + " -lock option");
            }
        }
    }

    /**
     * Exits the program with error message
     *
     */
    private void badArgs() {
        System.err.println();
        System.err.println("\tUsage to Import: gs-impex -f formatFile"
                + " -i importfile");
        System.err.println("\tUsage to Export: gs-impex -f formatFile"
                + " -x exportfile");
        System.err.println();

        System.exit(1);
    }

    /**
     * Loads a format file.
     *
     * @param format
     *                file to load
     * @throws IOException
     */
    private void loadFormatFile(File format) throws IOException {
        if (!format.exists()) {
            exitWithError("Format file does not exist");
            ;
        }
        Properties p = new Properties();
        p.load(new FileInputStream(format));

        // Database driver class.
        if (dbDriverClass == null) {
            dbDriverClass = getPropertyTrim(p, "DRIVERCLASS");
        }

        if (jdbcUrl == null) {
            jdbcUrl = getPropertyTrim(p, "JDBC_URL");
            /*
             * if ( dbHost == null ) dbHost = getPropertyTrim(p,"HOST"); //
             * Database driver. Default is jdbc:xdb if ( dbDriver == null )
             * dbDriver = getPropertyTrim(p,"DRIVER");
             *
             * if ( dbName == null ) dbName = getPropertyTrim(p,"DATABASE"); if (
             * dbUser == null ) dbUser = getPropertyTrim(p,"USER"); if ( dbPass ==
             * null ) dbPass = getPropertyTrim(p,"PASSWORD");
             */
        }

        if (sImportFile == null) {
            sImportFile = getPropertyTrim(p, "INFILE");
        }
        if (dbTable == null) {
            dbTable = getPropertyTrim(p, "TARGET");
        }
        if (sExportFile == null) {
            sExportFile = getPropertyTrim(p, "OUTFILE");
        }
        if (sErrorFile == null) {
            sErrorFile = getPropertyTrim(p, "DATA_ERROR_FILE");
        }
        if (quoted == null) {
            quoted = getPropertyTrim(p, "QUOTED");
        }
        if (sFieldSep == null) {
            sFieldSep = getPropertyTrim(p, "DELIMITER");
        }
        if (sTerminator == null) {
            sTerminator = getPropertyTrim(p, "TERMINATOR");
        }
        if (sPosFormatted == null) {
            sPosFormatted = getPropertyTrim(p, "POSITION_FORMATTED");
        }
        if (sExtractQuery == null) {
            sExtractQuery = getPropertyTrim(p, "EXTRACT");
        }

        if (startingLine == 1) {
            if (getPropertyTrim(p, "START_LINE") != null) {
                startingLine = Integer
                        .parseInt(getPropertyTrim(p, "START_LINE"));
            }
        }
        if (endLine == 0) {
            if (getPropertyTrim(p, "END_LINE") != null) {
                endLine = Integer.parseInt(getPropertyTrim(p, "END_LINE"));
            }
        }
        if (sMaxErrors == 1) {
            if (getPropertyTrim(p, "MAX_ERRORS") != null) {
                sMaxErrors = Integer.parseInt(getPropertyTrim(p, "MAX_ERRORS"));
            }
        }
        if (!(sCommitInt > 1)) {
            if (getPropertyTrim(p, "COMMIT_INTERVAL") != null) {
                sCommitInt = Integer.parseInt(getPropertyTrim(p,
                        "COMMIT_INTERVAL"));
            }
        }

        if (getPropertyTrim(p, "OVERWRITING") != null) {
            int val = Integer.parseInt(getPropertyTrim(p, "OVERWRITING"));
            if (val == 1) {
                overWrite = true;
            }
        }
        if (getPropertyTrim(p, "IGNORE") != null) {
            int val = Integer.parseInt(getPropertyTrim(p, "IGNORE"));
            if (val == 1) {
                ignore = true;
            }
        }
        if (getPropertyTrim(p, "SILENT") != null) {
            int val = Integer.parseInt(getPropertyTrim(p, "SILENT"));
            if (val == 1) {
                silent = true;
            }
        }
        if (getPropertyTrim(p, "TRIM_TRAILING_SPACES") != null) {
            int val = Integer.parseInt(getPropertyTrim(p,
                    "TRIM_TRAILING_SPACES"));
            if (val == 1) {
                trimTrailingSpaces = true;
            }
        }
        if (getPropertyTrim(p, "ADDED_TRAILING_DELIMITER") != null) {
            int val = Integer.parseInt(getPropertyTrim(p,
                    "ADDED_TRAILING_DELIMITER"));
            if (val == 1) {
                hasTrailingDelimiter = true;
            }
        }
        if (getPropertyTrim(p, "LOCK") != null) {
            int val = Integer.parseInt(getPropertyTrim(p, "LOCK"));
            if (val == 1) {
                lock = true;
            }
        }
        if (lockClass == null) {
            lockClass = getPropertyTrim(p, "LOCK_CLASS");
        }
    }

    /**
     * Trim the property result we obtain
     *
     * @param p
     * @param property
     * @return
     */
    private String getPropertyTrim(Properties p, String property) {
        String result = p.getProperty(property);

        if (result != null) {
            result = result.trim();
        }

        return result;
    }

    /**
     * Instantiates the necessary database drivers.
     *
     */
    private void instantiateDrivers() {
        try {
            Class.forName(dbDriverClass);
        } catch (ClassNotFoundException ce) {
            exitWithError("Error: could not find the database driver class.");
        } catch (Exception e) {
            exitWithError("Error: could not create the database driver class");
        }

    }

    /**
     * Opens the connections.
     *
     * @throws SQLException
     */
    private void openConnections() throws SQLException {

        // dbDriver = dbDriver+"://"+dbHost+"/"+dbName;
        dbConnection = DriverManager.getConnection(jdbcUrl); // , dbUser,
        // dbPass);
        dbConnection.setAutoCommit(sCommitInt <= 0);
        dbStatement = dbConnection.createStatement();
        if (lock) {
            lockTable();
        }
    }

    /**
     * Closes the connections.
     *
     * @throws SQLException
     */
    private void closeConnections() throws SQLException {
        if (lock) {
            unlockTable();
        }

        dbStatement.close();
        dbConnection.close();
    }

    /**
     * lock the database table.
     *
     */
    private void lockTable() {
        try {
            Class<?> classDefinition = Class.forName(lockClass);
            LockUnlockTable ll = (LockUnlockTable) classDefinition.newInstance();
            String query = ll.getLockString(dbTable);
            dbStatement.execute(query);
        } catch (Exception e) {
            exitWithError(e.getMessage());
        }
    }

    /**
     * unlock the database table.
     *
     * @return void
     */
    private void unlockTable() {
        try {
            Class<?> classDefinition = Class.forName(lockClass);
            LockUnlockTable ll = (LockUnlockTable) classDefinition.newInstance();
            String query = ll.getUnlockString(dbTable);
            dbStatement.execute(query);
        } catch (InstantiationException e) {
            exitWithError(e.getMessage());
        } catch (ClassNotFoundException ce) {
            exitWithError(ce.getMessage());
        } catch (Exception e) {
            exitWithError(e.getMessage());
        }
    }

    /**
     * Imports flat file to Database.
     *
     */
    private void importData() {
        if (sTerminator == null) {
            sTerminator = "\n";
        }
        String line = null;
        int linesRead = 0;
        rowsProcessed = 0;

        if (startingLine > endLine) {
            if (endLine != 0) {
                exitWithError("END_LINE option has smaller value"
                        + " than the START_LINE option");
            }
        }
        try {
            System.out.println("[" + new Date() + "] Loading: " + sImportFile);
            processResultSet();
            // open text file
            if (sImportFile == null || sImportFile.length() == 0) {
                in = new BufferedReader(new InputStreamReader(System.in));
            } else {
                File f = new File(sImportFile);
                if (!f.exists()) {
                    exitWithError("Data Import File does not exist...");
                }
                in = new BufferedReader(new FileReader(sImportFile));
            }

            if (sErrorFile == null) {
                sErrorFile = "data_error_file";
            }

            errorStream = new BufferedWriter(new FileWriter(sErrorFile));

            // first, skip over any lines,
            // in case restarting after a failure
            if (startingLine > 1) {
                for (int i = 1; i < startingLine; i++) {
                    // get next line
                    line = readLine();
                    if (line == null) {
                        break;
                    }
                    linesRead++;
                }
                if (linesRead + 1 < startingLine) {
                    exitWithError("Number of data lines in the import file"
                            + " less than the -s option given");
                }
            }

            line = readLine();
            String columnName[] = new String[columns.length];
            if (sPosFormatted == null) {
                for (int j = 0; j < columnName.length; j++) {
                    columnName[j] = columns[j].columnName;
                }
            } else {
                int i = 0;
                StringTokenizer postkr = new StringTokenizer(sPosFormatted, ",");
                columnName = new String[postkr.countTokens()];
                String token;
                while (postkr.hasMoreTokens()) {
                    token = postkr.nextToken(",");
                    token = token.trim();
                    int end = token.indexOf(" ", 0);
                    columnName[i++] = token.substring(0, end);
                }
            }

            // set connection to not auto commit if specifed
            notCommitted = 0;

            // Save original line so that we can write it to error/redo file
            String[] lineList = null;
            if (sCommitInt > 0) {
                lineList = new String[sCommitInt];
            } 

            int listCount = 0;

            String baseInsert = "INSERT INTO \"" + dbTable + "\" (";
            for (int j = 0; j < columnName.length - 1; j++) {
                baseInsert += "\"" + columnName[j] + "\", ";
            }
            baseInsert += "\"" + columnName[columnName.length - 1]
                    + "\") ";
            baseInsert += " VALUES (";

            // read and parse a line - insert/update to db table
            while (line != null) {
                linesRead++;
                if (endLine > 0) {
                    if (linesRead > endLine) {
                        break;
                    }
                }
            	String[] columnData;
                try {
                    if (sPosFormatted == null) {
                    	columnData = getFields(line, columns.length);
                    } else {
                    	columnData = new String[columns.length];
                        for (int j = 0; j < columnData.length; j++) {
                            int index = sPosFormatted.indexOf(columnName[j]);
                            if (index != -1) {
                                columnData[j] = getColumnData(line, index);
                            } else {
                                exitWithError("Error reading columns");
                            }
                        }
                    }
                } catch (Exception e) {
                    numErrors++;
                    errorStream.write(line);
                    errorStream.write(sTerminator);
                    errorStream.flush();
                    System.err.println("Data file " + sImportFile
                            + ": Error at line #" + linesRead);
                    if (sMaxErrors != 0) {
                        if (numErrors >= sMaxErrors) {
                            // System.out.println (e.getMessage());
                            System.err.println(e.getMessage());
                            e.printStackTrace();
                            break;
                        }
                    }
                    line = readLine();
                    continue;
                }

                StringBuffer sbQuery = new StringBuffer(baseInsert);

                for (int j = 0; j < columnData.length; j++) {
                    if (columnData[j] == null) {
                        sbQuery.append("null");
                    } else if (quoteDataType(columns[j].columnSQLType)) {
                        sbQuery.append(quote(columnData[j]));
                    } else {
                        sbQuery.append(columnData[j]);
                    }
                    sbQuery.append(", ");
                }
                sbQuery.setLength(sbQuery.length() - 2);
                sbQuery.append(")");
                sbQuery.append(stmtTerminator);
                String query = sbQuery.toString();
                try {
                    // System.out.println ("rowsProcessed = " + rowsProcessed
                    // + "; notCommitted = " + notCommitted);
                    if (sCommitInt > 0) {
                        dbStatement.addBatch(query);

                        lineList[listCount++] = line;
                    } else {
                        dbStatement.execute(query);
                    }

                    rowsProcessed++;
                    // prepare to process next line
                    notCommitted++;

                    if (linesRead % notifyInterval == 0 && !silent) {
                        // System.out.println("Number of lines read so far :"+
                        // linesRead);
                        System.out.println("[" + new Date() + "] Number of lines read so far :" + linesRead);
                    }

                    if (notCommitted == sCommitInt) {
                        listCount = 0;
                        // org.postgresql.stado.Misc.Timer bTimer = new
                        // org.postgresql.stado.Misc.Timer();
                        // bTimer.startTimer();
                        int[] updateCounts = dbStatement.executeBatch();
                        // bTimer.stopTimer();
                        // System.out.println ("--- batch Time: " + bTimer);
                        processUpdateCounts(updateCounts, lineList);
                    }

                } catch (BatchUpdateException b) {
                    processUpdateCounts(b.getUpdateCounts(), lineList);
                    if (sMaxErrors != 0) {
                        if (numErrors >= sMaxErrors) {
                            break;
                        }
                    }
                } catch (SQLException se) {
                    if (!ignore) {
                        if (!overWrite) {
                            numErrors++;
                            errorStream.write(line);
                            errorStream.write(sTerminator);
                            errorStream.flush();
                            System.err.println("Data file " + sImportFile
                                    + ": Error at line #" + linesRead);
                            System.err.println(se.getMessage());
                            se.printStackTrace();
                            if (sMaxErrors != 0) {
                                if (numErrors >= sMaxErrors) {
                                    break;
                                }
                            }
                        } else {
                            // execute SQL update statement
                            query = "UPDATE " + dbTable;
                            query += " SET ";
                            for (int j = 0; j < columnName.length - 1; j++) {
                                if (quoteDataType(columns[j].columnSQLType)) {
                                    query += columnName[j] + "="
                                            + quote(columnData[j]) + ",";
                                } else {
                                    query += columnName[j] + "="
                                            + columnData[j] + ",";
                                }
                            }
                            String primeKey = getPrimaryKey();
                            if (quoteDataType(columns[columnName.length - 1].columnSQLType)) {
                                query += columnName[columnName.length - 1]
                                        + "="
                                        + quote(columnData[columnName.length - 1]);
                            } else {
                                query += columnName[columnName.length - 1]
                                        + "="
                                        + columnData[columnName.length - 1];
                            }
                            query += " WHERE " + primeKey + "=";
                            int k;
                            try {
                                for (k = 0; k < columnName.length; k++) {
                                    if (primeKey.equals(columnName[k])) {
                                        break;
                                    }
                                }
                                query += columnData[k] + stmtTerminator;
                                // System.out.println(query);
                                System.out.println(query);
                                if (sCommitInt > 0) {
                                    dbStatement.addBatch(query);
                                } else {
                                    dbStatement.execute(query);
                                }
                                rowsProcessed++;
                            } catch (Exception e) {
                                numErrors++;
                                errorStream.write(line);
                                errorStream.write(sTerminator);
                                errorStream.flush();
                                System.err.println("Data file " + sImportFile
                                        + ": Error at line #" + linesRead);
                                System.err.println(se.getMessage());
                                se.printStackTrace();
                                if (sMaxErrors != 0) {
                                    if (numErrors >= sMaxErrors) {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    notCommitted++;
                    if (notCommitted == sCommitInt) {
                        try {
                            int[] updateCounts = dbStatement.executeBatch();
                            processUpdateCounts(updateCounts, lineList);
                        } catch (BatchUpdateException b) {
                            processUpdateCounts(b.getUpdateCounts(), lineList);
                        } catch (Exception e) {
                            System.err.println(se.getMessage());
                        }
                    }
                    if (linesRead % notifyInterval == 0 && !silent) {
                        System.out.println("Number of rows loaded so far :" + linesRead);
                    }
                }
                line = readLine();
            }

            // if some rows still not commited and auto commit is false
            // so commit them
            if (notCommitted > 0 && sCommitInt > 0) {
                try {
                    int[] updateCounts = dbStatement.executeBatch();
                    processUpdateCounts(updateCounts, lineList);
                } catch (BatchUpdateException b) {
                    processUpdateCounts(b.getUpdateCounts(), lineList);
                } catch (Exception e) {
                    System.err.println(e.getMessage());
                }
            }
            System.out.println("[" + new Date() + "] Total number of rows loaded :" + rowsProcessed);
            if (numErrors > 0) {
                System.out.println("# errors encountered=" + numErrors
                        + ". Maximum #errors allowed=" + sMaxErrors + ".");

            }
            in.close();
            errorStream.close();
        } catch (Exception e) {
            System.err.println("Data file " + sImportFile + ": Error at line #"
                    + linesRead);
            // System.out.println (e.getMessage());
            System.err.println(e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * find the number of fields in the input data
     *
     * @return int
     * @param line
     */
    private String[] getFields(String line, int expectedCount) throws SQLException {
    	String[] result = new String[expectedCount];
        int start = 0;
        int count = 0;
        while (true) {
            int pos = line.indexOf(sFieldSep, start);
            if (pos < 0) {
                break;
            }
            if (count < result.length) {
            	String value = line.substring(start, pos);
            	result[count++] = value.equals(nullValue) ? null : value;
                start = pos + 1;
            } else {
            	throw new SQLException("Extra data after last field");
            }
        }
        if (hasTrailingDelimiter) {
        	if (count < result.length) {
            	throw new SQLException("Field count too low");
        	}
        	if (start < line.length()) {
            	throw new SQLException("Extra data after last field");
        	}
        } else {
    		if (count < result.length - 1) { 
            	throw new SQLException("Field count too low");
    		}
        	if (count > result.length - 1) {
            	throw new SQLException("Extra data after last field");
        	}
        	String value = line.substring(start);
        	result[count++] = value.equals(nullValue) ? null : value;
        }
        return result;
    }

    /**
     * read the next input line from import file
     *
     * @return String
     */
    private String readLine() {
        int c;
        String line = null;
        buffer = new StringBuffer();
        // get next line
        try {
            while ((c = in.read()) != -1) {
                if ((char) c != sTerminator.charAt(0)) {
                    buffer.append((char) c);
                } else {
                    break;
                }
            }
            if (buffer.length() > 0) {
                line = buffer.toString();
            } else {
                line = null;
            }
        } catch (Exception e) {
            // System.out.println ("Could not read from file: "
            // + sImportFile);
            System.err.println("Could not read from file: " + sImportFile);

            System.exit(1);
        }
        return line;
    }

    /**
     * process update counts from batch execute (non-parallel mode)
     *
     * @param updateCounts
     * @param lineList
     * @throws java.sql.SQLException
     */
    private void processUpdateCounts(int[] updateCounts, String[] lineList) {
        for (int i = 0; i < lineList.length; i++) {
            if (updateCounts.length <= i || updateCounts[i] == Statement.EXECUTE_FAILED) {
            	if (lineList[i] == null) {
            		// EOF
            		break;
            	}
                rowsProcessed--;
                numErrors++;
                try {
                    errorStream.write(lineList[i]);
                    errorStream.write(sTerminator);
                    errorStream.flush();
                } catch (IOException e) {
                    System.err.println("Error while writing to error stream");
                }
                System.err.println("Data file " + sImportFile
                        + ":Error during batch update :" + lineList[i]);
            }
        }
        // Batches are not being committed, so do it here explicitly
        try {
            dbConnection.commit();
        } catch (SQLException se) {
            System.err.println("Failed to commit batch");
            try {
                dbConnection.rollback();
            } catch (SQLException ignore) {
            }
        }
        Arrays.fill(lineList, null);
        notCommitted = 0;
    }

    /**
     * check whether the data type needs to be quoted
     *
     * @return boolean
     * @param type
     */
    private boolean quoteDataType(int type) {
        boolean quote;
        switch (type) {
        case java.sql.Types.CHAR:
        case java.sql.Types.VARCHAR:
        case java.sql.Types.DATE:
        case java.sql.Types.TIME:
        case java.sql.Types.TIMESTAMP:
            quote = true;
            break;
        default:
            quote = false;
        }
        return quote;
    }

    /**
     * process the table data
     *
     * @throws java.sql.SQLException
     */
    private void processResultSet() throws SQLException {

        ResultSet rs = dbConnection.getMetaData().getColumns(
                dbConnection.getCatalog(), "%", dbTable, "%");
        try {
            ArrayList<ColumnDef> columnDefs = new ArrayList<ColumnDef>();
            while (rs.next()) {
                columnDefs.add(new ColumnDef(rs.getString("COLUMN_NAME"), rs
                        .getInt("DATA_TYPE")));
            }
            columns = columnDefs.toArray(new ColumnDef[columnDefs.size()]);
            if (columns.length == 0) {
            	throw new SQLException("Table not found: " + dbTable);
            }
        } finally {
            rs.close();
        }
    }

    /**
     * Get the column data in case of POSITION_FORMATTED
     *
     * @return String
     * @param line
     * @param i
     */
    private String getColumnData(String line, int i) {
        int index = sPosFormatted.indexOf(" ", i);
        index = skipWhiteSpaces(index, sPosFormatted);
        int colPos = sPosFormatted.indexOf(":", index);
        String from = sPosFormatted.substring(index + 1, colPos);
        int endIndex = sPosFormatted.indexOf(",", colPos);
        String to = null;
        if (endIndex < 0) {
            to = sPosFormatted.substring(colPos + 1, sPosFormatted.length());
        } else {
            to = sPosFormatted.substring(colPos + 1, sPosFormatted.indexOf(",",
                    colPos));
        }
        int start = Integer.parseInt(from);
        int end = Integer.parseInt(to);
        return line.substring(start - 1, end);
    }

    /**
     * skips any white spaces
     *
     * @return int
     * @param i
     * @param str
     */
    public static int skipWhiteSpaces(int i, String str) {
        char[] cha;
        cha = str.toCharArray();
        while (cha[i + 1] == ' ') {
            i++;
        }
        return i;
    }

    /**
     * protect data with quotes
     *
     * @return Stirng
     * @param include
     */
    private String quote(String include) {
        if (quoted == null && include != null) {
            return "'" + include + "'";
        }

        return include;
    }

    /**
     * export database table to flat file.
     *
     */
    private void exportData() {
        if (sTerminator == null) {
            sTerminator = "\n";
        }
        try {
            String query;
            StringTokenizer tk = null;
            if (sExtractQuery != null) {
                tk = new StringTokenizer(sExtractQuery);
                // execute select query
                if (tk.countTokens() == 1) {
                    dbTable = tk.nextToken();
                    query = "SELECT * FROM \"" + dbTable + "\""
                            + stmtTerminator;
                } else {
                    query = sExtractQuery;
                }
            } else {
                query = "SELECT * FROM \"" + dbTable + "\"" + stmtTerminator;
            }
            if (sCommitInt > 0) {
                dbStatement.setFetchSize(sCommitInt);
            }
            ResultSet tableRS = dbStatement.executeQuery(query);

            // open the output stream and dump row by row
            if (sExportFile == null || sExportFile.length() == 0) {
                outStream = System.out;
            } else {
                outStream = new FileOutputStream(sExportFile);
            }

            OutputFormatter pf = new OutputFormatter(sPosFormatted);
            pf.setHasTrailingDelimiter(hasTrailingDelimiter);
            pf.setTrimTrailingSpaces(trimTrailingSpaces);
            pf.setSFieldSep(sFieldSep);
            pf.setNullValue(nullValue);
            pf.setSTerminator(sTerminator);
            if (quoted != null) {
                pf.setQuoteInfo(quoted, null, null);
            }
            if (!silent) {
                pf.setNotification(notifyInterval, System.out);
            }
            long linesRead = pf.printRS(tableRS, outStream);
            System.out.println("Total number of rows processed :" + linesRead);
            // close the stream
            outStream.close();
            tableRS.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Finds the primary key for the table
     *
     * @return String
     */
    private String getPrimaryKey() {
        String pkey_colname = null;
        try {
            DatabaseMetaData meta = dbConnection.getMetaData();
            ResultSet rset = meta.getPrimaryKeys(null, null, dbTable);
            while (rset.next()) {
                pkey_colname = rset.getString(6);
            }
        } catch (SQLException se) {
            // System.out.println("Error finding the primary key ");
            System.err.println("Error finding the  primary key ");
        }
        return pkey_colname;
    }

    /**
     * Exits with an error. Outputs the given error string as well as the
     * current date.
     *
     * @param error
     *                message to output on standard error
     */
    private void exitWithError(String error) {
        // System.out.println();
        System.err.println(error);
        System.exit(1);
    }

    private class ColumnDef {
        private String columnName;

        private int columnSQLType;

        /**
         *
         * @param columnName
         * @param columnSQLType
         */
        private ColumnDef(String columnName, int columnSQLType) {
            this.columnName = columnName;
            this.columnSQLType = columnSQLType;
        }
    }

}
