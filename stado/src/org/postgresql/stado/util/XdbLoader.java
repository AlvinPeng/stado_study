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


import org.postgresql.stado.common.util.ParseCmdLine;
import org.postgresql.stado.engine.copy.CopyManager;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class XdbLoader {

    private static String tableName = null;

    private static String copyCommand = null;

    private static LoaderFilter filter = null;

    private static InputStream input = null;

    private static String copyDelimiter = null;

    private static boolean modeCSV = false;

    private static char quoteChar = '"';

    private static char quoteEscape = '"';

    private static String commentPrefix = null;

    private static int commitInterval = 0;

    private static int autoReduceRate = 0;

    private static int minCommitInterval = 1; 

    private static boolean keepOriginalFormat = false;

    private static int totalChunkCount = 0;

    private static int badChunkCount = 0;

    private static long badLinesCount = 0;

    private static String badFileName = null;

    private static final int DEFAULT_VERBOSE_COUNT = 100000;
    
    private static int verboseCount = 0;
    
    private static int maxErrors = 0;
    
    private static long startLine = 1;

    private static long endLine = 0;
        
    /* if using a rejection file, we still want to output at least 
     * on message from the server.
     */
    private static String firstErrorMessage = null;
        
    private static long startTime = 0;
    private static long finishTime = 0;
    
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
                .println("\nParameters: <connect> -t <table>  [-c <column_list>] [-i <inputfilename>]\n" 
                        + "\t[-f <delimiter>] [-z <NULL>]\n"
                        + "\t[-v [-q <quote>] [-e <escape>] -n <column_list>\n"
                        + "\t[-o] [-a] [-r <prefix>] [-w [<count>]] [-b <reject_filename>]\n"
                        + "\t[-k <commit_interval>[,<autoreducing_rate>[,<min_interval>]]\n"
                        + "\twhere <connect> is\n"
                        + "\t  -j jdbc:postgresql://<host>:<port>/<database>?user=<username>&password=<password>\n"
                        + "\t  or [-h <host>] [-s <port>] -d <database> -u <user> [-p <password>]\n"
                        + "\t-h <host> : Host where XDBServer is running. Default is localhost\n"
                        + "\t-s <port> : XDBServer's port. Default is 6453\n"
                        + "\t-d <database> : Name of database to connect to.\n"
                        + "\t-u <user>, -p <password> : Login to the database\n"
                        + "\t-t <table> : target table name\n"
                        + "\t-c <column_list> : comma or space separated list of columns\n"
                        + "\t-i <inputfilename> : name of file with data to be loaded.\n"
                        + "\t\tStandard input is used if omitted\n"
                        + "\t-f <delimiter> : field delimiter. Default is \\t (tab character)\n"
                        + "\t-z <NULL> : value to indicate NULL. Default is \\N \n"
                        + "\t-v : CSV mode\n"
                        + "\t-q <quote> : Quote character, default \" (CSV mode only)\n"
                        + "\t-e <quote> : Escape of character. Default is quote character (double)\n"
                        + "\t\t (CSV mode only)\n"
                        + "\t-n <column_list>: Force not null. Values for this column are never\n"
                        + "\t\t treated as NULL, as if they was qouted\n"
                        + "\t-a : remove trailing delimiter\n"
                        + "\t-o : same as WITH OIDS\n"                
                        + "\t-r <prefix> : ignore data lines starting from specified prefix\n"                        
                        + "\t-w [<count>] : verbose- every <count> lines (default 100000)\n"
                        + "\t\t display number of lines read\n"
                        + "\t-g <max_errors> : abort after this number of errors\n"                 
                        + "\t-l <line> : starting line number to load from input file\n"
                        + "\t-m <line> : max ending line number to load from input file\n"               
                        + "\t-b <filename> : file for reject lines or chunks\n"                
                        + "\t-k <commit_interval>[,<autoreducing_rate>[,<min_interval>]]:\n"
                        + "\t   <commit_interval> : number of lines to commit at a time\n"
                        + "\t   <autoreducing_rate> : if chunk failed, divide into this\n"
                        + "\t    number of chunks and retry\n"
                        + "\t   <min_interval> : do not further divide chunks of specified size\n"
                        + "\t-x keep original format for failed chunks \n"
                        );

        System.exit(1);
    }

    /**
     *
     *
     *
     * @param m
     *
     * @param key
     *
     * @param errorMessage
     *
     * @return
     *
     */
    private static String getRequiredArg(Map<String, List<String>> m,
            String key, String errorMessage) {
        String argVal = ParseArgs.getStrArg(m, key);
        if (argVal == null || argVal.length() == 0) {
            terminate(errorMessage);
        }

        return argVal;
    }

    /**
     *
     *
     *
     * @param args
     *
     */

    public static void main(String[] args) {
        FileOutputStream rejectStream = null;
        try {
            
            Map<String, List<String>> m = ParseArgs.parse(args,
                    "abcdefghijklmnopqrstuvwxz");

            modeCSV = m.containsKey("-v");
            copyDelimiter = ParseArgs.getStrArg(m, "-f");
            if (copyDelimiter == null) {
                copyDelimiter = modeCSV ? "," : "\t";
            } else {
                copyDelimiter = ParseCmdLine.stripEscapes(copyDelimiter);
            }
            String fname = ParseArgs.getStrArg(m, "-i");
            commentPrefix = ParseArgs.getStrArg(m, "-r");
            if (fname == null || fname.length() == 0) {
                input = System.in;
            } else {
                input = new FileInputStream(fname);
            }
            String commitIntervalStr = ParseArgs.getStrArg(m, "-k");
            if (commitIntervalStr != null) {
                int outTail = commitIntervalStr.indexOf(",");
                if (outTail > 0) {
                    commitInterval = Integer.parseInt(commitIntervalStr
                            .substring(0, outTail));
                    commitIntervalStr = commitIntervalStr
                            .substring(outTail + 1);
                    outTail = commitIntervalStr.indexOf(",");
                    if (outTail > 0) {
                        autoReduceRate = Integer.parseInt(commitIntervalStr
                                .substring(0, outTail));
                        minCommitInterval = Integer.parseInt(commitIntervalStr
                                .substring(outTail + 1));
                    } else {
                        autoReduceRate = Integer.parseInt(commitIntervalStr);
                    }
                } else {
                    commitInterval = Integer.parseInt(commitIntervalStr);
                }
                keepOriginalFormat = m.containsKey("-x");
            }
            filter = new LoaderFilter(new BufferedInputStream(input,65536), 1024,
                    commitInterval);
            filter.filterTrailingDelimiter = m.containsKey("-a");
            if (copyDelimiter != null && copyDelimiter.length() > 0) {
                if (copyDelimiter.length() == 1) {
                    filter.delimiter = copyDelimiter.charAt(0);
                } else {
                    terminate("Delimiter is specified as \"" + copyDelimiter
                            + "\", it must be single character");
                }
            }
            filter.commentPrefix = commentPrefix;
            StringBuffer sbCopyCommand = new StringBuffer();
            tableName = getRequiredArg(m, "-t", "Target table is not specified");
            sbCopyCommand.append("COPY ").append(tableName);
            List<String> columns = m.get("-c");
            if (columns != null) {
                sbCopyCommand.append("(");
                for (String column : columns) {
                    sbCopyCommand.append(column).append(",");
                }
                sbCopyCommand.setLength(sbCopyCommand.length() - 1);
                sbCopyCommand.append(")");
            }
            sbCopyCommand.append(" FROM STDIN WITH DELIMITER '").append(ParseCmdLine.escape(copyDelimiter)).append("'");
            String nulls = ParseArgs.getStrArg(m, "-z");
            if (nulls != null) {
                nulls = ParseCmdLine.stripEscapes(nulls);
                sbCopyCommand.append(" NULL '").append(ParseCmdLine.escape(nulls)).append("'");
                filter.nulls = nulls;
            } else {
                filter.nulls = modeCSV ? "" : "\\N";
            }
            if (m.containsKey("-g")) {
                sbCopyCommand.append(" OIDS");
            }
            filter.modeCSV = modeCSV;
            Collection<String> forceNotNulls = m.get("-n");
            if (forceNotNulls != null) {
                Collection<String> newForceNotNulls = new HashSet<String>();
                for (String forceColumns : forceNotNulls) {
                    int pos = 0;
                    int nextPos = 0;
                    while ((nextPos = forceColumns.indexOf(",", pos)) > pos) {
                        newForceNotNulls.add(forceColumns.substring(pos, nextPos));
                        pos = nextPos + 1;
                    }
                    newForceNotNulls.add(forceColumns.substring(pos));
                }
            }
            if (modeCSV) {
                sbCopyCommand.append(" CSV");
                String quoteStr = ParseArgs.getStrArg(m, "-q");
                if (quoteStr != null) {
                    quoteStr = ParseCmdLine.stripEscapes(quoteStr);
                    if (quoteStr.length() != 1) {
                        throw new Exception("Quote must be single charachter");
                    } else {
                        sbCopyCommand.append(" QUOTE '").append(ParseCmdLine.escape(quoteStr)).append("'");
                        quoteChar = quoteStr.charAt(0);
                        quoteEscape = quoteChar;
                    }
                }
                quoteStr = ParseArgs.getStrArg(m, "-e");
                if (quoteStr != null) {
                    quoteStr = ParseCmdLine.stripEscapes(quoteStr);
                    if (quoteStr.length() != 1) {
                        throw new Exception("Quote escape must be single charachter");
                    } else {
                        sbCopyCommand.append(" ESCAPE '").append(ParseCmdLine.escape(quoteStr)).append("'");
                        quoteEscape = quoteStr.charAt(0);
                    }
                }
                filter.quoteChar = quoteChar;
                filter.quoteEscape = quoteEscape;
                if (forceNotNulls != null && forceNotNulls.size() > 0) {
                    sbCopyCommand.append(" FORCE NOT NULL ");
                    for (String colName : forceNotNulls) {
                        sbCopyCommand.append(colName).append(", ");
                    }
                    sbCopyCommand.setLength(sbCopyCommand.length() - 2);
                }
            }
            copyCommand = sbCopyCommand.toString();
            badFileName = ParseArgs.getStrArg(m, "-b");
            String maxErrorString = ParseArgs.getStrArg(m, "-g");
            if (maxErrorString != null) {
                maxErrors = Integer.parseInt(maxErrorString);
            }
            String startLineString = ParseArgs.getStrArg(m, "-l");
            if (startLineString != null) {
                startLine = Integer.parseInt(startLineString);
            }
            String endLineString = ParseArgs.getStrArg(m, "-m");
            if (endLineString != null) {
                endLine = Integer.parseInt(endLineString);
            }
            if (commitInterval > 0 && badFileName == null) {
                throw new Exception(
                        "Bad file for rejected chunks must be specified if commit interval is limited.");
            }
            if (tableName == null && copyCommand == null) {
                terminate("Either table name or copy command should be specified");
            }
            if (m.containsKey("-w")) {
                String verboseCountString = ParseArgs.getStrArg(m,"-w");
                if (verboseCountString != null && verboseCountString.length() > 0) {
                    verboseCount = Integer.valueOf(verboseCountString);
                } else {
                    verboseCount = DEFAULT_VERBOSE_COUNT;
                }
            }
            
            oConn = Util.connect(m);
            
            if (badFileName != null) {
                File rejectFile = new File(badFileName);
                rejectStream = new FileOutputStream(rejectFile);
            }
        } catch (Exception e) {
            if (rejectStream != null) {
                try {
                    rejectStream.close();
                } catch (IOException ioe) {}
            }
            terminate(e.getMessage());
        }

        startTime = System.currentTimeMillis();
        try {
            CopyManager copyManager = CopyManager.getCopyManager(oConn);
            while (!filter.exhausted) {
                filter.resetChunk();
                try {
                    totalChunkCount++;
                    copyManager.copyIn(copyCommand, filter);
                } catch (SQLException se) {
                    if (commitInterval > 1) {
                        if (firstErrorMessage == null) {
                            firstErrorMessage = se.getMessage();
                        }
                        autoretry(copyManager, filter.lastChunk,
                                filter.originalChunk, 0, filter.chunkPos, 
                                rejectStream, se.getMessage());
                    } else {
                        throw se;
                    }
                }
            }
            System.out.println("Loading is finished");
            finish();
            
        } catch (SQLException se) {
            System.out.println("SQLException: " + se.getMessage());
            SQLException next = se.getNextException();
            while (next != null) {
                System.out.println("Next SQLException: " + next.getMessage());
                next = next.getNextException();
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
        finally {
            if (rejectStream != null) {
                try {
                    rejectStream.close();
                } catch (IOException ioe) {}
            }
        }
    }
    
    private static void finish() {

        long procCount = filter.lineCount - (filter.badLineCount + (startLine - 1));
        if (procCount < 0) {
            procCount = 0;
        }
        System.out.println(procCount
                + " input rows have been sent to Server in "
                + totalChunkCount + " chunks");          
        if (badChunkCount > 0) {
            System.out.println(badChunkCount + " chunks containing "
                    + badLinesCount
                    + " rows have been rejected by server and written to "
                    + badFileName);
        }
        if (firstErrorMessage != null) {
            System.out.println("First error message: ");
            System.out.println(" " + firstErrorMessage);
        }            
            
        try {
            oConn.close();
        } catch (SQLException se) {
            System.out.println("SQLException: " + se.getMessage());
            SQLException next = se.getNextException();
            while (next != null) {
                System.out.println("Next SQLException: " + next.getMessage());
                next = next.getNextException();
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }

        finishTime = System.currentTimeMillis();

        System.out.printf("\nTime : " + (double) (finishTime - startTime)
                / 1000 + " (seconds)\n");
    }
            

    private static void autoretry(CopyManager copyManager, byte[][] chunk,
            byte[][] originalChunk, int pos, int length, FileOutputStream out, 
            String reason)
            throws IOException, SQLException {
        if (autoReduceRate < 2 || length <= minCommitInterval) {
            badChunkCount++;
            badLinesCount += length;
            
            if (commentPrefix != null && commentPrefix.length() > 0
                    && reason != null && reason.length() > 0) {
                out.write(commentPrefix.getBytes());
                out.write(reason.getBytes());
                out.write(System.getProperty("line.separator").getBytes());
            }

            for (int i = pos; i < pos + length; i++) {
                out.write(keepOriginalFormat && originalChunk != null ? originalChunk[i] : chunk[i]);
            }

            if (maxErrors > 0 && commitInterval > 1 && maxErrors > badChunkCount) {
                // We exceeded the maximum number of errors.
                // If the user used a commit interval, it may be that 
                // some data got committed from their load file, so it
                // is important to indicate how many lines we read to.
                // This option should be used with caution so that the user does
                // not lose track of where they are in processing, but can be
                // useful for large loads on newly created tables with unreliable
                // input files
                System.err.println("Maximum number of errors reached: " + maxErrors);
                System.err.println("Read up until line " + filter.lineCount);
                finish();
                System.exit(1);
            }
        } else {
            int reducedSize = (length - 1) / autoReduceRate + 1;
            for (int i = pos; i < pos + length; i += reducedSize) {
                int size = Math.min(pos + length - i, reducedSize);
                try {
                    totalChunkCount++;
                    InputStream is = new ByteMatrixInputStream(chunk, i, size);
                    copyManager.copyIn(copyCommand, is);
                } catch (SQLException se) {
                    autoretry(copyManager, chunk, originalChunk, i, size, out, 
                            se.getMessage());
                }
            }
        }
    }

    // Destination table definitions
    private static Connection oConn = null;

    private static class LoaderFilter extends FilterInputStream {

        // stream state
        byte[] outBuffer;

        int outHead = 0;

        int outTail = 0;

        boolean exhausted = false;

        // converter options
        boolean filterTrailingDelimiter = false;

        char delimiter = '\t';

        String nulls = "\\N";

        boolean modeCSV = false;

        char quoteChar = '"';

        char quoteEscape = '"';

        TableColumnDescription[] columnDescriptions = null;

        String commentPrefix = null;

        int chunkSize = 0;

        byte[][] lastChunk = null;

        boolean keepOriginalFormat = false;

        int originalTail = 0;

        byte[] originalBuffer = null;

        byte[][] originalChunk = null;

        int chunkPos = 0;

        long lineCount = 0;

        OutputStream badLines = null;

        long badLineCount = 0;

        // Scanner state
        int ch;

        boolean charInBuffer = false;

        LoaderFilter(InputStream in, int bufferSize, int chunkSize) {
            super(in);
            outBuffer = new byte[bufferSize < 8096 ? 8096 : bufferSize];
            this.chunkSize = chunkSize;
        }

        /*
         * (non-Javadoc)
         *
         * @see java.io.FilterInputStream#available()
         */
        @Override
        public int available() throws IOException {
            return outTail - outHead;
        }

        /*
         * (non-Javadoc)
         *
         * @see java.io.FilterInputStream#mark(int)
         */
        @Override
        public synchronized void mark(int readlimit) {
            throw new UnsupportedOperationException();
        }

        /*
         * (non-Javadoc)
         *
         * @see java.io.FilterInputStream#markSupported()
         */
        @Override
        public boolean markSupported() {
            return false;
        }

        /*
         * (non-Javadoc)
         *
         * @see java.io.FilterInputStream#read()
         */
        @Override
        public int read() throws IOException {
            if (outHead == outTail && !exhausted) {
                if (lastChunk != null) {
                    if (chunkPos < lastChunk.length) {
                        readLine();
                        if (outTail > 0) {
                            if (keepOriginalFormat) {
                                byte[] originalLine = new byte[originalTail];
                                System.arraycopy(originalBuffer, 0,
                                        originalLine, 0, originalTail);
                                originalChunk[chunkPos] = originalLine;
                            }
                            byte[] lastLine = new byte[outTail];
                            System
                                    .arraycopy(outBuffer, 0, lastLine, 0,
                                            outTail);
                            lastChunk[chunkPos++] = lastLine;
                        }
                    }
                } else {
                    readLine();
                }
            }
            return outHead < outTail ? outBuffer[outHead++] & 0xff : -1;
        }

        /*
         * (non-Javadoc)
         *
         * @see java.io.FilterInputStream#read(byte[], int, int)
         */
        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (b == null) {
                throw new NullPointerException();
            } else if (off < 0 || off > b.length || len < 0
                    || off + len > b.length || off + len < 0) {
                throw new IndexOutOfBoundsException();
            }
            int totalRead = 0;
            while (totalRead < len && (!exhausted || outHead < outTail)) {
                if (outTail - outHead < len - totalRead) {
                    if (outTail > outHead) {
                        System.arraycopy(outBuffer, outHead, b,
                                off + totalRead, outTail - outHead);
                        totalRead += outTail - outHead;
                        outHead = outTail;
                    }
                    if (lastChunk != null) {
                        if (chunkPos < lastChunk.length) {
                            readLine();
                            if (outTail > 0) {
                                if (keepOriginalFormat) {
                                    byte[] originalLine = new byte[originalTail];
                                    System.arraycopy(originalBuffer, 0,
                                            originalLine, 0, originalTail);
                                    originalChunk[chunkPos] = originalLine;
                                }
                                byte[] lastLine = new byte[outTail];
                                System.arraycopy(outBuffer, 0, lastLine, 0,
                                        outTail);
                                lastChunk[chunkPos++] = lastLine;
                            }
                        } else {
                            break;
                        }
                    } else {
                        readLine();
                    }
                } else {
                    System.arraycopy(outBuffer, outHead, b, off + totalRead,
                            len - totalRead);
                    outHead += len - totalRead;
                    totalRead = len;
                }
            }
            return totalRead == 0 && len != 0 ? -1 : totalRead;
        }

        /*
         * (non-Javadoc)
         *
         * @see java.io.FilterInputStream#reset()
         */
        @Override
        public synchronized void reset() throws IOException {
            throw new UnsupportedOperationException();
        }

        public void resetChunk() {
            if (chunkSize > 1) {
                lastChunk = new byte[chunkSize][];
                chunkPos = 0;
                if (keepOriginalFormat) {
                    originalChunk = new byte[chunkSize][];
                }
            }
        }

        private byte[] expandBuffer(byte[] b) {
            byte[] xBuffer = new byte[b.length * 2];
            System.arraycopy(b, 0, xBuffer, 0, b.length);
            return xBuffer;
        }

        private int getNextChar() throws IOException {
            int ch = in.read();
            if (keepOriginalFormat && ch != -1) {
                originalBuffer[originalTail++] = (byte) ch;
                if (originalTail == originalBuffer.length) {
                    originalBuffer = expandBuffer(originalBuffer);
                }
            }
            return ch;
        }

        private void outputNextChar(int ch) {
            outBuffer[outTail++] = (byte) ch;
            if (outTail == outBuffer.length) {
                outBuffer = expandBuffer(outBuffer);
            }
        }

        private void readLine() throws IOException {
            // Check if we only read up until a max number of lines
            if (endLine > 0 && lineCount >= endLine) {
                exhausted = true;
                return;
            }
            // Filter out lines we were told to skip
            do {
                doReadLine();
            } while (lineCount < startLine);
        }
        
        private void doReadLine() throws IOException {
            outHead = 0;
            outTail = 0;
            boolean commentMatch = commentPrefix != null
                    && commentPrefix.length() > 0;
            boolean nullMatch = true;
            boolean quoted = false;
            int currentFieldNumber = 0;
            int currentFieldStart = 0;
            String badLineReason = null;
            if (keepOriginalFormat) {
                originalTail = 0;
                originalBuffer = new byte[outBuffer.length];
            }
            while (true) {
                // Put next character into ch, if it is not there
                if (charInBuffer) {
                    charInBuffer = false;
                } else {
                    ch = getNextChar();
                }
                // EOF reached
                if (ch == -1) {
                    exhausted = true;
                    break;
                // Handle csv mode
                } else if (modeCSV && quoted) {
                    outputNextChar(ch);
                    if (ch == quoteEscape) {
                        ch = getNextChar();
                        if (ch == quoteChar) {
                            outputNextChar(ch);
                        } else {
                            charInBuffer = true;
                            if (quoteChar == quoteEscape) {
                                // Actually closing quote, not escape
                                quoted = false;
                                if (ch != delimiter && ch != '\r' && ch != '\n') {
                                    badLineReason = "Found characters after closing quote";
                                }
                            }
                        }
                    } else if (ch == quoteChar) {
                        ch = getNextChar();
                        charInBuffer = true;
                        quoted = false;
                        if (ch != delimiter && ch != '\r' && ch != '\n') {
                            badLineReason = "Found characters after closing quote";
                        }
                    }
                // detect EOL
                } else if (ch == '\r') {
                    outBuffer[outTail++] = (byte) ch;
                    if (outTail == outBuffer.length) {
                        outBuffer = expandBuffer(outBuffer);
                    }
                    ch = getNextChar();
                    if (ch == '\n') {
                        outBuffer[outTail++] = (byte) ch;
                        if (outTail == outBuffer.length) {
                            outBuffer = expandBuffer(outBuffer);
                        }
                    } else {
                        charInBuffer = true;
                    }
                    break;
                } else if (ch == '\n') {
                    outBuffer[outTail++] = (byte) ch;
                    if (outTail == outBuffer.length) {
                        outBuffer = expandBuffer(outBuffer);
                    }
                    break;
                } else if (ch == delimiter) {
                    TableColumnDescription colDesc = null;
                    if (columnDescriptions != null) {
                        if (currentFieldNumber < columnDescriptions.length) {
                            colDesc = columnDescriptions[currentFieldNumber++];
                        } else {
                            badLineReason = "Extra data after last field";
                        }
                    }
                    if (colDesc != null) {
                        if (nullMatch && !colDesc.nullable) {
                            badLineReason = "Column " + colDesc.name
                                    + " does not allow nulls";
                        }
                    }
                    if (filterTrailingDelimiter) {
                        if (columnDescriptions == null
                                || currentFieldNumber != columnDescriptions.length) {
                            ch = in.read();
                            if (keepOriginalFormat && ch != -1) {
                                originalBuffer[originalTail++] = (byte) ch;
                                if (originalTail == originalBuffer.length) {
                                    originalBuffer = expandBuffer(originalBuffer);
                                }
                            }
                            if (ch != '\r' && ch != '\n' && ch != -1) {
                                outBuffer[outTail++] = (byte) delimiter;
                                if (outTail == outBuffer.length) {
                                    outBuffer = expandBuffer(outBuffer);
                                }
                            }
                            charInBuffer = true;
                            // } else { discard trailing delimiter
                        }
                    } else {
                        outBuffer[outTail++] = (byte) delimiter;
                        if (outTail == outBuffer.length) {
                            outBuffer = expandBuffer(outBuffer);
                        }
                    }
                } else {
                    if (commentMatch && outTail < commentPrefix.length()
                            && ch != commentPrefix.charAt(outTail)) {
                        commentMatch = false;
                    }
                    if (nullMatch
                            && (outTail - currentFieldStart < nulls.length()
                                    && ch != nulls.charAt(outTail
                                            - currentFieldStart) || outTail
                                    - currentFieldStart >= nulls.length())) {
                        nullMatch = false;
                    }
                    outBuffer[outTail++] = (byte) ch;
                    if (outTail == outBuffer.length) {
                        outBuffer = expandBuffer(outBuffer);
                    }
                }
            }
            if (outTail == 0) {
                // EOF is reached
                return;
            }
            if (commentMatch && outTail > commentPrefix.length()) {
                outTail = 0;
                return;
            }
            if (!filterTrailingDelimiter && columnDescriptions != null) {
                TableColumnDescription colDesc = null;
                if (currentFieldNumber < columnDescriptions.length) {
                    colDesc = columnDescriptions[currentFieldNumber++];
                } else {
                    badLineReason = "Extra data after last field";
                }
                if (colDesc != null) {
                    if (nullMatch && !colDesc.nullable) {
                        badLineReason = "Column " + colDesc.name
                                + " does not allow nulls";
                    }
                }
            }
            if (columnDescriptions != null && badLineReason == null
                    && currentFieldNumber != columnDescriptions.length) {
                // Skip empty line
                if (outTail == 1
                        && (outBuffer[0] == '\r' || outBuffer[0] == '\n')
                        || outTail == 2 && outBuffer[0] == '\r'
                        && outBuffer[1] == '\n') {
                    outTail = 0;
                    return;
                } else {
                    badLineReason = "Too few columns in the line";
                }
            }
            lineCount++;
            if (verboseCount > 0 && lineCount % verboseCount == 0) {
                System.out.print(lineCount);
                System.out.println(" lines read");
            }
            if (badLineReason != null) {
                if (badLines != null) {
                    if (commentPrefix != null) {
                        badLines.write(commentPrefix.getBytes());
                        badLines.write(' ');
                        badLines.write(badLineReason.getBytes());
                        badLines.write('\n');
                    }
                    badLines.write(outBuffer, 0, outTail);
                } else {
                    throw new IOException(badLineReason);
                }
                outTail = 0;
                badLineCount++;
            }
        }
    }

    private static class ByteMatrixInputStream extends InputStream {

        byte[][] matrix;

        byte[] currentLine;

        int lastLinePos;

        int currentLinePos;

        int currentPos;

        ByteMatrixInputStream(byte[][] matrix, int offset, int length) {
            this.matrix = matrix;
            currentLinePos = offset;
            lastLinePos = Math.min(offset + length, matrix.length);
            if ((currentLine = matrix[currentLinePos++]) == null) {
                moveToNextLine();
            }
        }

        /*
         * (non-Javadoc)
         *
         * @see java.io.InputStream#available()
         */
        @Override
        public int available() throws IOException {
            return currentLine == null ? 0 : currentLine.length - currentPos;
        }

        /*
         * (non-Javadoc)
         *
         * @see java.io.InputStream#mark(int)
         */
        @Override
        public synchronized void mark(int readlimit) {
            throw new UnsupportedOperationException();
        }

        /*
         * (non-Javadoc)
         *
         * @see java.io.InputStream#markSupported()
         */
        @Override
        public boolean markSupported() {
            return false;
        }

        /*
         * (non-Javadoc)
         *
         * @see java.io.InputStream#read()
         */
        @Override
        public int read() throws IOException {
            if (currentLine == null) {
                return -1;
            }
            if (currentLine.length == currentPos) {
                moveToNextLine();
            }
            return currentLine == null ? -1 : currentLine[currentPos++] & 0xff;
        }

        /*
         * (non-Javadoc)
         *
         * @see java.io.#read(byte[], int, int)
         */
        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (b == null) {
                throw new NullPointerException();
            } else if (off < 0 || off > b.length || len < 0
                    || off + len > b.length || off + len < 0) {
                throw new IndexOutOfBoundsException();
            }
            int totalRead = 0;
            while (totalRead < len && currentLine != null) {
                if (currentLine.length - currentPos < len - totalRead) {
                    if (currentLine.length > currentPos) {
                        System.arraycopy(currentLine, currentPos, b, off
                                + totalRead, currentLine.length - currentPos);
                        totalRead += currentLine.length - currentPos;
                        currentPos = currentLine.length;
                    }
                    moveToNextLine();
                } else {
                    System.arraycopy(currentLine, currentPos, b, off
                            + totalRead, len - totalRead);
                    currentPos += len - totalRead;
                    totalRead = len;
                }
            }
            return totalRead == 0 && len != 0 ? -1 : totalRead;
        }

        /*
         * (non-Javadoc)
         *
         * @see java.io.InputStream#reset()
         */
        @Override
        public synchronized void reset() throws IOException {
            throw new UnsupportedOperationException();
        }

        private void moveToNextLine() {
            currentLine = null;
            currentPos = 0;
            while (currentLine == null && currentLinePos < lastLinePos) {
                currentLine = matrix[currentLinePos++];
                if (currentLine != null && currentLine.length == 0) {
                    currentLine = null;
                }
            }
        }
    }
    
    /**
     * Utility class to store information about table columns retrieved from
     * the server. We are interested in column name and if column allows nulls 
     */
    private static class TableColumnDescription {
        private String name;

        private boolean nullable;
    }
}
