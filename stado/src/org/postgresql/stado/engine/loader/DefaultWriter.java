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
 * DefaultWriter.java
 *
 *
 */
package org.postgresql.stado.engine.loader;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.log4j.Level;
import org.postgresql.stado.common.util.ParseCmdLine;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.StreamGobbler;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.NodeDBConnectionInfo;


/**
 *
 */
public class DefaultWriter implements INodeWriter {
    private static final XLogger logger = XLogger.getLogger(DefaultWriter.class);

    private String writerID;

    private String command;

    private OutputStream writer;

    private Process loader;

    private long rowCount = 0;

    private volatile boolean terminated = false;

    private String header;

    private String footer;

    private int totalChunks = 0;

    private long totalRows = 0;

    /**
     * @param connectionInfo
     *                the connection info for specified node database
     * @param table
     *                the target table name
     * @param delimiter
     *                the delimiter
     * @param columns
     *                a list of column names formatted according to template
     */
    public DefaultWriter(NodeDBConnectionInfo connectionInfo, String commandTemplate, Map<String,String> m) {
        writerID = connectionInfo.getDbName() + "@"
                + connectionInfo.getDbHost();
        m.put("dbhost", connectionInfo.getDbHost());
        if (connectionInfo.getDbPort() > 0) {
            m.put("dbport", "" + connectionInfo.getDbPort());
        }
        m.put("database", connectionInfo.getDbName());
        m.put("dbusername", connectionInfo.getDbUser());
        m.put("dbpassword", connectionInfo.getDbPassword());
        m.put("psql-util-name", Props.XDB_PSQL_UTIL_NAME);
        command = ParseCmdLine.substitute(commandTemplate, m);
        header = m.get("outHeader");
        footer = m.get("outFooter");
    };

    public void start() throws IOException {
        rowCount = 0;
        loader = Runtime.getRuntime().exec(parseCommand(command));
        writer = new BufferedOutputStream(loader.getOutputStream(), 65536);
        StreamGobbler out = new StreamGobbler(loader.getInputStream(), logger, Level.DEBUG);
        StreamGobbler err = new StreamGobbler(loader.getErrorStream(), logger, Level.ERROR);
        // kick off
        out.start();
        err.start();
        terminated = false;
        if (header != null) {
            writeRow(header.getBytes());
        }
    }

    public void commit() throws SQLException {

    }

    public void rollback() throws SQLException {

    }

    public void close() throws SQLException {

    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Util.loader.INodeWriter#finish()
     */
    public synchronized void finish(boolean success) throws IOException {
        if (writer == null) {
            throw new IOException(writerID + ": loader is not initialized");
        }
        try {
            if (success) {
                if (footer != null) {
                    writeRow(footer.getBytes());
                }
                writer.flush();
            }
            writer.close();
        } catch (IOException e) {
        }
        terminated = true;
        try {
            ForceKill processMonitor = new ForceKill(loader,
                    Props.XDB_STEP_ENDWAITTIME);
            int exitCode = loader.waitFor();
            // We do not wait while processMonitor is finished, because if
            // loader was already completed wasRanning() would return false
            if (processMonitor.wasRunning()) {
                throw new IOException(writerID
                        + ": loader was terminated due to network error");
            }
            if (success && exitCode != 0) {
                throw new IOException(writerID
                        + ": loader error occurred, exit code " + exitCode);
            }
            if (exitCode == 0 && rowCount > 0) {
                totalChunks++;
                totalRows += rowCount;
                logger.log(Level.DEBUG, "%0%: chunk completed, %1% rows are output",
                        new Object[] {writerID, rowCount});
            }
        } catch (InterruptedException e) {
        }
    }

    private String[] parseCommand(String command) {
        LinkedList<String> tokens = new LinkedList<String>();
        int pos = 0;
        int quoteStart = command.indexOf("\"");
        while (quoteStart >= 0) {
            int quoteEnd = command.indexOf("\"", quoteStart + 1);
            if (quoteEnd < 0) {
                break;
            }
            StringTokenizer st = new StringTokenizer(command.substring(pos,
                    quoteStart), " ");
            while (st.hasMoreTokens()) {
                tokens.add(st.nextToken());
            }
            tokens.add(command.substring(quoteStart + 1, quoteEnd));
            pos = quoteEnd + 1;
            quoteStart = command.indexOf("\"", pos);
        }
        StringTokenizer st = new StringTokenizer(command.substring(pos), " ");
        while (st.hasMoreTokens()) {
            tokens.add(st.nextToken());
        }
        return tokens.toArray(new String[tokens.size()]);
    }

    public synchronized void writeRow(byte[] row, int offset, int length) throws IOException {
        if (writer == null) {
            throw new XDBServerException(writerID
                    + ": loader is not initialized");
        }
        if (terminated) {
            throw new IOException("Process terminated");
        }
        writer.write(row, offset, length);
        writer.write('\n');
    }

    public synchronized void writeRow(byte[] row) throws IOException {
        writeRow(row, 0, row.length);
    }

    private class ForceKill implements Runnable {
        private Process process;

        private long waittime;

        private volatile boolean wasRunning = false;

        ForceKill(Process process, long waittime) {
            this.process = process;
            this.waittime = waittime;
            if (waittime > 0) {
                // kick off
                Thread t = new Thread(this);
                // We do not want loader is rinning 30 seconds more after
                // completion
                t.setDaemon(true);
                t.start();
            }
        }

        public void run() {
            long endtime = System.currentTimeMillis() + waittime;
            while (true) {
                try {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ignore) {
                    }
                    // this throws an exception if process is still running
                    process.exitValue();
                    return;
                } catch (IllegalThreadStateException itse) {
                    if (System.currentTimeMillis() > endtime) {
                        wasRunning = true;
                        // force process death
                        process.destroy();
                        return;
                    }
                }
            }
        }

        public boolean wasRunning() {
            return wasRunning;
        }
    }

    public String getStatistics() {
        return "Node Writer " + writerID + " has output " + totalRows
                + " rows in " + totalChunks + " chunks";
    }

    /**
     * @return row count from writer
     */
    public long getRowCount() {
        return rowCount;
    }

}
