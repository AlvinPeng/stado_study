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
 * QueryCombiner.java
 *
 */
package org.postgresql.stado.queryproc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.ProducerSender;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.parser.handler.IdentifierHandler;
import org.postgresql.stado.planner.StepDetail;


/**
 * QueryCombiner used when executing a step that takes place on the coordinator
 */
public class QueryCombiner {
    private static final XLogger logger = XLogger
            .getLogger(QueryCombiner.class);

    private XDBSessionContext client;

    private Connection oConn;

    private boolean bTempCreated = false;

    private String sTempTableName;

    public String sCreateStatement;

    private long insCount;

    private int iBatchCount;

    private Statement stmt;

    private static final boolean USEBATCHES = true;

    private static final boolean AUTOCOMMIT = false;

    private static final int BATCHSIZE = 1000;

    // pass in an existing connection
    // --------------------------------------------------------
    /**

     * 

     * @param client 

     * @param sTempTable 

     */

    public QueryCombiner(XDBSessionContext client, String sTempTable) {
        this.client = client;
        oConn = client.getAndSetCoordinatorConnection();

        try {

            oConn.setAutoCommit(AUTOCOMMIT);
        } catch (SQLException e) {
            // Just leave it as it is --
        }
        this.sTempTableName = sTempTable;
        insCount = 0;
    }

    /**
     * 
     * @param sql 
     * @throws java.sql.SQLException 
     */

    public void execute(String sql) throws SQLException {
        final String method = "execute";
        logger.entering(method, new Object[] { sql });
        try {

            Statement s = oConn.createStatement();
            s.executeUpdate(sql);
            if (!oConn.getAutoCommit()) {
                oConn.commit();
            }
            s.close();

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Creates combiner temp table
     */
    public synchronized void createTempTable() {
        final String method = "createTempTable";
        logger.entering(method);

        try {
            if (this.bTempCreated) {
                return;
            }
            try {
                execute(sCreateStatement);
            } catch (SQLException se) {
                if (handleSqlException(se, sCreateStatement)) {
                    try {
                        execute(sCreateStatement);
                        if (Props.XDB_COMMIT_AFTER_CREATE_TEMP_TABLE
                        // || Properties.XDB_USE_LOAD_FOR_STEP
                        ) {
                            oConn.commit();
                        }
                    } catch (SQLException se1) {
                        XDBServerException ex = new XDBServerException(
                                se1.getMessage() + "\nQUERY: "
                                        + sCreateStatement, se1,
                                ErrorMessageRepository.SQL_EXEC_FAILURE_CODE);
                        logger.throwing(ex);
                        throw ex;

                    }
                }
            }
            this.bTempCreated = true;
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * 
     * @param insertString 
     * @throws java.sql.SQLException 
     */

    private void doInsertFromStatement(String insertString) throws SQLException {
        String insertCmdStr = "";
        if (Props.XDB_JUST_DATA_VALUES) {
            insertCmdStr = "INSERT INTO " + IdentifierHandler.quote(sTempTableName) + " VALUES "
                    + insertString;
        } else {
            insertCmdStr = insertString;
        }

        if (!USEBATCHES) {
            execute(insertCmdStr);
            ++insCount;
        } else {
            stmt.addBatch(insertCmdStr);
            insCount++;
            if (++iBatchCount % BATCHSIZE == 0) {
                stmt.executeBatch();
                iBatchCount = 0;
                stmt.close();
                stmt = null;
            }
        }
    }

    /**
     * Inserts on coordinator
     * 
     * @param insertString
     *            the INSERT statement to execute
     */
    public synchronized void insertFromStatementOnCombiner(String insertString) {
        final String method = "insertFromStatementOnCombiner";
        logger.entering(method);

        try {

            if (stmt == null) {
                try {
                    stmt = oConn.createStatement();
                } catch (SQLException se) {
                    if (handleSqlException(se, null)) {
                        try {
                            stmt = oConn.createStatement();
                        } catch (SQLException se1) {
                            throw new XDBServerException(
                                    ErrorMessageRepository.SQL_STATEMENT_CREATE_FAILURE,
                                    se,
                                    ErrorMessageRepository.SQL_STATEMENT_CREATE_FAILURE_CODE);
                        }
                    }
                }
            }

            try {
                doInsertFromStatement(insertString);
            } catch (SQLException se) {
                if (handleSqlException(se, insertString)) {
                    try {
                        doInsertFromStatement(insertString);
                    } catch (SQLException se1) {
                        XDBServerException ex = new XDBServerException(
                                se1.getMessage() + "\nQUERY: "
                                        + sCreateStatement, se1,
                                ErrorMessageRepository.SQL_EXEC_FAILURE_CODE);
                        logger.throwing(ex);
                        throw ex;
                    }
                }
            }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * returns the number of items passed believed to be inserted
     * 
     * @return the number of items passed believed to be inserted
     */
    // 
    public long getRowCount() {
        return insCount;
    }

    /**
     * 
     * @throws java.sql.SQLException 
     */

    private void doFinishInserts() throws SQLException {
        if (USEBATCHES && iBatchCount > 0) {
            stmt.executeBatch();
            stmt.close();
            stmt = null;
        }

        if (!oConn.getAutoCommit()) {
            oConn.commit();
        }
    }

    /**
     * Finishes any remaining inserts
     */
    public synchronized void finishInserts() {
        final String method = "finishInserts";
        logger.entering(method);

        try {

            try {
                doFinishInserts();
            } catch (SQLException se) {
                if (handleSqlException(se, null)) {
                    try {
                        doFinishInserts();
                    } catch (SQLException se1) {
                        XDBServerException ex = new XDBServerException(
                                se1.getMessage() + "\nQUERY: "
                                        + sCreateStatement, se1,
                                ErrorMessageRepository.SQL_EXEC_FAILURE_CODE);
                        logger.throwing(ex);
                        throw ex;
                    }
                }
            }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * 
     * @param queryString 
     * @throws java.sql.SQLException 
     * @return 
     */

    private ResultSet doQuery(String queryString) throws SQLException {
        final String method = "doQuery";
        logger.entering(method, new Object[] { queryString });
        try {

            Statement aStatement = oConn.createStatement();
            return aStatement.executeQuery(queryString);
        } catch (SQLException se) {
            try {
                oConn.rollback();
            } catch (SQLException ignore) {
            }
            throw se;
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * 
     * @param queryString 
     * @return 
     */

    public synchronized ResultSet queryOnCoord(String queryString) {
        final String method = "queryOnCoord";
        logger.entering(method, new Object[] { queryString });
        try {

            ResultSet rs = null;
            try {
                rs = doQuery(queryString);
            } catch (SQLException se) {
                if (handleSqlException(se, queryString)) {
                    try {
                        rs = doQuery(queryString);
                    } catch (SQLException se1) {
                        XDBServerException ex = new XDBServerException(
                                se1.getMessage() + "\nQUERY: "
                                        + sCreateStatement, se1,
                                ErrorMessageRepository.SQL_EXEC_FAILURE_CODE);
                        logger.throwing(ex);
                        throw ex;
                    }
                }
            }
            return rs;

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * 
     * @param queryString 
     * @param aStepDetail 
     * @param aProducerSender 
     * @throws java.sql.SQLException 
     */

    public synchronized void queryOnCoordAndSendToNodes(String queryString,
            StepDetail aStepDetail, ProducerSender aProducerSender)
            throws SQLException {
        final String method = "queryOnCoordAndSendToNodes";
        logger.entering(method, new Object[] { queryString, aStepDetail });
        try {

            ResultSet rs = null; 
            try {
                rs = queryOnCoord(queryString);
                if (rs != null) {
                    aProducerSender.sendToNodes(rs, aStepDetail, oConn, 
                            client.getSysDatabase().getCoordinatorNodeID(), 
                            client.getRequestId());
    
                    rs.close();
                }
            } finally {
                dropTempTables(aStepDetail.dropList);
            }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * 
     * @param dropList 
     */

    public synchronized void dropTempTables(Collection<String> dropList) {
        final String method = "dropTempTables";
        logger.entering(method, new Object[] { dropList });
        try {

            for (String tableName : dropList) {

                String command = "DROP TABLE " + IdentifierHandler.quote(tableName);
                try {
                    execute(command);
                    oConn.commit();
                } catch (SQLException se) {
                    // mds 
                    // handleSqlException was just throwing an
                    // XDBServerException
                    // so the rest of the code did not run
                    /*
                     * if (handleSqlException(se, command)) { try {
                     * execute(command); } catch (SQLException se1) {
                     * logger.catching(se1);
                     *  // Not a critical one, ignore } }
                     */
                    // ok to ignore this one, due to PostgreSQL
                    try {
                        oConn.rollback();
                    } catch (SQLException se2) {
                    }
                }
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Try and handle the SQLException. Compatibility note: Implementation
     * depends on underlying database
     * 
     * @param arg 
     * @param se 
     * @return 
     */
    private boolean handleSqlException(SQLException se, Object arg) {
        logger.catching(se);
        switch (se.getErrorCode()) {
        case -708:
            if (!client.isPersistent()) {
                oConn = client.resetCoordinatorConnection();
                return true;
            }
            break;
        // Unknown table name
        case -4004:
            // If it is DROP TABLE ..., ignore the error
            if (arg instanceof String) {
                String sql = ((String) arg).trim();
                if (sql.length() > 5) {
                    String firstWord = sql.substring(0, 4).toUpperCase();
                    if (firstWord.equals("DROP")) {
                        sql = sql.substring(5).trim();
                        if (sql.length() > 6) {
                            String secondWord = sql.substring(0, 5)
                                    .toUpperCase();
                            if (secondWord.equals("TABLE")) {
                                return false;
                            }

                        }
                    }
                }
            }
            break;
        }

        // PostgreSQL section
        if ("57P01".equals(se.getSQLState())
                || "08006".equals(se.getSQLState())) {
            // Broken connection
            if (!client.isPersistent()) {
                oConn = client.resetCoordinatorConnection();
                return true;
            }
        }
        // End of PostgreSQL section

        XDBServerException ex = new XDBServerException(
                se.getMessage() + "\nQUERY: "
                        + arg, se,
                ErrorMessageRepository.SQL_EXEC_FAILURE_CODE);
        logger.throwing(ex);
        throw ex;
    }
}
