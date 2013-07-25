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

package org.postgresql.stado.parser;

import org.postgresql.stado.common.util.ParseCmdLine;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.misc.Timer;
import org.postgresql.stado.parser.core.CSQLParser;
import org.postgresql.stado.parser.core.ParseException;
import org.postgresql.stado.parser.core.syntaxtree.AddNodeToDB;
import org.postgresql.stado.parser.core.syntaxtree.AlterCluster;
import org.postgresql.stado.parser.core.syntaxtree.AlterTable;
import org.postgresql.stado.parser.core.syntaxtree.AlterTableSpace;
import org.postgresql.stado.parser.core.syntaxtree.AlterUser;
import org.postgresql.stado.parser.core.syntaxtree.AnalyzeDatabase;
import org.postgresql.stado.parser.core.syntaxtree.BeginTransaction;
import org.postgresql.stado.parser.core.syntaxtree.CloseCursor;
import org.postgresql.stado.parser.core.syntaxtree.Cluster;
import org.postgresql.stado.parser.core.syntaxtree.CommitTransaction;
import org.postgresql.stado.parser.core.syntaxtree.CopyData;
import org.postgresql.stado.parser.core.syntaxtree.CreateDatabase;
import org.postgresql.stado.parser.core.syntaxtree.CreateTablespace;
import org.postgresql.stado.parser.core.syntaxtree.CreateUser;
import org.postgresql.stado.parser.core.syntaxtree.Deallocate;
import org.postgresql.stado.parser.core.syntaxtree.DeclareCursor;
import org.postgresql.stado.parser.core.syntaxtree.Delete;
import org.postgresql.stado.parser.core.syntaxtree.DescribeTable;
import org.postgresql.stado.parser.core.syntaxtree.DropDatabase;
import org.postgresql.stado.parser.core.syntaxtree.DropIndex;
import org.postgresql.stado.parser.core.syntaxtree.DropNodeFromDB;
import org.postgresql.stado.parser.core.syntaxtree.DropTablespace;
import org.postgresql.stado.parser.core.syntaxtree.DropUser;
import org.postgresql.stado.parser.core.syntaxtree.DropView;
import org.postgresql.stado.parser.core.syntaxtree.ExecDirect;
import org.postgresql.stado.parser.core.syntaxtree.Explain;
import org.postgresql.stado.parser.core.syntaxtree.FetchCursor;
import org.postgresql.stado.parser.core.syntaxtree.Grant;
import org.postgresql.stado.parser.core.syntaxtree.InsertTable;
import org.postgresql.stado.parser.core.syntaxtree.Kill;
import org.postgresql.stado.parser.core.syntaxtree.RenameTable;
import org.postgresql.stado.parser.core.syntaxtree.Revoke;
import org.postgresql.stado.parser.core.syntaxtree.RollbackTransaction;
import org.postgresql.stado.parser.core.syntaxtree.Select;
import org.postgresql.stado.parser.core.syntaxtree.SelectAddGeometryColumn;
import org.postgresql.stado.parser.core.syntaxtree.SetProperty;
import org.postgresql.stado.parser.core.syntaxtree.ShowAgents;
import org.postgresql.stado.parser.core.syntaxtree.ShowCluster;
import org.postgresql.stado.parser.core.syntaxtree.ShowConstraints;
import org.postgresql.stado.parser.core.syntaxtree.ShowDatabases;
import org.postgresql.stado.parser.core.syntaxtree.ShowIndexes;
import org.postgresql.stado.parser.core.syntaxtree.ShowProperty;
import org.postgresql.stado.parser.core.syntaxtree.ShowStatements;
import org.postgresql.stado.parser.core.syntaxtree.ShowTables;
import org.postgresql.stado.parser.core.syntaxtree.ShowTranIsolation;
import org.postgresql.stado.parser.core.syntaxtree.ShowUsers;
import org.postgresql.stado.parser.core.syntaxtree.ShowViews;
import org.postgresql.stado.parser.core.syntaxtree.ShutdownXDB;
import org.postgresql.stado.parser.core.syntaxtree.StartDatabase;
import org.postgresql.stado.parser.core.syntaxtree.StopDatabase;
import org.postgresql.stado.parser.core.syntaxtree.Truncate;
import org.postgresql.stado.parser.core.syntaxtree.Unlisten;
import org.postgresql.stado.parser.core.syntaxtree.UpdateStats;
import org.postgresql.stado.parser.core.syntaxtree.UpdateTable;
import org.postgresql.stado.parser.core.syntaxtree.VacuumDatabase;
import org.postgresql.stado.parser.core.syntaxtree.WithDef;
import org.postgresql.stado.parser.core.syntaxtree.createIndex;
import org.postgresql.stado.parser.core.syntaxtree.createTable;
import org.postgresql.stado.parser.core.syntaxtree.createView;
import org.postgresql.stado.parser.core.syntaxtree.dropTable;
import org.postgresql.stado.parser.core.syntaxtree.process;
import org.postgresql.stado.parser.core.visitor.DepthFirstRetArguVisitor;

/**
 * 
 */

public class Parser extends DepthFirstRetArguVisitor {

    // BUILD_CUT_START
    public static Timer parseTimer = new Timer();

    // BUILD_CUT_END

    private IXDBSql sqlObject;

    private XDBSessionContext client;
    
    private boolean isWith = false;

    // -------------------------------------------------------------

    /**
     * Constructor
     * @param client
     *          the session context
     */

    public Parser(XDBSessionContext client) {
        this.client = client;
    }

    /**
     * This function is the entry point in to the parser , and is called to
     * parse a SQL statement
     *
     * @param sqlStatement :
     *                SQL Statement to parse
     * @throws ParseException :
     *                 In case it is not able to parse the string
     */

    public void parseStatement(String sqlStatement) throws ParseException {
        // trim whitespace at beginning and end
        if (sqlStatement.length() > 0) {
            int beginIndex = 0;
            int endIndex = sqlStatement.length() - 1;

            while (Character.isWhitespace(sqlStatement.charAt(beginIndex))
                    && beginIndex < endIndex) {
                beginIndex++;
            }

            while ((Character.isWhitespace(sqlStatement.charAt(endIndex)) || Character.isISOControl(sqlStatement.charAt(endIndex)))
                    && beginIndex < endIndex) {
                endIndex--;
            }

            sqlStatement = sqlStatement.substring(beginIndex, endIndex + 1);
        }
        parse(sqlStatement);
    }

    public void parseBulkInsert(String cmd) {
        sqlObject = new SqlBulkInsert(cmd, client);
    }

    public void parseBulkInsertNext(String cmd) {
        sqlObject = new SqlBulkInsertNext(cmd, client);
    }

    public void parseAddDropNode(String cmd) {
        String[] args = ParseCmdLine.splitCmdLine(cmd);
        String[] nodeList = new String[args.length - 1];
        System.arraycopy(args, 1, nodeList, 0, nodeList.length);
        if (args[0].equals("AddNodesToDB")) {
            sqlObject = new SqlAddNodesToDB(nodeList, client);
        } else if (args[0].equals("DropNodesFromDB")) {
            sqlObject = new SqlDropNodesFromDB(nodeList, client);
        }
    }

    public void parseCloseResultSet(String cmd) {
        sqlObject = new SqlDropTempTables(cmd, client);
    }

    @Override
    public Object visit(DropIndex n, Object argu) {
        SqlDropIndex aDropIndex = new SqlDropIndex(client);
        n.accept(aDropIndex, argu);
        sqlObject = aDropIndex;
        return null;
    }

    @Override
    public Object visit(RenameTable n, Object argu) {
        SqlRenameTable aRenameTable = new SqlRenameTable(client);
        n.accept(aRenameTable, argu);
        sqlObject = aRenameTable;
        return null;
    }

    @Override
    public Object visit(UpdateStats n, Object argu) {
        SqlAnalyzeDatabase aSqlAnalyzeDatabase = new SqlAnalyzeDatabase(client);
        n.accept(aSqlAnalyzeDatabase, argu);
        sqlObject = aSqlAnalyzeDatabase;
        return null;
    }

    @Override
    public Object visit(UpdateTable n, Object argu) {
        SqlUpdateTable aUpdateTable = new SqlUpdateTable(client);
        n.accept(aUpdateTable, argu);
        sqlObject = aUpdateTable;
        return null;
    }

    @Override
    public Object visit(InsertTable n, Object argu) {
        SqlInsertTable aSqlInsertTable = new SqlInsertTable(client);
        n.accept(aSqlInsertTable, argu);
        sqlObject = aSqlInsertTable;
        return null;
    }

    @Override
    public Object visit(dropTable n, Object argu) {
        SqlDropTable aSqlDropTable = new SqlDropTable(client);
        n.accept(aSqlDropTable, argu);
        sqlObject = aSqlDropTable;
        return null;
    }

    @Override
    public Object visit(DropTablespace n, Object argu) {
        SqlDropTableSpace aSqlDropTableSpase = new SqlDropTableSpace(client);
        n.accept(aSqlDropTableSpase, argu);
        sqlObject = aSqlDropTableSpase;
        return null;
    }

    /**
     * Grammar production: f0 -> <TABLE_> f1 -> TableName(prn) f2 ->
     * AlterTableActon(prn) f3 -> ( "," AlterTableActon(prn) )*
     */
    @Override
    public Object visit(AlterTable n, Object argu) {
        SqlAlterTable aSqlAlterTable = new SqlAlterTable(client);
        n.accept(aSqlAlterTable, argu);
        sqlObject = aSqlAlterTable;
        return null;
    }

    @Override
    public Object visit(AlterTableSpace n, Object argu) {
        SqlAlterTableSpace aSqlAlterTableSpace = new SqlAlterTableSpace(client);
        n.accept(aSqlAlterTableSpace, argu);
        sqlObject = aSqlAlterTableSpace;
        return null;
    }

    @Override
    public Object visit(ShowAgents n, Object argu) {
        SqlShowAgents aShowAgents = new SqlShowAgents(client);
        n.accept(aShowAgents, argu);
        sqlObject = aShowAgents;
        return null;
    }

    @Override
    public Object visit(ShowCluster n, Object argu) {
        SqlShowCluster aShowCluster = new SqlShowCluster(client);
        n.accept(aShowCluster, argu);
        sqlObject = aShowCluster;
        return null;
    }

    @Override
    public Object visit(ShowDatabases n, Object argu) {
        SqlShowDatabases aShowDatabases = new SqlShowDatabases(client);
        n.accept(aShowDatabases, argu);
        sqlObject = aShowDatabases;
        return null;
    }

    @Override
    public Object visit(ShowStatements n, Object argu) {
        SqlShowStatements aShowStatements = new SqlShowStatements(client);
        n.accept(aShowStatements, argu);
        sqlObject = aShowStatements;
        return null;
    }

    @Override
    public Object visit(ShowTables n, Object argu) {
        SqlShowTables aShowTables = new SqlShowTables(client);
        n.accept(aShowTables, argu);
        sqlObject = aShowTables;
        return null;
    }

    @Override
    public Object visit(BeginTransaction n, Object argu) {
        SqlBeginTransaction aBeginTransaction = new SqlBeginTransaction(client);
        n.accept(aBeginTransaction, argu);
        sqlObject = aBeginTransaction;
        return null;
    }

    @Override
    public Object visit(CommitTransaction n, Object argu) {
        SqlCommitTransaction aCommitTransaction = new SqlCommitTransaction(
                client);
        n.accept(aCommitTransaction, argu);
        sqlObject = aCommitTransaction;
        return null;
    }

    @Override
    public Object visit(RollbackTransaction n, Object argu) {
        SqlRollbackTransaction aRollbackTransaction = new SqlRollbackTransaction(
                client);
        n.accept(aRollbackTransaction, argu);
        sqlObject = aRollbackTransaction;
        return null;
    }

    @Override
    public Object visit(DescribeTable n, Object argu) {
        SqlDescribeTable aSqlDescribeTable = new SqlDescribeTable(client);
        n.accept(aSqlDescribeTable, argu);
        sqlObject = aSqlDescribeTable;
        return null;
    }

    @Override
    public Object visit(ShowConstraints n, Object argu) {
        SqlShowConstraints aSqlShowConstraints = new SqlShowConstraints(client);
        n.accept(aSqlShowConstraints, argu);
        sqlObject = aSqlShowConstraints;
        return null;
    }

    @Override
    public Object visit(ShowIndexes n, Object argu) {
        SqlShowIndexes aSqlShowIndexes = new SqlShowIndexes(client);
        n.accept(aSqlShowIndexes, argu);
        sqlObject = aSqlShowIndexes;
        return null;
    }

    @Override
    public Object visit(createIndex n, Object argu) {
        SqlCreateIndex aSqlCreateIndex = new SqlCreateIndex(client);
        n.accept(aSqlCreateIndex, argu);
        sqlObject = aSqlCreateIndex;
        return null;
    }

    @Override
    public Object visit(createTable n, Object argu) {
        SqlCreateTable aSqlCreateTable = new SqlCreateTable(client);
        n.accept(aSqlCreateTable, argu);
        sqlObject = aSqlCreateTable;
        return null;
    }

    @Override
    public Object visit(CreateTablespace n, Object argu) {
        SqlCreateTableSpace aSqlCreateTableSpase = new SqlCreateTableSpace(
                client);
        n.accept(aSqlCreateTableSpase, argu);
        sqlObject = aSqlCreateTableSpase;
        return null;
    }

    @Override
    public Object visit(createView n, Object argu) {
        SqlCreateView aSqlCreateView = new SqlCreateView(client);
        n.accept(aSqlCreateView, argu);
        sqlObject = aSqlCreateView;
        return null;
    }

    @Override
    public Object visit(CreateUser n, Object argu) {
        SqlCreateUser aCreateUser = new SqlCreateUser(client);
        n.accept(aCreateUser, argu);
        sqlObject = aCreateUser;
        return null;
    }

    @Override
    public Object visit(AlterUser n, Object argu) {
        SqlAlterUser aAlterUser = new SqlAlterUser(client);
        n.accept(aAlterUser, argu);
        sqlObject = aAlterUser;
        return null;
    }

    @Override
    public Object visit(DropUser n, Object argu) {
        SqlDropUser aDropUser = new SqlDropUser(client);
        n.accept(aDropUser, argu);
        sqlObject = aDropUser;
        return null;
    }

    @Override
    public Object visit(DropView n, Object argu) {
        SqlDropView aDropView = new SqlDropView(client);
        n.accept(aDropView, argu);
        sqlObject = aDropView;
        return null;
    }

    @Override
    public Object visit(Deallocate n, Object argu) {
        SqlDeallocate aDeallocate = new SqlDeallocate(client);
        n.accept(aDeallocate, argu);
        sqlObject = aDeallocate;
        return null;
    }

    @Override
    public Object visit(AddNodeToDB n, Object argu) {
        SqlAddNodesToDB addNode = new SqlAddNodesToDB(client);
        n.accept(addNode, argu);
        sqlObject = addNode;
        return null;
    }

    @Override
    public Object visit(DropNodeFromDB n, Object argu) {
        SqlDropNodesFromDB dropNode = new SqlDropNodesFromDB(client);
        n.accept(dropNode, argu);
        sqlObject = dropNode;
        return null;
    }

    @Override
    public Object visit(StartDatabase n, Object argu) {
        SqlStartDatabase startDB = new SqlStartDatabase(client);
        n.accept(startDB, argu);
        sqlObject = startDB;
        return null;
    }

    @Override
    public Object visit(StopDatabase n, Object argu) {
        SqlStopDatabase stopDB = new SqlStopDatabase(client);
        n.accept(stopDB, argu);
        sqlObject = stopDB;
        return null;
    }

    @Override
    public Object visit(AlterCluster n, Object argu) {
        SqlAlterCluster alterCluster = new SqlAlterCluster(client);
        n.accept(alterCluster, argu);
        sqlObject = alterCluster;
        return null;
    }

    @Override
    public Object visit(CreateDatabase n, Object argu) {
        SqlCreateDatabase createDB = new SqlCreateDatabase(client);
        n.accept(createDB, argu);
        sqlObject = createDB;
        return null;
    }

    @Override
    public Object visit(DropDatabase n, Object argu) {
        SqlDropDatabase dropDB = new SqlDropDatabase(client);
        n.accept(dropDB, argu);
        sqlObject = dropDB;
        return null;
    }

    /*
    @Override
    public Object visit(CreateNode n, Object argu) {
        SqlCreateNode createNode = new SqlCreateNode(client);
        n.accept(createNode, argu);
        sqlObject = createNode;
        return null;
    }
*/
    @Override
    public Object visit(ShutdownXDB n, Object argu) {
        SqlShutdownServer shutDownSvr = new SqlShutdownServer(client);
        n.accept(shutDownSvr, argu);
        sqlObject = shutDownSvr;
        return null;
    }

    @Override
    public Object visit(Grant n, Object argu) {
        SqlGrant aSqlGrant = new SqlGrant(client);
        n.accept(aSqlGrant, argu);
        sqlObject = aSqlGrant;
        return null;
    }

    @Override
    public Object visit(Revoke n, Object argu) {
        SqlGrant aSqlRevoke = new SqlGrant(client);
        n.accept(aSqlRevoke, argu);
        sqlObject = aSqlRevoke;
        return null;
    }

    @Override
    public Object visit(ShowViews n, Object argu) {
        SqlShowViews aShowViews = new SqlShowViews(client);
        n.accept(aShowViews, argu);
        sqlObject = aShowViews;
        return null;
    }

    @Override
    public Object visit(ShowProperty n, Object argu) {
        SqlShowProperty aSqlShowProperty = new SqlShowProperty(client);
        n.accept(aSqlShowProperty, argu);
        sqlObject = aSqlShowProperty;
        return null;
    }

    @Override
    public Object visit(ShowTranIsolation n, Object argu) {
        SqlShowTranIsolation aSqlShowTranIsolation = new SqlShowTranIsolation(
                client);
        n.accept(aSqlShowTranIsolation, argu);
        sqlObject = aSqlShowTranIsolation;
        return null;
    }

    @Override
    public Object visit(SetProperty n, Object argu) {
        SqlSetProperty aSetProperty = new SqlSetProperty(client);
        n.accept(aSetProperty, argu);
        sqlObject = aSetProperty;
        return null;
    }

    @Override
    public Object visit(ShowUsers n, Object argu) {
        SqlShowUsers aShowUsers = new SqlShowUsers(client);
        n.accept(aShowUsers, argu);
        sqlObject = aShowUsers;
        return null;
    }

    @Override
    public Object visit(Delete n, Object argu) {
        SqlDeleteTable aDeleteTable = new SqlDeleteTable(client);
        n.accept(aDeleteTable, argu);
        sqlObject = aDeleteTable;
        return null;
    }

    @Override
    public Object visit(VacuumDatabase n, Object argu) {
        SqlAnalyzeDatabase aSqlAnalyzeDatabase = new SqlAnalyzeDatabase(client);
        n.accept(aSqlAnalyzeDatabase, argu);
        sqlObject = aSqlAnalyzeDatabase;
        return null;
    }

    @Override
    public Object visit(AnalyzeDatabase n, Object argu) {
        SqlAnalyzeDatabase aSqlAnalyzeDatabase = new SqlAnalyzeDatabase(client);
        n.accept(aSqlAnalyzeDatabase, argu);
        sqlObject = aSqlAnalyzeDatabase;
        return null;
    }

    @Override
    public Object visit(Cluster n, Object argu) {
        SqlCluster aSqlCluster = new SqlCluster(client);
        n.accept(aSqlCluster, argu);
        sqlObject = aSqlCluster;
        return null;
    }

    @Override
    public Object visit(Truncate n, Object argu) {
        SqlTruncate aSqlTruncate = new SqlTruncate(client);
        n.accept(aSqlTruncate, argu);
        sqlObject = aSqlTruncate;
        return null;
    }

    @Override
    public Object visit(ExecDirect n, Object argu) {
        SqlExecDirect aSqlExecDirect = new SqlExecDirect(client);
        n.accept(aSqlExecDirect, argu);
        sqlObject = aSqlExecDirect;
        return null;
    }

    @Override
    public Object visit(Explain n, Object argu) {
        SqlExplain aSqlExplain = new SqlExplain(client);
        n.accept(aSqlExplain, argu);
        sqlObject = aSqlExplain;
        return null;
    }

    @Override
    public Object visit(Select n, Object argu) {
        // See if we were called because of a WITH statement,
        // in which case use same object
        if (isWith) {
            n.accept((SqlSelect) sqlObject, argu);            
        } else {
            SqlSelect aSqlSelect = new SqlSelect(client);
            n.accept(aSqlSelect, argu);
            sqlObject = aSqlSelect;
        }
        return null;
    }

    @Override
    public Object visit(WithDef n, Object argu) {
        // Using SqlSelect, though we deal with WITH 
        // Note, this may be called multiple times
        // Check if first time
        if (isWith) {
            n.accept((SqlSelect) sqlObject, argu);
        } else {
            SqlSelect aSqlSelect = new SqlSelect(client);
            n.accept(aSqlSelect, argu);
            sqlObject = aSqlSelect;
            isWith = true;
        }
        return null;
    }
        
    @Override
    public Object visit(CopyData n, Object argu) {
        SqlCopyData aSqlCopyData = new SqlCopyData(client);
        n.accept(aSqlCopyData, argu);
        sqlObject = aSqlCopyData;
        return null;
    }

    @Override
    public Object visit(Kill n, Object argu) {
        SqlKill aSqlKill = new SqlKill(client);
        n.accept(aSqlKill, argu);
        sqlObject = aSqlKill;
        return null;
    }

    @Override
    public Object visit(Unlisten  n, Object argu) {
        SqlUnlisten aSqlUnlisten = new SqlUnlisten(client);
        n.accept(aSqlUnlisten, argu);
        sqlObject = aSqlUnlisten;
        return null;
    }
        
    @Override
    public Object visit(SelectAddGeometryColumn n, Object argu) {
    	SqlAddGeometryColumn aSqlAddGeometryColumn = new SqlAddGeometryColumn(client);
        n.accept(aSqlAddGeometryColumn, argu);
        sqlObject = aSqlAddGeometryColumn;
        return null;
    }

    @Override
    public Object visit(DeclareCursor n, Object argu) {
    	SqlDeclareCursor aSqlDeclareCursor = new SqlDeclareCursor(client);
        n.accept(aSqlDeclareCursor, argu);
        sqlObject = aSqlDeclareCursor;
        return null;
    }

    @Override
    public Object visit(CloseCursor n, Object argu) {
    	SqlCloseCursor aSqlCloseCursor = new SqlCloseCursor(client);
        n.accept(aSqlCloseCursor, argu);
        sqlObject = aSqlCloseCursor;
        return null;
    }

    @Override
    public Object visit(FetchCursor n, Object argu) {
    	SqlFetchCursor aSqlFetchCursor = new SqlFetchCursor(client);
        n.accept(aSqlFetchCursor, argu);
        sqlObject = aSqlFetchCursor;
        return null;
    }

    private static boolean isEmptyQuery(String sql){
    	
    	if(sql == null || sql.replaceAll(";", "").trim().length() == 0){
    		return true;
    	}
    	
    	return false;
    }

    private void parse(String str) throws ParseException {
        // BUILD_CUT_START
        parseTimer.startTimer();
        // BUILD_CUT_END
        str = str.trim();

        if(isEmptyQuery(str)){
        	SqlEmptyQuery aSqlEmptyQuery = new SqlEmptyQuery(client);

        	sqlObject = aSqlEmptyQuery;
            // BUILD_CUT_START
            parseTimer.stopTimer();
                
        	return;
        }
        
        if (str.length() > 6) {
            // Try manual parse
            String start = str.substring(0, 6).toUpperCase();
            // only do manual parse if set in xdb.config
            if (Props.XDB_FASTPARSE_SELECT) {
                if ("SELECT".equals(start)) {
                    try {
                        SqlSelect aSqlSelect = new SqlSelect(client);
                        if (aSqlSelect.manualParse(str)) {
                            sqlObject = aSqlSelect;
                            // BUILD_CUT_START
                            parseTimer.stopTimer();
                            // BUILD_CUT_END
                            return;
                        }
                    } catch (Exception e) {
                        // don't do anything, we want to just reparse
                        // more thoroughly below.
                    }
                }
            }

            // only do manual parse if set in xdb.config
            if (Props.XDB_FASTPARSE_INSERT) {
                if ("INSERT".equals(start)) {
                    try {
                        SqlInsertTable aSqlInsertTable = new SqlInsertTable(
                                client);
                        if (aSqlInsertTable.manualParse(str)) {
                            sqlObject = aSqlInsertTable;
                            // BUILD_CUT_START
                            parseTimer.stopTimer();
                            // BUILD_CUT_END
                            return;
                        }
                    } catch (Exception e) {
                        // don't do anything, we want to just reparse
                        // more thoroughly below.
                    }
                }
            }

            // only do manual parse if set in xdb.config
            if (Props.XDB_FASTPARSE_UPDATE) {
                if ("UPDATE".equals(start)) {
                    try {
                        SqlUpdateTable aUpdateTable = new SqlUpdateTable(client);
                        if (aUpdateTable.manualParse(str)) {
                            sqlObject = aUpdateTable;
                            // BUILD_CUT_START
                            parseTimer.stopTimer();
                            // BUILD_CUT_END
                            return;
                        }
                    } catch (Exception e) {
                        // don't do anything, we want to just reparse
                        // more thoroughly below.
                    }
                }
            }

            // only do manual parse if set in xdb.config
            if (Props.XDB_FASTPARSE_DELETE) {
                if ("DELETE".equals(start)) {
                    try {
                        SqlDeleteTable aDeleteTable = new SqlDeleteTable(client);
                        if (aDeleteTable.manualParse(str)) {
                            sqlObject = aDeleteTable;
                            // BUILD_CUT_START
                            parseTimer.stopTimer();
                            // BUILD_CUT_END
                            return;
                        }
                    } catch (Exception e) {
                        // don't do anything, we want to just reparse
                        // more thoroughly below.
                    }
                }
            }
        }

        java.io.StringReader sr = new java.io.StringReader(str);
        CSQLParser sqlParser = new CSQLParser(sr);
        try {
            process proot = sqlParser.process(System.out);
            proot.accept(this, str);
        } catch (ParseException e) {
            throw e;
        } catch (Throwable t) {
            t.printStackTrace();
            throw new ParseException(t.getMessage());
        }
        // For activity logging
        if (Props.XDB_ENABLE_ACTIVITY_LOG && sqlObject instanceof SqlSelect) {
            ((SqlSelect) sqlObject).setSelectString(str);
        }
        // BUILD_CUT_START
        parseTimer.stopTimer();
        // BUILD_CUT_END
    }

    /**
     * @return Returns the sqlObject.
     */
    public IXDBSql getSqlObject() {
        return sqlObject;
    }
} // class Parser

