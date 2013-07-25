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
 * SqlCopyData.java
 *
 *
 */

package org.postgresql.stado.parser;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import org.postgresql.stado.common.ColumnMetaData;
import org.postgresql.stado.common.util.OutputFormatter;
import org.postgresql.stado.common.util.ParseCmdLine;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.engine.copy.CopyManager;
import org.postgresql.stado.engine.loader.Loader;
import org.postgresql.stado.engine.loader.LoaderConnectionPool;
import org.postgresql.stado.exception.ColumnNotFoundException;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.NodeDBConnectionInfo;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.optimizer.AttributeColumn;
import org.postgresql.stado.optimizer.OrderByElement;
import org.postgresql.stado.optimizer.QueryTree;
import org.postgresql.stado.optimizer.RelationNode;
import org.postgresql.stado.optimizer.SqlExpression;
import org.postgresql.stado.parser.core.syntaxtree.ColumnNameList;
import org.postgresql.stado.parser.core.syntaxtree.CopyData;
import org.postgresql.stado.parser.core.syntaxtree.FormatDefCSV;
import org.postgresql.stado.parser.core.syntaxtree.FormatDefDelimiter;
import org.postgresql.stado.parser.core.syntaxtree.FormatDefNull;
import org.postgresql.stado.parser.core.syntaxtree.FormatDefOIDS;
import org.postgresql.stado.parser.core.syntaxtree.NodeChoice;
import org.postgresql.stado.parser.core.syntaxtree.NodeSequence;
import org.postgresql.stado.parser.core.syntaxtree.NodeToken;
import org.postgresql.stado.parser.core.syntaxtree.Select;
import org.postgresql.stado.parser.core.syntaxtree.TableName;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.ColumnNameListHandler;
import org.postgresql.stado.parser.handler.OrderByClauseHandler;
import org.postgresql.stado.parser.handler.QueryTreeHandler;
import org.postgresql.stado.parser.handler.QueryTreeTracker;
import org.postgresql.stado.parser.handler.TableNameHandler;
import org.postgresql.stado.planner.NodeUsage;
import org.postgresql.stado.queryproc.QueryProcessor;

/**
 *
 *
 */
public class SqlCopyData extends DepthFirstVoidArguVisitor implements IXDBSql,
        IPreparable {
    private static final XLogger logger = XLogger.getLogger(SqlCopyData.class);

    private XDBSessionContext client;

    private boolean prepared = false;

    // COPY FROM = true COPY TO = false
    private boolean copyIn;

    // Null if COPY TO with query
    private String tableName;

    // Null if column list is missed
    private List<String> columnNameList;

    private QueryTree aQueryTree = null;

    private SysTable table;

    private QueryProcessor qProcessor;

    private String copyCommand = null;

    // Null if from/to console
    private String filename;

    // Options
    private boolean binary;

    private boolean header;

    private boolean oids;

    private String delimiter = null;

    private String nulls = null;

    private boolean csv;

    private String quote = "\"";

    private String quoteEscape = null;

    private List<String> forceQuoteColumns = null;

    private Loader loader;

    private OutputFormatter formatter;

    LockSpecification<SysTable> lockSpec;

    private InputStream consoleIn = null;

    private OutputStream consoleOut = null;

    /** Creates a new instance of SqlCopyData */
    public SqlCopyData(XDBSessionContext client) {
        this.client = client;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IPreparable#isPrepared()
     */
    public boolean isPrepared() {
        return prepared;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Parser.IXDBSql#getNodeList()
     */
    public Collection<DBNode> getNodeList() {
        return Collections.emptyList();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#getCost()
     */
    public long getCost() {
        return LOW_COST;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#getLockSpecs()
     */
    public LockSpecification<SysTable> getLockSpecs() {
        if (lockSpec == null) {
            if (qProcessor == null) {
                Collection<SysTable> empty = Collections.emptyList();
                lockSpec = new LockSpecification<SysTable>(Collections
                        .singleton(getSysTable()), empty);
            } else {
                lockSpec = qProcessor.getLockSpecs();
            }
        }
        return lockSpec;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#needCoordinatorConnection()
     */
    public boolean needCoordinatorConnection() {
        return false;
    }

    public void setStdOut(OutputStream os) {
        consoleOut = os;
    }

    public void setStdIn(InputStream is) {
        consoleIn = is;
    }

    public boolean isCopyIn() {
        return copyIn;
    }

    public boolean isConsole() {
        return filename == null;
    }

    public ColumnMetaData[] getColumnMeta() {
        if (getSysTable() == null) {
            if (qProcessor != null) {
                return qProcessor.getMetaData();
            }
        } else {
            if (columnNameList == null) {
                List<ColumnMetaData> result = new ArrayList<ColumnMetaData>(
                        table.getColumns().size());
                for (SysColumn column : table.getColumns()) {
                    if (!column.getColName().equals(
                            SqlCreateTableColumn.XROWID_NAME)) {
                        result.add(new ColumnMetaData(column.getColName(),
                                null, column.getColLength(), column
                                        .getColType(),
                                column.getColPrecision(), column.getColScale(),
                                tableName, (short) 0, false));
                    }
                }
                return result.toArray(new ColumnMetaData[result.size()]);
            } else {
                ColumnMetaData[] result = new ColumnMetaData[columnNameList
                        .size()];
                int i = 0;
                for (String colName : columnNameList) {
                    SysColumn column = table.getSysColumn(colName);
                    if (column == null) {
                        throw new XDBServerException("Column " + colName
                                + " is not found");
                    }
                    result[i++] = new ColumnMetaData(column.getColName(), null,
                            column.getColLength(), column.getColType(), column
                                    .getColPrecision(), column.getColScale(),
                            tableName, (short) 0, false);
                }
                return result;
            }
        }
        return null;
    }

    private SysTable getSysTable() {
        if (table == null && tableName != null) {
            table = client.getSysDatabase().getSysTable(tableName);
        }
        return table;
    }

    /*
     * (non-Javadoc)
     *
     * @see  org.postgresql.stado.Engine.IPreparable#prepare()
     */
    public void prepare() throws Exception {
        final String method = "prepare";
        logger.entering(method);
        try {

            prepared = true;
            if (copyIn) {
                if (aQueryTree != null) {
                    throw new XDBServerException("\"TO\" is expected but \"FROM\" is found");
                }
                if (delimiter == null) {
                    delimiter = csv ? "," : Props.XDB_LOADER_NODEWRITER_DEFAULT_DELIMITER;
                }
                if (nulls == null) {
                    nulls = csv ? "" : Props.XDB_LOADER_NODEWRITER_DEFAULT_NULL;
                }
                loader = new Loader(delimiter, nulls);
                getSysTable();
                loader.setLocalTableInfo(table, client, columnNameList, oids);
                if (binary) {
                    // TODO handle
                }
                if (header) {
                    // TODO handle
                }
                if (csv) {
                    char quoteChar = quote == null || quote.length() == 0 ? '"' : quote.charAt(0);
                    char quoteEscapeChar = quoteEscape == null || quoteEscape.length() == 0 ? quoteChar : quoteEscape.charAt(0);
                    loader.setQuotes(quoteChar, quoteEscapeChar);
                    if (forceQuoteColumns != null) {
                        loader.setForceNotNullColumns(forceQuoteColumns);
                    }
                }
                loader.prepareLoad();
                loader.setVerbose(logger.isDebugEnabled());
            } else {
            	if (aQueryTree != null) {
	                qProcessor = new QueryProcessor(client, aQueryTree);
	                qProcessor.prepare();
            	}

                if (aQueryTree == null || qProcessor != null && qProcessor.getExecPlan().isSingleStep()) {
                	if (Props.XDB_USE_COPY_OUT_FOR_STEP) {
                		StringBuffer sbCopyCommand = new StringBuffer("COPY ");
                		if (qProcessor != null) {
                			sbCopyCommand.append("(");
                			sbCopyCommand.append(qProcessor.getExecPlan().stepList.get(0).aStepDetail.queryString);
                		} else {
                            table = client.getSysDatabase().getSysTable(tableName);
	                		sbCopyCommand.append(table.getTableName());
	                		sbCopyCommand.append("(");
	                    	if (columnNameList == null ) {
		                        for (SysColumn aSysColumn : table.getColumns()) {
		                        	if (!oids && aSysColumn.getColName().equalsIgnoreCase(SqlCreateTableColumn.XROWID_NAME)) {
		                        		continue;
		                        	}
	                    			sbCopyCommand.append(aSysColumn.getColName());
	                    			sbCopyCommand.append(", ");
		                        }
	                    	} else {
	                    		for (String colName : columnNameList) {
	                                SysColumn aSysColumn = table.getSysColumn(colName);
	                                if (aSysColumn == null) {
	                                    throw new XDBServerException("Column "
	                                            + colName + " is not found");
	                                }
	                    			sbCopyCommand.append(aSysColumn.getColName());
	                    			sbCopyCommand.append(", ");
	                    		}
	                    		if (oids) {
	                                SysColumn aSysColumn = table.getSysColumn(SqlCreateTableColumn.XROWID_NAME);
	                                if (aSysColumn != null) {
	                                	sbCopyCommand.append(aSysColumn.getColName());
	                                	sbCopyCommand.append(", ");
	                                }
	                    		}
	                    	}
	                    	sbCopyCommand.setLength(sbCopyCommand.length() - 2);
                		}
            			sbCopyCommand.append(") TO STDOUT");
            			if (delimiter != null) {
                			sbCopyCommand.append(" DELIMITER '");
                			sbCopyCommand.append(delimiter.replaceAll("'", "''"));
                			sbCopyCommand.append("'");
            			}
            			if (nulls != null) {
                			sbCopyCommand.append(" NULL '");
                			sbCopyCommand.append(nulls.replaceAll("'", "''"));
                			sbCopyCommand.append("'");
            			}
            			if (csv) {
                			sbCopyCommand.append(" CSV");
                			if (quote != null) {
                    			sbCopyCommand.append(" QUOTE '");
                    			sbCopyCommand.append(quote.replaceAll("'", "''"));
                    			sbCopyCommand.append("'");
                			}
                			if (quoteEscape != null) {
                    			sbCopyCommand.append(" ESCAPE '");
                    			sbCopyCommand.append(quoteEscape.replaceAll("'", "''"));
                    			sbCopyCommand.append("'");
                			}
                			if (forceQuoteColumns != null) {
                    			sbCopyCommand.append(" FORCE QUOTE ");
                    			for (String colName : forceQuoteColumns) {
                    				sbCopyCommand.append(colName);
                    				sbCopyCommand.append(", ");
                    			}
                            	sbCopyCommand.setLength(sbCopyCommand.length() - 2);
                			}
            			}
            			copyCommand = sbCopyCommand.toString();
            			return;
                	}
                    table = client.getSysDatabase().getSysTable(tableName);
                    aQueryTree = new QueryTree();
                    RelationNode aRelationNode = aQueryTree.newRelationNode();
                    aRelationNode.setNodeType(RelationNode.TABLE);

                    aRelationNode.setTableName(tableName);
                    aRelationNode.setTemporaryTable(client
                            .getTempTableName(tableName) != null);
                    aRelationNode.setClient(client);
                    aRelationNode.setAlias("");
                    if (columnNameList == null) {
                        for (SysColumn aSysColumn : table.getColumns()) {
                            if (!oids
                                    && aSysColumn
                                            .getColName()
                                            .equalsIgnoreCase(
                                                    SqlCreateTableColumn.XROWID_NAME)) {
                                continue;
                            }
                            // now we can add the column.
                            createColumnExpression(aRelationNode, aSysColumn);
                        }
                    } else {
                        for (String colName : columnNameList) {
                            SysColumn column = table.getSysColumn(colName);
                            if (column == null) {
                                throw new XDBServerException("Column "
                                        + colName + " is not found");
                            }

                            // now we can add the column.
                            createColumnExpression(aRelationNode, column);
                        }
                        if (oids) {
                            createColumnExpression(
                                    aRelationNode,
                                    table.getSysColumn(SqlCreateTableColumn.XROWID_NAME));
                        }
                    }
                }

                formatter = new OutputFormatter(null);
                if (delimiter == null) {
                    delimiter = csv ? "," : Props.XDB_LOADER_NODEWRITER_DEFAULT_DELIMITER;
                }
                if (nulls == null) {
                    nulls = csv ? "" : Props.XDB_LOADER_NODEWRITER_DEFAULT_NULL;
                }
                formatter.setSFieldSep(delimiter);
                formatter.setNullValue(nulls);
                if (binary) {
                    // TODO handle
                }
                if (header) {
                    // TODO handle
                }
                if (csv) {
                    formatter.setQuoteInfo(quote, quoteEscape, forceQuoteColumns);
                }
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * @param aRelationNode
     * @param aSysColumn
     * @return
     */
    private void createColumnExpression(RelationNode aRelationNode,
            SysColumn aSysColumn) {
        ExpressionType anET = new ExpressionType();
        anET.type = aSysColumn.getColType();
        anET.length = aSysColumn.getColumnLength();

        AttributeColumn anAC = new AttributeColumn();
        anAC.setTableName(tableName);
        anAC.columnName = aSysColumn.getColName();
        // anAC.columnAlias = columnAlias;

        // there should just be one relationNode here
        anAC.relationNode = aRelationNode;

        SqlExpression aSE = new SqlExpression();

        aSE.setExprType(SqlExpression.SQLEX_COLUMN);
        aSE.setColumn(anAC);
        aSE.setExprDataType(anET);

        aRelationNode.getProjectionList().add(aSE);
        aQueryTree.getProjectionList().add(aSE);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IExecutable#execute(org.postgresql.stado.Engine.Engine)
     */
    public ExecutionResult execute(Engine engine) throws Exception {
        final String method = "execute";
        logger.entering(method, new Object[] { engine });
        try {
            if (!isPrepared()) {
                prepare();
            }

            if (copyIn) {
                if (filename == null) {
                    loader.setDataSource(consoleIn);
                } else {
                    loader.setDataSource(filename);
                }
                boolean success = false;
                try {
                    loader.runWriters();
                    success = true;
                } finally {
                    loader.finishLoad(success);
                }
                long lines = loader.getRowCount();
                if (table.isLookup()) {
                    lines /= table.getNodeList().size();
                }
                return ExecutionResult.createRowCountResult(
                        ExecutionResult.COMMAND_COPY_IN, (int) lines);
            } else {
                long lines = 0;
                OutputStream out = isConsole() ? consoleOut : new FileOutputStream(filename);
                try {
	            	if (copyCommand != null) {
	            		List<NodeDBConnectionInfo> nodeInfos = new LinkedList<NodeDBConnectionInfo>();
	            		if (table == null) {
	            			for (NodeUsage nu : qProcessor.getExecPlan().stepList.get(0).nodeUsageTable.values()) {
	            				if (nu.isProducer) {
                                    nodeInfos.add(client.getSysDatabase().getDBNode(nu.nodeId).getNodeDBConnectionInfo());
                                }
	            			}
	            		} else {
		            		for (DBNode dbNode : table.getJoinNodeList()) {
                                nodeInfos.add(dbNode.getNodeDBConnectionInfo());
                            }
	            		}
            			LoaderConnectionPool pool = LoaderConnectionPool.getConnectionPool();
	            		for (NodeDBConnectionInfo connectionInfo : nodeInfos) {
	                        Connection conn = pool.getConnection(connectionInfo);
	                        try {
                                conn.createStatement().executeUpdate("BEGIN");
	                        	lines += CopyManager.getCopyManager(conn).copyOut(copyCommand, out);
                                conn.createStatement().executeUpdate("COMMIT");
	                        } finally {
	                        	pool.releaseConnection(connectionInfo, conn);
	                        }
	            		}
	                    return ExecutionResult.createRowCountResult(
	                            ExecutionResult.COMMAND_COPY_OUT, (int) lines);
	            	}
	                ExecutionResult result = qProcessor.execute(engine);
	                ResultSet rs = result.getResultSet();
	                try {
		                if (isConsole()) {
		                    formatter.setSTerminator("\n");
		                }
	                	lines = formatter.printRS(rs, out);
	                } finally {
	                	rs.close();
	                }
	                return ExecutionResult.createRowCountResult(
	                        ExecutionResult.COMMAND_COPY_OUT, (int) lines);
                } finally {
                	out.close();
                }
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Grammar production:
     * f0 -> <COPY_>
     * f1 -> ( TableName(prn) [ ColumnNameListWithParenthesis(prn) ] | "(" Select(prn) ")" )
     * f2 -> ( <FROM_> ( <STDIN_> | <STRING_LITERAL> ) | <TO_> ( <STDOUT_> | <STRING_LITERAL> ) )
     * f3 -> [ [ <WITH_> ] ( FormatDefOIDS(prn) | FormatDefDelimiter(prn) | FormatDefNull(prn) | FormatDefCSV(prn) )+ ]
     */
    @Override
    public void visit(CopyData n, Object argu) {
        n.f1.accept(this, argu);
        copyIn = n.f2.which == 0;
        NodeSequence ns = (NodeSequence) n.f2.choice;
        NodeChoice nc = (NodeChoice) ns.nodes.get(1);
        if (nc.which == 1) {
            NodeToken nt = (NodeToken) nc.choice;
            filename = nt.tokenImage;
            filename = filename.substring(1, filename.length() - 1).replaceAll(
                    "''", "'");
        }
        n.f3.accept(this, argu);
    }

    /**
     * Grammar production:
     * f0 -> <CSV_>
     * f1 -> ( <QUOTE_STRING_> [ <AS_> ] <STRING_LITERAL> | <ESCAPE_> [ <AS_> ] <STRING_LITERAL> | <FORCE_QUOTE_> [ Identifier(prn) ( "," Identifier(prn) )* ] | <FORCE_NOT_NULL_> [ Identifier(prn) ( "," Identifier(prn) )* ] )*
     */
    @Override
    public void visit(FormatDefCSV n, Object argu) {
        csv = true;
        for (Iterator it1 = n.f1.nodes.iterator(); it1.hasNext();) {
            NodeChoice csvOption = (NodeChoice) it1.next();
            switch (csvOption.which) {
            case 0:
                // <QUOTE_STRING_> [ <AS_> ] <STRING_LITERAL>
                NodeSequence ns = (NodeSequence) csvOption.choice;
                NodeToken nt = (NodeToken) ns.nodes.get(2);
                quote = getTokenImage(nt.tokenImage);
                break;
            case 1:
                // <ESCAPE_> [ <AS_> ] <STRING_LITERAL>
                ns = (NodeSequence) csvOption.choice;
                nt = (NodeToken) ns.nodes.get(2);
                quoteEscape = getTokenImage(nt.tokenImage);
                break;
            case 2:
                // <FORCE_QUOTE_> IdentifierAndUnreservedWords(prn) ( "," IdentifierAndUnreservedWords(prn) )*
                // COPY .. TO only
                if (copyIn) {
                    throw new XDBServerException("FORCE QUOTE is not allowed in COPY FROM mode");
                } else {
                    ColumnNameListHandler aHandler = new ColumnNameListHandler();
                    csvOption.accept(aHandler, argu);
                    forceQuoteColumns = aHandler.getColumnNameList();
                }
                break;
            case 3:
                // <FORCE_NOT_NULL_> IdentifierAndUnreservedWords(prn) ( "," IdentifierAndUnreservedWords(prn) )*
                // COPY .. FROM only
                if (!copyIn) {
                    throw new XDBServerException("FORCE NOT NULL is not allowed in COPY TO mode");
                } else {
                    ColumnNameListHandler aHandler = new ColumnNameListHandler();
                    csvOption.accept(aHandler, argu);
                    forceQuoteColumns = aHandler.getColumnNameList();
                }
                break;
            }
        }
    }


    /**
     * Grammar production:
     * f0 -> <DELIMITER_>
     * f1 -> [ <AS_> ]
     * f2 -> <STRING_LITERAL>
     */
    @Override
    public void visit(FormatDefDelimiter n, Object argu) {
        delimiter = getTokenImage(n.f2.tokenImage);
    }


    /**
     * Grammar production:
     * f0 -> <NULL_>
     * f1 -> [ <AS_> ]
     * f2 -> <STRING_LITERAL>
     */
    @Override
    public void visit(FormatDefNull n, Object argu) {
        nulls = getTokenImage(n.f2.tokenImage);
    }


    /**
     * Grammar production:
     * f0 -> <OIDS_>
     */
    @Override
    public void visit(FormatDefOIDS n, Object argu) {
        oids = true;
    }


    @Override
    public void visit(TableName n, Object argu) {
        TableNameHandler aTableNameHandler = new TableNameHandler(client);
        n.accept(aTableNameHandler, argu);
        tableName = aTableNameHandler.getTableName();
    }

    @Override
    public void visit(ColumnNameList n, Object obj) {
        ColumnNameListHandler aHandler = new ColumnNameListHandler();
        n.accept(aHandler, obj);
        columnNameList = aHandler.getColumnNameList();
    }

    @Override
    public void visit(Select n, Object obj) {
        Command commandToExecute = new Command(Command.INSERT, this,
                new QueryTreeTracker(), client);
        QueryTreeHandler aSelectQuery = new QueryTreeHandler(commandToExecute);
        aQueryTree = new QueryTree();
        n.f0.accept(aSelectQuery, aQueryTree);
        OrderByClauseHandler aOrderByClauseHandler = new OrderByClauseHandler(
                commandToExecute);
        n.f1.accept(aOrderByClauseHandler, aQueryTree);
        aQueryTree.setOrderByList(aOrderByClauseHandler.orderByList);
        aQueryTree.preProcessOrderByList();
        QueryTreeHandler.FillSQLExpressionInformation(aQueryTree, 
                aQueryTree.getOrderByList(), commandToExecute);
        n.f2.accept(aSelectQuery, aQueryTree);
        n.f3.accept(aSelectQuery, aQueryTree);
        if (aQueryTree.isHasUnion()) {
            for (int i = 0; i < aQueryTree.getUnionQueryTreeList().size(); i++) {
                aQueryTree.getUnionQueryTreeList().get(i)
                        .checkExpressionTypes(aQueryTree.getProjectionList());
            }
        }
    }

    private String getTokenImage(String image) {
        image = ParseCmdLine.stripEscapes(image);
        return image.substring(1, image.length() - 1).replaceAll("''", "'");
    }

	@Override
	public boolean isReadOnly() {
		// TODO Auto-generated method stub
		return false;
	}
}
