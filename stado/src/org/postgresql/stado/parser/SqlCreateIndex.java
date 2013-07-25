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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.MetaData;
import org.postgresql.stado.metadata.SyncCreateIndex;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysPermission;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.SysTablespace;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.parser.core.syntaxtree.INode;
import org.postgresql.stado.parser.core.syntaxtree.NodeChoice;
import org.postgresql.stado.parser.core.syntaxtree.NodeOptional;
import org.postgresql.stado.parser.core.syntaxtree.NodeSequence;
import org.postgresql.stado.parser.core.syntaxtree.NodeToken;
import org.postgresql.stado.parser.core.syntaxtree.WhereClause;
import org.postgresql.stado.parser.core.syntaxtree.columnListIndexSpec;
import org.postgresql.stado.parser.core.syntaxtree.createIndex;
import org.postgresql.stado.parser.core.syntaxtree.tablespaceDef;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.IdentifierHandler;
import org.postgresql.stado.parser.handler.QueryConditionHandler;
import org.postgresql.stado.parser.handler.QueryTreeTracker;
import org.postgresql.stado.parser.handler.SQLExpressionHandler;
import org.postgresql.stado.parser.handler.TableNameHandler;

// -----------------------------------------------------------------
public class SqlCreateIndex extends DepthFirstVoidArguVisitor implements IXDBSql,
        IPreparable {
    private static final XLogger logger = XLogger
            .getLogger(SqlCreateIndex.class);

    private XDBSessionContext client;

    private SysDatabase database;

    private SysTable table;

    private Command commandToExecute;

    private String indexName;

    private LinkedList<SqlCreateIndexKey> indexKeyDefinitions;

    private String indexTableName;

    private boolean unique;

    private boolean isSysCreated = false;

    private String createIndexSQL;

    private String usingType = null;

    private String wherePred = null;

    private String tablespaceName = null;

    private SysTablespace tablespace = null;

    /**
     * Constructor
     */
    public SqlCreateIndex(XDBSessionContext client) {
        this.client = client;
        database = client.getSysDatabase();
        indexKeyDefinitions = new LinkedList<SqlCreateIndexKey>();
        commandToExecute = new Command(Command.CREATE, this,
                new QueryTreeTracker(), client);

    }

    /**
     * Grammar production:
     * f0 -> <CREATE_>
     * f1 -> [ <UNIQUE_> ]
     * f2 -> <INDEX_>
     * f3 -> Identifier(prn)
     * f4 -> <ON_>
     * f5 -> TableName(prn)
     * f6 -> [ <USING_> Identifier(prn) ]
     * f7 -> "("
     * f8 -> columnListIndexSpec(prn)
     * f9 -> ")"
     * f10 -> [ tablespaceDef(prn) ]
     * f11 -> [ WhereClause(prn) ]
     */
    @Override
    public void visit(createIndex n, Object argu) {
        IdentifierHandler ih = new IdentifierHandler();
        unique = n.f1.present();
        this.indexName = (String) n.f3.accept(ih, argu);
        TableNameHandler aTableNameHandler = new TableNameHandler(client);
        n.f5.accept(aTableNameHandler, argu);
        this.indexTableName = aTableNameHandler.getTableName();
        if (n.f6.present()) {
            NodeSequence aNodeSequence = (NodeSequence) n.f6.node;
            setUsingType((String) aNodeSequence.elementAt(1).accept(ih, argu));
        }
        n.f8.accept(this, argu);
        n.f10.accept(this, argu);
        n.f11.accept(this, argu);
    }

    /**
     * Grammar production:
     * f0 -> <WHERE_>
     * f1 -> SQLComplexExpression(prn)
     */
    @Override
    public void visit(WhereClause n, Object argu) {
        QueryConditionHandler aQueryConditionHandler = new QueryConditionHandler(commandToExecute);
        n.f1.accept(aQueryConditionHandler, argu);
        setWherePred(aQueryConditionHandler.aRootCondition.rebuildString());
    }

    /**
     * Grammar production:
     * f0 -> <TABLESPACE_>
     * f1 -> Identifier(prn)
     */
    @Override
    public void visit(tablespaceDef n, Object argu) {
        tablespaceName = (String) n.f1.accept(new IdentifierHandler(), argu);
    }

    /**
     * Grammar production:
     * f0 -> ( Identifier(prn) | <PARENTHESIS_START_> SQLSimpleExpression(prn) <PARENTHESIS_CLOSE_> )
     * f1 -> [ <ASC_> | <DESC_> | Identifier(prn) ]
     * f2 -> ( "," ( Identifier(prn) | <PARENTHESIS_START_> SQLSimpleExpression(prn) <PARENTHESIS_CLOSE_> ) [ <ASC_> | <DESC_> | Identifier(prn) ] )*
     */
    @Override
    public void visit(columnListIndexSpec n, Object argu) {
        String indexDefinition = null;
        String keyDirection = null;
        String colOperateor = null;
        IdentifierHandler ih = new IdentifierHandler();
        if (n.f1.present()) {
            if (((NodeChoice) n.f1.node).which == 2) {
                colOperateor = (String) n.f1.node.accept(ih, argu);
            } else {
                keyDirection = ((NodeToken) ((NodeChoice) n.f1.node).choice).tokenImage;
            }
        }
        if (n.f0.which == 0) {
            indexDefinition = (String) n.f0.choice.accept(ih, argu);
            indexKeyDefinitions.add(new SqlCreateIndexKey(indexDefinition,
                    keyDirection, colOperateor));
        } else {
            SQLExpressionHandler handler = new SQLExpressionHandler(commandToExecute);
            ((INode) ((NodeSequence) n.f0.choice).nodes.get(1)).accept(handler,
                    null);
            indexKeyDefinitions.add(new SqlCreateIndexKey(handler.aroot,
                    keyDirection, colOperateor));
        }

        n.f1.accept(this, argu);
        n.f2.accept(this, argu);
        for (Object node : n.f2.nodes) {
            NodeSequence nodeSeq = (NodeSequence) node;
            NodeOptional aOptionalNode = (NodeOptional) nodeSeq.elementAt(2);

            if (aOptionalNode.present()) {
                if (((NodeChoice) aOptionalNode.node).which == 2) {
                    colOperateor = (String) aOptionalNode.node.accept(ih, argu);
                } else {
                    keyDirection = ((NodeToken) ((NodeChoice) aOptionalNode.node).choice).tokenImage;
                }
            }
            NodeChoice nc = (NodeChoice) nodeSeq.elementAt(1);
            if (nc.which == 0) {
                indexDefinition = (String) nc.choice.accept(ih, argu);
                indexKeyDefinitions.add(new SqlCreateIndexKey(indexDefinition,
                        keyDirection, colOperateor));
            } else {
                SQLExpressionHandler handler = new SQLExpressionHandler(
                        commandToExecute);
                ((INode) ((NodeSequence) nc.choice).nodes.get(1)).accept(
                        handler, null);
                indexKeyDefinitions.add(new SqlCreateIndexKey(handler.aroot,
                        keyDirection, colOperateor));
            }
        }
    }

    // This function will get the index definition
    public List<SqlCreateIndexKey> getIndexKeyDefinitions() {
        return indexKeyDefinitions;
    }

    public void setSysCreated(boolean sysCreated) {
        isSysCreated = sysCreated;
    }

    // -----------------------------------
    /**
     * Get if the Create index object is system created or user created
     *
     * @return
     */
    public boolean isSysCreated() {
        return isSysCreated;
    }

    /**
     *
     * @return String The table name on which the index has to be created
     */
    public String getIndexTableName() {
        return indexTableName;
    }

    /**
     *
     * @return The index name
     */
    public String getIndexName() {
        return indexName;
    }

    /**
     *
     * @return Int Number of columns involved in the index keys
     */
    public int getSize() {
        return indexKeyDefinitions.size();
    }

    /**
     *
     * @return true if the index type is of type unique
     */
    public boolean isUnique() {
        return unique;
    }

    public String getTableName() {
        return indexTableName;
    }

    public SysTablespace getTablespace() {
        return tablespace;
    }

    /**
     * This will return the cost of executing this statement in time , milli
     * seconds
     */
    public long getCost() {
        // TODO
        // Actually, cost is roughly equivalent to statement
        // select <indexKeys> from <indexTable> order by <indexKeys>;
        // but we do not want to waste a lot of resources on calculating.
        // Is there way to calculate it quickly ?
        return LOW_COST;
    }

    /**
     * This return the Lock Specification for the system
     *
     * @param theMData
     * @return
     */
    public LockSpecification<SysTable> getLockSpecs() {
        Vector<SysTable> readObjects = new Vector<SysTable>();
        Vector<SysTable> writeObjects = new Vector<SysTable>();
        readObjects.add(database.getSysTable(this.getTableName()));
        return new LockSpecification<SysTable>(readObjects, writeObjects);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Parser.IXDBSql#getNodeList()
     */
    public Collection<DBNode> getNodeList() {
        return new ArrayList<DBNode>(database.getSysTable(this.getTableName())
                .getNodeList());
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IPreparable#isPrepared()
     */
    public boolean isPrepared() {
        final String method = "isPrepared";
        logger.entering(method, new Object[] {});
        try {

            return table != null;

        } finally {
            logger.exiting(method);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IPreparable#prepare()
     */
    public void prepare() throws Exception {
        final String method = "prepare";
        logger.entering(method, new Object[] {});
        try {

            table = database.getSysTable(indexTableName);
            table.ensurePermission(client.getCurrentUser(),
                    SysPermission.PRIVILEGE_INDEX);
            if (table.getSysIndex(indexName) != null) {
                throw new XDBServerException("Index with name "
                        + IdentifierHandler.quote(indexName)
                        + " already exists on table "
                        + IdentifierHandler.quote(indexTableName));
            }
            List<SysColumn> columns = new ArrayList<SysColumn>(
                    indexKeyDefinitions.size());
            for (SqlCreateIndexKey element : indexKeyDefinitions) {
                for (String colName : element.getKeyColumnNames()) {
                    columns.add(table.getSysColumn(colName));
                }
            }
            if (unique) {
                // TODO constraint checker
            }
            if (tablespaceName != null) {
                tablespace = MetaData.getMetaData().getTablespace(
                        tablespaceName);
                for (Object element : table.getNodeList()) {
                    DBNode dbNode = (DBNode) element;
                    if (!tablespace.getLocations().containsKey(
                            new Integer(dbNode.getNodeId()))) {
                        throw new XDBServerException("Tablespace "
                                + IdentifierHandler.quote(tablespaceName)
                                + " does not exist on Node "
                                + dbNode.getNodeId());
                    }
                }
            }
            if (columns.size() != 1
                    || !columns.get(0).equals(table.getSerialColumn())) {
                StringBuffer sbCreateIndex = new StringBuffer("CREATE ");
                sbCreateIndex.append(unique ? "UNIQUE" : "").append(" INDEX ");
                sbCreateIndex.append(indexName).append(" ON ").append(
                        IdentifierHandler.quote(indexTableName));
                if (getUsingType() != null) {
                    sbCreateIndex.append(" USING ");
                    sbCreateIndex.append(getUsingType());
                }
                sbCreateIndex.append(" (");
                for (SqlCreateIndexKey element : indexKeyDefinitions) {
                    sbCreateIndex.append(element.rebuildString());
                    sbCreateIndex.append(", ");
                }
                sbCreateIndex.setLength(sbCreateIndex.length() - 2);
                sbCreateIndex.append(")");
                createIndexSQL = sbCreateIndex.toString();
            }

        } finally {
            logger.exiting(method);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IExecutable#execute(org.postgresql.stado.Engine.Engine)
     */
    public ExecutionResult execute(Engine engine) throws Exception {
        final String method = "execute";
        logger.entering(method, new Object[] {});
        try {

            // TODO constraint checker
            if (tablespace == null) {
                if (getWherePred() != null) {
                    createIndexSQL = createIndexSQL + " WHERE "
                            + getWherePred();
                }
                engine.executeDDLOnMultipleNodes(createIndexSQL, table
                        .getNodeList(), new SyncCreateIndex(this, database),
                        client);
            } else {
                HashMap<DBNode, String> commandMap = new HashMap<DBNode, String>();
                for (Object element : table.getNodeList()) {
                    DBNode dbNode = (DBNode) element;
                    String command = createIndexSQL + " TABLESPACE "
                            + tablespace.getNodeTablespaceName(dbNode.getNodeId());
                    if (getWherePred() != null) {
                        command = command + " WHERE " + getWherePred();
                    }

                    commandMap.put(dbNode, command);
                }
                engine.executeDDLOnMultipleNodes(commandMap,
                        new SyncCreateIndex(this, database), client);
            }
            return ExecutionResult
                    .createSuccessResult(ExecutionResult.COMMAND_CREATE_INDEX);

        } finally {
            logger.exiting(method);
        }
    }

    public String getUsingType() {
        return usingType;
    }

    public void setUsingType(String usingType) {
        this.usingType = usingType;
    }

    public String getWherePred() {
        return wherePred;
    }

    public void setWherePred(String wherePred) {
        this.wherePred = wherePred;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#needCoordinatorConnection()
     */
    public boolean needCoordinatorConnection() {
        return false;
    }

	@Override
	public boolean isReadOnly() {
		// TODO Auto-generated method stub
		return false;
	}

}
