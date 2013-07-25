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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.MultinodeExecutor;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBSecurityException;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.IMetaDataUpdate;
import org.postgresql.stado.metadata.MetaData;
import org.postgresql.stado.metadata.SyncCreateTable;
import org.postgresql.stado.metadata.SyncTempCreateTable;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysIndex;
import org.postgresql.stado.metadata.SysLogin;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.SysTablespace;
import org.postgresql.stado.metadata.partitions.HashPartitionMap;
import org.postgresql.stado.metadata.partitions.PartitionMap;
import org.postgresql.stado.metadata.partitions.ReplicatedPartitionMap;
import org.postgresql.stado.metadata.partitions.RobinPartitionMap;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.optimizer.AttributeColumn;
import org.postgresql.stado.optimizer.QueryCondition;
import org.postgresql.stado.optimizer.QueryTree;
import org.postgresql.stado.optimizer.SqlExpression;
import org.postgresql.stado.parser.core.syntaxtree.CheckDef;
import org.postgresql.stado.parser.core.syntaxtree.ColumnNameList;
import org.postgresql.stado.parser.core.syntaxtree.Constraint;
import org.postgresql.stado.parser.core.syntaxtree.CreateDefinition;
import org.postgresql.stado.parser.core.syntaxtree.NodeChoice;
import org.postgresql.stado.parser.core.syntaxtree.NodeOptional;
import org.postgresql.stado.parser.core.syntaxtree.NodePartitionList;
import org.postgresql.stado.parser.core.syntaxtree.NodeSequence;
import org.postgresql.stado.parser.core.syntaxtree.NodeToken;
import org.postgresql.stado.parser.core.syntaxtree.OnCommitClause;
import org.postgresql.stado.parser.core.syntaxtree.PartitionChoice;
import org.postgresql.stado.parser.core.syntaxtree.PartitionDeclare;
import org.postgresql.stado.parser.core.syntaxtree.PrimaryKeyDef;
import org.postgresql.stado.parser.core.syntaxtree.SelectWithoutOrderAndSet;
import org.postgresql.stado.parser.core.syntaxtree.TableName;
import org.postgresql.stado.parser.core.syntaxtree.WithXRowID;
import org.postgresql.stado.parser.core.syntaxtree.createTable;
import org.postgresql.stado.parser.core.syntaxtree.inheritsDef;
import org.postgresql.stado.parser.core.syntaxtree.tablespaceDef;
import org.postgresql.stado.parser.core.visitor.DepthFirstRetArguVisitor;
import org.postgresql.stado.parser.handler.ColumnNameListHandler;
import org.postgresql.stado.parser.handler.ForeignKeyHandler;
import org.postgresql.stado.parser.handler.IdentifierHandler;
import org.postgresql.stado.parser.handler.QueryConditionHandler;
import org.postgresql.stado.parser.handler.QueryTreeHandler;
import org.postgresql.stado.parser.handler.QueryTreeTracker;
import org.postgresql.stado.parser.handler.TableNameHandler;
import org.postgresql.stado.queryproc.QueryProcessor;

// PreCConditions for engines working

/*
 * a) Does not require "("
 *
 * b) Just concatenates the "originaldefiniton" in to the sql statement...
 *
 * We might like to change this to re creation
 * of SQL statement using a mapper.
 *
 */
// -----------------------------------------------------------------
public class SqlCreateTable extends DepthFirstRetArguVisitor implements IXDBSql,
        IPreparable {
    private static final XLogger logger = XLogger.getLogger(SqlCreateTable.class);

    private XDBSessionContext client;

    private SysDatabase database;

    private Command commandToExecute;

    // Holds the table Name for the table to be created
    private String referenceName;

    private String tableName;

    public boolean temporary;

    // Contains the column definitions - SqlCreateTableColumn
    public List<SqlCreateTableColumn> columnDefinitions;

    // Holds the columnName on which this table is partitioned - Provided
    // the table is partitioned.The varialble holds no value other wise
    public String partColumn;

    // The partition scheme used - The only schemes implemented as yet are
    // PTYPE-HASH - A hash function is used to decide on which node the
    // row is stored.
    // PTYPE_LOOKUP - Table is replicated on all nodes
    // PTYPE_ONE - Only one node holds the table
    // PTYPE_SPECIFIC_NODE -- Not implemented as yet
    private int partScheme = SysTable.PTYPE_DEFAULT;

    // This will have all the foreignKey relationship
    private List<ForeignKeyHandler> aForeignKeyDefsVector = new ArrayList<ForeignKeyHandler>();

    // This will contain the column Names only and not the SqlCreateTableColumn
    private List<String> primaryKeyNameColList;

    private Vector<String> checkList;

    private Vector<String> inheritsTables;

    private SysTable parentTable = null;

    private String pKConstraintName = null;

    private String constraintName = null;

    private Vector<String> checkConstraintName = new Vector<String>();

    // This field is for storing the nodes on which the user will like to
    // distiribute his database.
    private Collection<Integer> partitionNodeList = null;

    private PartitionMap partitionMap;

    // -----------------------------------

    private String rowidIndexName;

    private String tablespaceName = null;

    private SysTablespace tablespace = null;

    private String serialColumnIndexString = null;

    private String createRowIDIndex = null;

    private String sqlStatement = null;

    // For CREATE TABLE AS
    private QueryProcessor qProcessor;

    private QueryTree aQueryTree = null;

    private List<String> colNameList;

    private ArrayList<DBNode> nodeList;

    /**
     * Constructor
     */
    public SqlCreateTable(XDBSessionContext client) {
        columnDefinitions = new ArrayList<SqlCreateTableColumn>();
        checkList = new Vector<String>();
        this.client = client;
        database = client.getSysDatabase();
        commandToExecute = new Command(Command.CREATE, this,
                new QueryTreeTracker(), client);
    }

    /**
     * Gives the static cost of creating a table
     */
    public long getCost() {
        return HIGH_COST;
    }

    /**
     * This is for getting the lock specs
     */
    public LockSpecification<SysTable> getLockSpecs() {
        Vector<SysTable> readTable = new Vector<SysTable>();
        Vector<SysTable> writeTable = new Vector<SysTable>();

        for (ForeignKeyHandler aFkDef : aForeignKeyDefsVector) {
            readTable.add(aFkDef.getForeignTable());
        }
        return new LockSpecification<SysTable>(readTable, writeTable);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Parser.IXDBSql#getNodeList()
     */
    public Collection<Integer> getPartitionList() {
        if (partitionNodeList == null) {
            defaultPartition();
        }
        return partitionNodeList;
    }

    /**
     * Grammar production:
     * f0 -> ColumnDeclare(prn)
     *       | [ Constraint(prn) ] ( PrimaryKeyDef(prn) | ForeignKeyDef(prn) | CheckDef(prn) )
     */
    @Override
    public Object visit(CreateDefinition n, Object argu) {
        Object _ret = null;
        switch (n.f0.which) {
        case 0:
            // create a new column here and then have the this parameter be
            // changed
            // so that we can have the values filled in the new column
            // declaration
            SqlCreateTableColumn col = new SqlCreateTableColumn(
                    commandToExecute);
            // Just pass the responsibilty to the SqlCreateTableColumn
            n.f0.accept(col, argu);
            if (col.isPrimaryKey) // [ <PRIMARY_> <KEY_> ]
            {
                isDefinePrimaryKey();
                primaryKeyNameColList = new ArrayList<String>();
                primaryKeyNameColList.add(col.columnName);
            }
            columnDefinitions.add(col);
            break;
        case 1:
            NodeSequence ns = (NodeSequence) n.f0.choice;
            NodeChoice nc = (NodeChoice) ns.elementAt(1);
            if (((NodeOptional) ns.elementAt(0)).present()) {
                constraintName = (String) ns.elementAt(0).accept(this, argu);
            }
            switch (nc.which) {
            case 0:
            case 2:
                nc.accept(this, argu);
                break;
            case 1:
                ForeignKeyHandler fkDef = new ForeignKeyHandler(client);
                n.accept(fkDef, argu);
                aForeignKeyDefsVector.add(fkDef);
                break;
            }
            break;

        }
        return _ret;
    }

    /**
     * Grammar production:
     * f0 -> <CHECK_>
     * f1 -> "("
     * f2 -> skip_to_matching_brace(prn)
     * f3 -> ")"
     */
    @Override
    public Object visit(CheckDef n, Object argu) {
        Object _ret = null;
        if (constraintName != null) {
            checkConstraintName.add(constraintName);
            constraintName = null;
        } else {
            checkConstraintName.add("");
        }

        QueryConditionHandler qch = new QueryConditionHandler(commandToExecute);
        n.f2.accept(qch, argu);
        checkList.add(qch.aRootCondition.rebuildString());
        return _ret;
    }

    /**
     * Grammar production: f0 -> <INHERITS_> f1 -> "(" f2 -> TableName(prn) f3 -> (
     * "," TableName(prn) )* f4 -> ")"
     */
    @Override
    public Object visit(inheritsDef n, Object argu) {
        Object _ret = null;
        inheritsTables = new Vector<String>();
        TableNameHandler aTableHandler = new TableNameHandler(client);
        n.f2.accept(aTableHandler, argu);

        inheritsTables.add(aTableHandler.getTableName());

        for (int i = 0; i < n.f3.size(); i++) {
            NodeSequence seq = (NodeSequence) n.f3.elementAt(i);
            TableName tbl = (TableName) seq.elementAt(1);
            TableNameHandler aTableHandler1 = new TableNameHandler(client);
            tbl.accept(aTableHandler1, argu);
            inheritsTables.add(aTableHandler1.getTableName());

        }

        return _ret;
    }

    /**
     * Grammar production: f0 -> <PRIMARYKEY_> f1 -> "(" f2 ->
     * ColumnNameList(prn) f3 -> ")"
     */

    @Override
    public Object visit(PrimaryKeyDef n, Object argu) {
        Object _ret = null;
        if (constraintName != null) {
            pKConstraintName = constraintName;
            constraintName = null;
        }
        ColumnNameListHandler aColumnNameListHandler = new ColumnNameListHandler();
        n.f2.accept(aColumnNameListHandler, null);
        isDefinePrimaryKey();
        primaryKeyNameColList = aColumnNameListHandler.getColumnNameList();
        return _ret;
    }

    /**
     * Grammar production:
     * f0 -> <CONSTRAINT_>
     * f1 -> Identifier(prn)
     */
    @Override
    public Object visit(Constraint n, Object argu) {
        return n.f1.accept(new IdentifierHandler(), argu);
    }

    /**
     * Grammar production:
     * f0 -> <PARTITIONINGKEY_> [ Identifier(prn) ] <ON_> PartitionChoice(prn)
     *       | <PARTITION_WITH_> <PARENT_>
     *       | <REPLICATED_>
     *       | <ON_> ( <NODE_> | <NODES_> ) <INT_LITERAL>
     *       | <ROUND_ROBIN_> <ON_> PartitionChoice(prn)
     */
    @Override
    public Object visit(PartitionDeclare n, Object argu) {
        switch (n.f0.which)

        {

        case 0:
            partScheme = SysTable.PTYPE_HASH;
            // verify if the column is ok
            NodeSequence aNodeSequence = (NodeSequence) n.f0.choice;
            NodeOptional aNodeOptional = (NodeOptional) aNodeSequence.elementAt(1);
            if (aNodeOptional.present()) {
                // We can get the name
                // NodeToken aNodeToken =(NodeToken) aNodeOptional.node;
                partColumn = (String) aNodeOptional.node.accept(new IdentifierHandler(), argu);
            }
            n.f0.accept(this, argu);
            break;
        case 1:
            // Presently the underlying engine does not support partitioning
            // with parent
            break;

        case 2:
            this.partScheme = SysTable.PTYPE_LOOKUP;
            Collection<DBNode> dbNodes = database.getDBNodeList();
            partitionNodeList = new ArrayList<Integer>(dbNodes.size());
            for (DBNode dbNode : dbNodes) {
                partitionNodeList.add(dbNode.getNodeId());
            }
            break;

        case 3:
            partScheme = SysTable.PTYPE_ONE;
            aNodeSequence = (NodeSequence) n.f0.choice;
            NodeToken aNodeToken = (NodeToken) aNodeSequence.elementAt(2);
            partitionNodeList = Collections.singletonList(Integer.parseInt(aNodeToken.tokenImage));
            break;

        case 4:
            this.partScheme = SysTable.PTYPE_ROBIN;
            n.f0.accept(this, argu);
            break;
        }

        Object _ret = null;
        return _ret;
    }// End Partition Declare

    /**
     * f0 -> <ALL_> | <NODES_> "(" NodePartitionList(prn) ")"
     *
     *
     */
    @Override
    public Object visit(PartitionChoice n, Object argu) {
        Object _ret = null;
        // Get all the nodes
        try {
            switch (n.f0.which) {
            case 0:
                partitionNodeList = new ArrayList<Integer>(
                        database.getDBNodeList().size());
                for (DBNode dbNode : database.getDBNodeList()) {
                    partitionNodeList.add(dbNode.getNodeId());
                }
                break;
            case 1:
                // In this case we will have the information gathering
                // done with the function NodePartitioningList()
                n.f0.accept(this, argu);
                break;
            }
        } catch (Exception nodeException) {
            throw new XDBServerException(
                    ErrorMessageRepository.NODEINFO_CORRUPT, nodeException,
                    ErrorMessageRepository.NODEINFO_CORRUPT_CODE);
        }

        return _ret;
    }

    /**
     * <DECIMAL_LITERAL> ( "," <DECIMAL_LITERAL>)+
     *
     */
    @Override
    public Object visit(NodePartitionList n, Object argu) {
        try {
            partitionNodeList = new ArrayList<Integer>();
            Object _ret = null;
            n.f0.accept(this, argu);
            partitionNodeList.add(Integer.parseInt(n.f0.tokenImage));
            n.f1.accept(this, argu);
            for (Object node : n.f1.nodes) {
                NodeSequence aNodeSequence = (NodeSequence) node;
                NodeToken aNodeToken = (NodeToken) aNodeSequence.elementAt(1);
                partitionNodeList.add(Integer.parseInt(aNodeToken.tokenImage));
            }
            return _ret;
        } catch (Exception nodeException) {
            throw new XDBServerException(
                    ErrorMessageRepository.NODEINFO_CORRUPT, nodeException,
                    ErrorMessageRepository.NODEINFO_CORRUPT_CODE);
        }
    }

    private boolean validPartitioningKey(String colName) {
        for (SqlCreateTableColumn colDef : columnDefinitions) {
            if (colName.equals(colDef.columnName)) {
                return colDef.canBePartitioningKey();
            }
        }
        throw new XDBServerException("Column not found: " + colName);
    }

    private String findPartitioningKey() {
        if (columnDefinitions.isEmpty() && aQueryTree != null) {
            for (SqlExpression projExpr : aQueryTree.getProjectionList()) {
                SqlCreateTableColumn aCreateTabCol = new SqlCreateTableColumn(projExpr);
                if (aCreateTabCol.canBePartitioningKey()) {
                    return aCreateTabCol.columnName;
                }
            }
        } else {
            for (SqlCreateTableColumn aCreateTabCol : columnDefinitions) {
                if (aCreateTabCol.canBePartitioningKey()) {
                    return aCreateTabCol.columnName;
                }
            }
        }
        return null;
    }

    private void defaultPartition() {
        // In case we have a primary key
        if (primaryKeyNameColList != null && primaryKeyNameColList.size() > 0) {
            for (String pkColName : primaryKeyNameColList) {
                if (validPartitioningKey(pkColName)) {
                    partColumn = pkColName;
                    break;
                }
            }
        } else {
            // In case we dont have a primary key - look for a non-blob column
            partColumn = findPartitioningKey();
        }

        partitionNodeList = new ArrayList<Integer>(
                database.getDBNodeList().size());
        for (DBNode dbNode : database.getDBNodeList()) {
            partitionNodeList.add(dbNode.getNodeId());
        }

        partScheme = partColumn == null ? SysTable.PTYPE_ROBIN : SysTable.PTYPE_HASH;
    }

    /**
     * Grammar production:
     * f0 -> <CREATE_>
     * f1 -> [ [ <LOCAL_> | <GLOBAL_> ] ( <TEMP_> | <TEMPORARY_> ) ]
     * f2 -> <TABLE_>
     * f3 -> TableName(prn)
     * f4 -> ( "(" CreateDefinition(prn) ( "," CreateDefinition(prn) )* ")" [ PartitionDeclare(prn) ] [ inheritsDef(prn) ] [ WithXRowID(prn) ] [ OnCommitClause(prn) ] [ tablespaceDef(prn) ] | [ "(" ColumnNameList(prn) ")" ] [ PartitionDeclare(prn) ] [ WithXRowID(prn) ] [ OnCommitClause(prn) ] [ tablespaceDef(prn) ] <AS_> SelectWithoutOrderAndSet(prn) )
     */
    @Override
    public Object visit(createTable n, Object argu) {

        Object _ret = null;

        TableNameHandler aTableHandler = new TableNameHandler(client);
        n.f3.accept(aTableHandler, argu);
        referenceName = aTableHandler.getReferenceName();
        temporary = n.f1.present() || aTableHandler.isTemporary();
        if (temporary) {
            tableName = client.getSysDatabase().getUniqueTempTableName(referenceName);
        } else {
            tableName = aTableHandler.getTableName();
        }

        n.f4.accept(this, argu);
        return _ret;

    }

    /**
     * Grammar production:
     * f0 -> <SELECT_>
     * f1 -> [ <ALL_> | <DISTINCT_> | <UNIQUE_> ]
     * f2 -> SelectList(prn)
     * f3 -> [ IntoClause(prn) ]
     * f4 -> [ FromClause(prn) ]
     * f5 -> [ WhereClause(prn) ]
     * f6 -> [ GroupByClause(prn) ]
     */
    @Override
    public Object visit(SelectWithoutOrderAndSet n, Object argu) {
    	if (n.f3.present()) {
    		throw new XDBServerException("Syntax error: INTO");
    	}
        aQueryTree = new QueryTree();
    	QueryTreeHandler handler = new QueryTreeHandler(commandToExecute);
    	n.accept(handler, aQueryTree);
    	aQueryTree.setIntoTable(tableName, referenceName, temporary);
    	PartitionMap partMap = getPartitionMap();
    	aQueryTree.setIntoTablePartitioning((short) partScheme, partColumn, partMap);
    	aQueryTree.setIntoTableColumns(colNameList);
    	return null;
    }

    /**
     * Grammar production:
     * f0 -> <WITH_XROWID_>
     *       | <WITHOUT_XROWID_>
     */
    @Override
    public Object visit(WithXRowID n, Object argu) {
        if (n.f0.which == 0) {
            columnDefinitions.add(SqlCreateTableColumn.ROW_ID_COLUMN);
            if (isPossibleToCreateIndex()) {
                rowidIndexName = SysIndex.ROWID_INDEXNAME + "_"
                        + getTableName();
                createRowIDIndex = "create unique index " + rowidIndexName
                        + " on " + getTableName() + " ( "
                        + SqlCreateTableColumn.XROWID_NAME + " ) ";
            }
        }
        return null;
    }

    /**
     * Grammar production:
     * f0 -> <ON_COMMIT_>
     * f1 -> ( <PRESERVE_ROWS_> | <DELETE_ROWS_> | <DROP_> )
     */
    @Override
    public Object visit(OnCommitClause n, Object argu) {
        switch (n.f1.which) {
        case 0:
            break;
        case 1:
            // fall through
        case 2:
            throw new XDBServerException("Not supported: " + n.f1.choice);
        }
        return null;
    }

    @Override
    public Object visit(ColumnNameList n, Object argu) {
        ColumnNameListHandler aColumnNameListHandler = new ColumnNameListHandler();
        n.accept(aColumnNameListHandler, argu);
        colNameList = aColumnNameListHandler.getColumnNameList();
        return null;
    }

    /**
     * Grammar production:
     * f0 -> <TABLESPACE_>
     * f1 -> Identifier(prn)
     */
    @Override
    public Object visit(tablespaceDef n, Object argu) {
        tablespaceName = (String) n.f1.accept(new IdentifierHandler(), argu);
        return null;
    }

    /**
     *
     * @return String rebuild String
     */
    public String rebuildString() {
        StringBuffer sbCreateTable = new StringBuffer(256);

        if (isTempTable()) {
            sbCreateTable.append(Props.XDB_SQLCOMMAND_CREATETEMPTABLE_START);
        } else {
            sbCreateTable.append("CREATE TABLE");
        }

        sbCreateTable.append(" ").append(IdentifierHandler.quote(getTableName())).append(" (");

        for (SqlCreateTableColumn colDef : columnDefinitions) {
            sbCreateTable.append(colDef.rebuildString());
            sbCreateTable.append(", ");
        }

        if (primaryKeyNameColList != null && primaryKeyNameColList.size() > 0) {
            if (pKConstraintName == null) {
                pKConstraintName = "PK_IDX_" + getReferenceName();
            }
            sbCreateTable.append("CONSTRAINT "
                    + IdentifierHandler.quote(pKConstraintName)
                    + " PRIMARY KEY (");
            for (String colName : primaryKeyNameColList) {
                sbCreateTable.append(IdentifierHandler.quote(colName));
                sbCreateTable.append(", ");
            }

            sbCreateTable.setLength(sbCreateTable.length() - 2);
            sbCreateTable.append("), ");
        }

        if (aForeignKeyDefsVector != null) {
            for (ForeignKeyHandler fkHandler : aForeignKeyDefsVector) {
                if (!fkHandler.isSoft(partColumn, getPartitionMap())) {
                    if (fkHandler.getConstraintName() != null) {
                        sbCreateTable.append("CONSTRAINT ").append(
                                IdentifierHandler.quote(fkHandler.getConstraintName())).append(" ");
                    }
                    sbCreateTable.append("FOREIGN KEY (");
                    for (String colName : fkHandler.getLocalColumnNames()) {
                        sbCreateTable.append(IdentifierHandler.quote(colName)).append(", ");
                    }
                    sbCreateTable.setLength(sbCreateTable.length() - 2);
                    sbCreateTable.append(") REFERENCES ").append(
                            IdentifierHandler.quote(fkHandler.getForeignTableName())).append(" (");
                    for (String colName : fkHandler.getForeignColumnNames()) {
                        sbCreateTable.append(IdentifierHandler.quote(colName)).append(", ");
                    }
                    sbCreateTable.setLength(sbCreateTable.length() - 2);
                    sbCreateTable.append("), ");
                }
            }
        }

        if (checkList != null) {
            int i = 0;
            for (String element : checkList) {
                if ("".equals(checkConstraintName.elementAt(i))) {
                    sbCreateTable.append("CHECK (").append(element).append(
                            "), ");
                } else {
                    sbCreateTable.append(
                            "CONSTRAINT "
                                    + checkConstraintName.elementAt(i)
                                    + " CHECK (").append(element).append(
                            "), ");
                }
                i++;
            }
        }

        sbCreateTable.setLength(sbCreateTable.length() - 2);
        sbCreateTable.append(")");

        if (parentTable != null) {
            sbCreateTable.append(" INHERITS (").append(
                    IdentifierHandler.quote(parentTable.getTableName())).append(")");

            // Additionally, inherit the index on xrowid, if it exists
            // This will slow down the load times, but will improve the query times
            if (parentTable.getSysColumn(SqlCreateTableColumn.XROWID_NAME) != null) {
                if (isPossibleToCreateIndex()) {
                    rowidIndexName = SysIndex.ROWID_INDEXNAME + "_" + getTableName();
                    createRowIDIndex = "create unique index " +
                            rowidIndexName + " on " + getTableName() +
                            " ( " + SqlCreateTableColumn.XROWID_NAME + " ) ";
                }
            }
        }

        if (isTempTable()) {
            sbCreateTable.append(Props.XDB_SQLCOMMAND_CREATETEMPTABLE_SUFFIX);
        }
        return sbCreateTable.toString();
    }

    private SqlCreateTableColumn getColumn(String colname) {
        for (SqlCreateTableColumn colDef : columnDefinitions) {
            if (colDef.columnName.toUpperCase().equals(colname.toUpperCase())) {
                return colDef;
            }
        }
        return null;
    }

    private boolean checkSemantics(SysDatabase database) {
        boolean allIsFine = true;
        // Make the following checks before we insert into the xsys table
        // Check 1.0
        if (tableName.length() > 255) {
            throw new XDBServerException(
                    ErrorMessageRepository.TABLE_NAME_CANNOT_EXCEED_255 + "("
                            + tableName + ")", 0,
                    ErrorMessageRepository.TABLE_NAME_CANNOT_EXCEED_255_CODE);
        }

        if (!temporary
                && tableName.startsWith(Props.XDB_TEMPTABLEPREFIX)) {
            throw new XDBServerException("Table name can not start with \""
                    + Props.XDB_TEMPTABLEPREFIX
                    + "\", this prefix is reserved for internal temp tables");
        }

        // Check if the table already exists in the system -- This is the only
        // location
        // where we use the table name directly. The reason being that we dont
        // want to
        // allow temp tables which have the same names as the real underlying
        // tables.
        if (database.isTableExists(tableName)
                || database.isTableExists(referenceName)
                || client.getTempTableName(referenceName) != null) {
            throw new XDBServerException(ErrorMessageRepository.DUP_TABLE_NAME
                    + "(" + referenceName + ")", 0,
                    ErrorMessageRepository.DUP_TABLE_NAME_CODE);
        }

        if (inheritsTables != null && !inheritsTables.isEmpty()) {
            parentTable = database.getSysTable(inheritsTables.get(0));
            if (partScheme != SysTable.PTYPE_DEFAULT
                    && parentTable.getPartitionScheme() != partScheme
                    || partColumn != null
                    && partColumn.length() > 0
                    && !partColumn.equalsIgnoreCase(parentTable.getPartitionColumn())) {
                throw new XDBServerException(
                        "Child table must have same partitioning as parent table");
            }
            if (client.getCurrentUser() != parentTable.getOwner()) {
                throw new XDBServerException(
                        "You can inherit only from own table");
            }
        }

        // Check if the column names are not duplicate
        for (int i = 0; i < columnDefinitions.size(); i++) {
            SqlCreateTableColumn aTableColumn = columnDefinitions.get(i);
            String aColumnName = aTableColumn.columnName;
            for (int j = 0; j < i; j++) {
                SqlCreateTableColumn aTableColumnCheck = columnDefinitions.get(j);
                if (aColumnName.equals(aTableColumnCheck.columnName)) {
                    throw new XDBServerException(
                            ErrorMessageRepository.DUP_COLUMN_NAME + "("
                                    + aColumnName + ")", 0,
                            ErrorMessageRepository.DUP_COLUMN_NAME_CODE);
                }
            }
            if (parentTable != null) {
                if (parentTable.getSysColumn(aColumnName) != null) {
                    throw new XDBServerException(
                            ErrorMessageRepository.DUP_COLUMN_NAME + "("
                                    + aColumnName + ")", 0,
                            ErrorMessageRepository.DUP_COLUMN_NAME_CODE);
                }
            }
        }
        //
        // Also check if we have more than one serial column
        int serialCount = parentTable != null
                && parentTable.getSerialColumn() != null ? 1 : 0;
        for (SqlCreateTableColumn colDef : columnDefinitions) {
            if (colDef.isSerial()) {
                serialCount++;
                if (serialCount > 1) {
                    throw new XDBServerException(
                            "The number of serial columns should not be greater than 1");
                }
            }
        }

        // Check whether the columns involved in the check const. are present in
        // the
        // table -- The idea is to get all the columns and check if they are the
        // part
        // of the declaration
        for (SqlCreateTableColumn colDef : columnDefinitions) {
            if (colDef.checkCondition != null) {
                for (QueryCondition aQC : QueryCondition.getNodes(
                        colDef.checkCondition, QueryCondition.QC_SQLEXPR)) {
                    for (SqlExpression columnExpr : SqlExpression.getNodes(aQC.getExpr(),
                            SqlExpression.SQLEX_COLUMN)) {
                        AttributeColumn aAttributeColumn = columnExpr.getColumn();
                        String columnName = aAttributeColumn.columnName;

                        // check if the column is present in the column
                        // definition
                        for (SqlCreateTableColumn colDef1 : columnDefinitions) {
                            if (colDef1.columnName.equals(columnName)) {
                                if (colDef1.getDefaultValue() != null
                                        && colDef1.getDefaultValue().equalsIgnoreCase(
                                                "NULL")) {
                                    throw new XDBServerException(
                                            ErrorMessageRepository.UNKOWN_COLUMN_NAME
                                                    + "(" + columnName + ")",
                                            0,
                                            ErrorMessageRepository.UNKOWN_COLUMN_NAME_CODE);
                                }
                            }
                        }

                    }

                }
            }
            // Foreign Constraints - Make sure that we dont have duplicate
            // constriant names in the
            // table definition
            HashSet<String> aCheckTable = new HashSet<String>();
            for (ForeignKeyHandler foreignDef : aForeignKeyDefsVector) {
                while (!aCheckTable.add(foreignDef.getConstraintName())) {
                    foreignDef.clearConstraintName();
                }
            }

            // Check if the referenced table exists in the database
            for (ForeignKeyHandler foreignDef : aForeignKeyDefsVector) {
                SysTable aSysTable = foreignDef.getForeignTable();

                SysIndex aSysIndex = null;
                // Now we need to check if the primary keys are the same or if
                // the columns form a unique index
                aSysIndex = aSysTable.getPrimaryOrUniqueIndex(foreignDef.getForeignColumnNames());
                if (aSysIndex == null) {
                    throw new XDBServerException(
                            ErrorMessageRepository.COLUMN_MUST_PRIMARY_KEY_OR_UNIQUE_INDEX,
                            0,
                            ErrorMessageRepository.COLUMN_MUST_PRIMARY_KEY_OR_UNIQUE_INDEX_CODE);
                }
                // Now get the columns from the SysIndex and then match the data
                // types
                List<SysColumn> vOfKeySysColumns = aSysIndex.getKeyColumns();
                Iterator<SysColumn> it = vOfKeySysColumns.iterator();
                for (String columnName : foreignDef.getLocalColumnNames()) {
                    // This will search and get u the column def. for this
                    // particular
                    // column
                    SqlCreateTableColumn columnDef = getColumn(columnName);
                    // Also check to make sure that the column is one of the
                    // declared columns
                    if (columnDef == null) {
                        throw new XDBServerException(
                                ErrorMessageRepository.COLUMN_NOT_IN_TABLE
                                        + " (" + columnName + " )", 0,
                                ErrorMessageRepository.COLUMN_NOT_IN_TABLE_CODE);
                    }
                    if (!it.hasNext()) {
                        throw new XDBServerException(
                                ErrorMessageRepository.KEY_COUNT_NOT_EQUAL, 0,
                                ErrorMessageRepository.KEY_COUNT_NOT_EQUAL_CODE);
                    }
                    SysColumn aForeignSysColumn = it.next();
                    if (columnDef.getColumnType() != aForeignSysColumn.getColType()) {
                        throw new XDBServerException(
                                ErrorMessageRepository.DATA_TYPE_MISMATCH + "("
                                        + columnName + ","
                                        + aForeignSysColumn.getColName() + ")",
                                0,
                                ErrorMessageRepository.DATA_TYPE_MISMATCH_CODE);
                    }

                }
            }
        }

        if (parentTable == null) {
            if (partScheme == SysTable.PTYPE_HASH) {
                if (partColumn == null) {
                    partColumn = findPartitioningKey();
                    if (partColumn == null) {
                        throw new XDBServerException("Can not choose column for partitioning key");
                    }
                } else if (!validPartitioningKey(partColumn)) {
                    throw new XDBServerException(
                            ErrorMessageRepository.PARTITION_COLUMN_NOT_FOUND_IN_COLUMNLIST,
                            0,
                            ErrorMessageRepository.PARTITION_COLUMN_NOT_FOUND_IN_COLUMNLIST_CODE);
                }
            }
        }

        if (tablespaceName != null) {
            tablespace = MetaData.getMetaData().getTablespace(tablespaceName);
            for (DBNode dbNode : getNodeList()) {
                if (!tablespace.getLocations().containsKey(dbNode.getNodeId())) {
                    throw new XDBServerException("Tablespace "
                            + IdentifierHandler.quote(tablespaceName)
                            + " does not exist on Node " + dbNode.getNodeId());
                }
            }
        }
        return allIsFine;
    }

    // This function is a getter for primary keys
    public List<String> getPrimaryKeyNameColList() {
        return primaryKeyNameColList;
    }

    // This function is to get the local column name list
    public List<ForeignKeyHandler> getForeignKeyDefs() {
        return aForeignKeyDefsVector;
    }

    public boolean isTempTable() {
        return temporary;
    }

    public String getReferenceName() {
        return referenceName;
    }

    public SqlCreateTableColumn getSerialColumnDef() {

        for (SqlCreateTableColumn aSqlCreateTableColumn : columnDefinitions) {
            if (aSqlCreateTableColumn.isSerial()) {
                return aSqlCreateTableColumn;
            }
        }
        return null;
    }

    private String getSerialColumnIndexStrings() {

        SqlCreateTableColumn aSqlCreateTableColumn = getSerialColumnDef();
        if (aSqlCreateTableColumn != null) {
            String createSerialIndex = "create  index "
                    + IdentifierHandler.quote(SqlCreateTableColumn.IDX_SERIAL_NAME + "_" + getTableName())
                    + " on " + IdentifierHandler.quote(getTableName())
                    + " ( "
                    + IdentifierHandler.quote(aSqlCreateTableColumn.columnName)
                    + " ) ";
            return createSerialIndex;

        }
        return null;
    }

    public boolean isPossibleToCreateIndex() {
        // It is possible to create temp tables if the user has specified that
        // his database allows for creation of underlying
        // temp table indexes or if it is not a temp table at all
        return Props.XDB_ALLOWTEMPTABLEINDEX > 0 || !isTempTable();
    }

    // Get the nodes on which we want to create this particular
    // table
    public List<DBNode> getNodeList() {
        if (nodeList == null) {
            nodeList = new ArrayList<DBNode>();
            if (qProcessor != null) {
                nodeList.addAll(qProcessor.getNodeList());
            } else if (parentTable != null) {
                nodeList.addAll(parentTable.getNodeList());
            } else {
                for (Integer nodeID : getPartitionList()) {
                    nodeList.add(database.getDBNode(nodeID));
                }
            }
            Collections.sort(nodeList, new Comparator<DBNode>() {

                public int compare(DBNode arg0, DBNode arg1) {
                    int node1 = arg0.getNodeId();
                    int node2 = arg1.getNodeId();
                    return node1 == node2 ? 0 : node1 > node2 ? 1 : -1;
                }
            });
        }
        return nodeList;
    }

    public SysTablespace getTablespace() {
        return tablespace;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IPreparable#isPrepared()
     */
    public boolean isPrepared() {
        return sqlStatement != null;
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

            if (!temporary && client.getCurrentUser().getUserClass() == SysLogin.USER_CLASS_STANDARD) {
                XDBSecurityException ex = new XDBSecurityException(
                        "You are not allowed to create tables");
                logger.throwing(ex);
                throw ex;
            }
            SysDatabase dataBase = client.getSysDatabase();

            if (aQueryTree != null) {
                for (SqlExpression aColumn : aQueryTree.getProjectionList()) {
                    columnDefinitions.add(new SqlCreateTableColumn(aColumn));
                }
            }

            checkSemantics(dataBase);

            if (aQueryTree != null) {
                aQueryTree.setIntoTableSpace(tablespace);
                qProcessor = new QueryProcessor(client, aQueryTree);
                qProcessor.prepare();
            }

            // Incase it is possible to create index, then we will create it
            // else we have to skip it
            if (isPossibleToCreateIndex()) {
                serialColumnIndexString = getSerialColumnIndexStrings();
            }
            sqlStatement = rebuildString();

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

            if (!isPrepared()) {
                prepare();
            }

            if (qProcessor != null) {
            	return qProcessor.execute(engine);
            }

            // build up string to run on the nodes
            SysDatabase database = client.getSysDatabase();

            IMetaDataUpdate metaDataUpdater;
            if (temporary) {
                metaDataUpdater = new SyncTempCreateTable(this);
            } else {
                metaDataUpdater = new SyncCreateTable(this);
            }

            List<DBNode> nodeList = getNodeList();
            MetaData meta = MetaData.getMetaData();
            meta.beginTransaction();
            try {
                metaDataUpdater.execute(client);
                MultinodeExecutor aMultinodeExecutor = client.getMultinodeExecutor(nodeList);
                if (!client.isInTransaction()) {
                    engine.beginTransaction(client, nodeList);
                }
                try {
                    if (tablespace == null) {
                        aMultinodeExecutor.executeCommand(sqlStatement,
                                nodeList);
                    } else {
                        HashMap<DBNode, String> commandMap = new HashMap<DBNode, String>();
                        for (Object element : nodeList) {
                            DBNode dbNode = (DBNode) element;
                            String command = sqlStatement + " TABLESPACE "
                                    + tablespace.getNodeTablespaceName(dbNode.getNodeId());
                            commandMap.put(dbNode, command);
                        }
                        aMultinodeExecutor.executeCommand(commandMap);
                    }

                    if (serialColumnIndexString != null) {
                        aMultinodeExecutor.executeCommand(
                                serialColumnIndexString, nodeList);
                    }

                    if (createRowIDIndex != null) {
                        // check for tablespace
                        if (tablespace == null) {
                            aMultinodeExecutor.executeCommand(createRowIDIndex,
                                    nodeList);
                        } else {
                            HashMap<DBNode, String> commandMap = new HashMap<DBNode, String>();
                            for (Object element : nodeList) {
                                DBNode dbNode = (DBNode) element;
                                String command = createRowIDIndex + " TABLESPACE "
                                        + tablespace.getNodeTablespaceName(dbNode.getNodeId());
                                commandMap.put(dbNode, command);
                            }
                            aMultinodeExecutor.executeCommand(commandMap);
                        }                        
                    }

                    engine.commitTransaction(client, nodeList);
                } catch (Exception ex) {
                    engine.rollbackTransaction(client, nodeList);
                    throw ex;
                }
                meta.commitTransaction(metaDataUpdater);
            } catch (Exception ex) {
                logger.catching(ex);
                meta.rollbackTransaction();
                throw ex;
            }

            if (temporary) {
                // If every thing went well , we should register the tables in
                // the session only if it is a temptable
                client.registerTempTableWithSession(getReferenceName(),
                        getTableName());
                SysTable aSysTable = database.getSysTable(getTableName());
                aSysTable.setTableTemporary(true);
            }
            return ExecutionResult.createSuccessResult(ExecutionResult.COMMAND_CREATE_TABLE);

        } finally {
            logger.exiting(method);
        }
    }

    public String getRowidIndexName() {
        return rowidIndexName;
    }

    /**
     * @return Returns the tableName.
     */
    public String getTableName() {
        return tableName;
    }

    public SysTable getParentTable() {
        return parentTable;
    }

    public List<String> getChecks() {
        return checkList;
    }

    private void isDefinePrimaryKey() {
        if (primaryKeyNameColList != null) {
            throw new XDBServerException("multiple primary keys for table "
                    + this.tableName + " are not allowed");
        }

    }

    /**
     * @return Returns the pKConstraintName.
     */
    public String getPKConstraintName() {
        return pKConstraintName;
    }

    /**
     * @return Returns the checkConstraintName.
     */
    public Vector<String> getCheckConstraintName() {
        return checkConstraintName;
    }

    /**
     * Get target table's partitioning scheme
     * @return
     */
    public short getPartScheme() {
        if (partScheme == SysTable.PTYPE_DEFAULT) {
            defaultPartition();
        }
        return (short) partScheme;
    }

    /**
     * Get target table's partition map
     * @return
     */
    public PartitionMap getPartitionMap() {
        if (partitionMap == null) {
            // Trigger default partitioning if not specified
            Collection<Integer> nodes = getPartitionList();
            switch (getPartScheme()) {
            case SysTable.PTYPE_HASH:
                partitionMap = new HashPartitionMap();
                break;
            case SysTable.PTYPE_LOOKUP:
            case SysTable.PTYPE_ONE:
                partitionMap = new ReplicatedPartitionMap();
                break;
            case SysTable.PTYPE_ROBIN:
                partitionMap = new RobinPartitionMap();
                break;
            }
            partitionMap.generateDistribution(nodes);
        }
        return partitionMap;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#needCoordinatorConnection()
     */
    public boolean needCoordinatorConnection() {
        return true;
    }

	@Override
	public boolean isReadOnly() {
		// TODO Auto-generated method stub
		return false;
	}
}