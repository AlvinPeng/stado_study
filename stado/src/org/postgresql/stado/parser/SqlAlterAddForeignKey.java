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
 * SqlAlterAddForeignKey.java
 *
 * Class to handle MetaData updates for
 * ALTER TABLE ADD FOREIGN KEY
 *
 *
 */

package org.postgresql.stado.parser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.postgresql.stado.common.util.ParseCmdLine;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.SyncAlterTableForeignKey;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysIndex;
import org.postgresql.stado.metadata.SysPermission;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.parser.core.syntaxtree.Constraint;
import org.postgresql.stado.parser.core.syntaxtree.ForeignKeyDef;
import org.postgresql.stado.parser.core.visitor.DepthFirstRetArguVisitor;
import org.postgresql.stado.parser.handler.ForeignKeyHandler;
import org.postgresql.stado.parser.handler.IdentifierHandler;


/**
 * For adding a foreign key
 *
 *
 */

public class SqlAlterAddForeignKey extends DepthFirstRetArguVisitor implements IXDBSql,
        IPreparable {
    private static final XLogger logger = XLogger
            .getLogger(SqlAlterAddForeignKey.class);

    // Limit of some databases
    private static final int MAX_IDENTIFIER_LENGTH = 32;

    private XDBSessionContext client;

    private SqlAlterTable parent;

    private String indexName = null;

    private ForeignKeyHandler fkHandler;

    // useful members
    int indexIdToUse; // specifies the id of the index to use for

    // setting xsysreferences. If no index exists
    // bomb out.
    int referingIndexId; // we create a unique index on the refering table

    // as well. If a unique exists; we use that.
    // -1 means create a new one.
    // This is not normally done on
    // conventional db systems but will give us better
    // performance (hopefully).
    private String[] commands;

    /**
     * @param table
     * @param client
     */
    public SqlAlterAddForeignKey(SqlAlterTable parent, XDBSessionContext client) {
        this.client = client;
        this.parent = parent;
    }

    /**
     * Grammar production: f0 -> <FOREIGNKEY_> f1 -> "(" f2 ->
     * ColumnNameList(prn) f3 -> ")" f4 -> <REFERENCES_> f5 -> TableName(prn) f6 ->
     * "(" f7 -> ColumnNameList(prn) f8 -> ")"
     */
    @Override
    public Object visit(ForeignKeyDef n, Object argu) {
        if (fkHandler == null) {
            fkHandler = new ForeignKeyHandler(client);
        }
        return n.accept(fkHandler, argu);
    }

    /**
     * Grammar production: f0 -> <CONSTRAINT_> f1 ->
     * IdentifierAndUnreservedWords(prn)
     */
    @Override
    public Object visit(Constraint n, Object argu) {
        if (fkHandler == null) {
            fkHandler = new ForeignKeyHandler(client);
        }
        return n.accept(fkHandler, argu);
    }

    public SqlAlterTable getParent() {
        return parent;
    }

    // returns vector of columnNames
    public List<String> getColumnNames() {
        return fkHandler.getLocalColumnNames();
    }

    // returns the referred table name
    public String getReferedTableName() {
        return fkHandler.getForeignTableName();
    }

    // returns the vector of refered column names
    public List<String> getReferedColumnNames() {
        return fkHandler.getForeignColumnNames();
    }

    // returns the constraint name
    public String getConstraintName() {
        String constrName = fkHandler.getConstraintName();
        return constrName.length() > MAX_IDENTIFIER_LENGTH ? constrName
                .substring(0, MAX_IDENTIFIER_LENGTH) : constrName;
    }

    // returns the index id used to create the foreign key
    // -1 if no such index exists
    public int getIndexIDUsed() {
        return indexIdToUse;
    }

    // returns the index id used on the refering table
    // -1 if no such index exists
    public int getReferingIndexID() {
        return referingIndexId;
    }

    // returns an index name to be used for creating the index
    public String getIndexName() {
        if (indexName == null) {
            // to make the index name unique
            String uniqueStr = "";
            for (String columnName : fkHandler.getForeignColumnNames()) {
                uniqueStr = uniqueStr + "_" + columnName;
            }
            try {
                java.security.MessageDigest md5 = java.security.MessageDigest
                        .getInstance("MD5");
                if (md5 != null) {
                    StringBuffer buf = new StringBuffer();
                    md5.update(uniqueStr.getBytes());
                    byte[] checkSum = md5.digest();
                    for (byte element : checkSum) {
                        int low = element & 0xf;
                        if (low < 10) {
                            buf.append(low);
                        } else {
                            buf.append((char) (low - 10 + 'A'));
                        }
                    }
                    uniqueStr = buf.toString();
                }
            } catch (java.security.NoSuchAlgorithmException e) {
                // do nothing.
            }

            indexName = "FK_" + parent.getTableName() + "_"
                    + fkHandler.getForeignTableName();
            if (indexName.length() > MAX_IDENTIFIER_LENGTH - uniqueStr.length()) {
                indexName = indexName.substring(0, MAX_IDENTIFIER_LENGTH
                        - uniqueStr.length());
            }
            indexName += uniqueStr;
        }
        return indexName;
    }

    /**
     * returns true - if this constraint is software managed. returns false - if
     * the underlying db maganegs the constarint.
     */
    public boolean isSoftConstraint() {
        return fkHandler.isSoft(parent.getTable().getPartitionColumn(), parent
                .getTable().getPartitionMap());
    }

    public LockSpecification getLockSpecs() {
        LockSpecification aLSpec = new LockSpecification(Collections
                .singleton(fkHandler.getForeignTable()), Collections.EMPTY_LIST);
        return aLSpec;
    }

    public Collection getNodeList() {
        return new ArrayList<DBNode>(fkHandler.getForeignTable().getNodeList());
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
     * @see org.postgresql.stado.Engine.IPreparable#isPrepared()
     */
    public boolean isPrepared() {
        return commands != null;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IPreparable#prepare()
     */
    public void prepare() throws Exception {
        final String method = "prepare";
        LinkedList<String> comm = new LinkedList<String>();
        String sql;

        logger.entering(method, new Object[] {});
        try {

            SysTable table = parent.getTable();
            table.ensurePermission(client.getCurrentUser(),
                    SysPermission.PRIVILEGE_ALTER);

            // make sure that this table contains the column names
            for (String columnName : fkHandler.getLocalColumnNames()) {
                SysColumn sysCol = table.getSysColumn(columnName);
                if (sysCol == null) {
                    throw new XDBServerException(
                            ErrorMessageRepository.COLUMN_NOT_IN_TABLE + " ( "
                                    + columnName + " , " + table.getTableName()
                                    + " ) ", 0,
                            ErrorMessageRepository.COLUMN_NOT_IN_TABLE_CODE);
                }
            }
            // also confirm that the referred table has the columns referred to
            for (String columnName : fkHandler.getForeignColumnNames()) {
                SysColumn sysCol = fkHandler.getForeignTable().getSysColumn(
                        columnName);
                if (sysCol == null) {

                    throw new XDBServerException(
                            ErrorMessageRepository.COLUMN_NOT_IN_TABLE + " ( "
                                    + columnName + " , "
                                    + fkHandler.getForeignTableName() + " ) ",
                            0, ErrorMessageRepository.COLUMN_NOT_IN_TABLE_CODE);
                }
            }
            // Additionally check for a unique index on the
            // referenced table.
            SysIndex indexToUse = fkHandler.getForeignTable()
                    .getPrimaryOrUniqueIndex(fkHandler.getForeignColumnNames());
            if (indexToUse == null || (indexIdToUse = indexToUse.idxid) == -1) {
                throw new XDBServerException(
                        ErrorMessageRepository.NO_PRIMARY_UNQIUE_INDEX + "( "
                                + fkHandler.getForeignTableName() + " ) ", 0,
                        ErrorMessageRepository.NO_PRIMARY_UNQIUE_INDEX_CODE);
            }
            // Additionally check for a unique index on the
            // refering table.
            // if there is none we Must create one.
            SysIndex referingIndex = null;
            for (SysIndex anIndex : table.getSysIndexes(fkHandler
                    .getLocalColumnNames())) {
                if (referingIndex == null
                        || anIndex.getIndexLength() < referingIndex
                                .getIndexLength()) {
                    referingIndex = anIndex;
                }
            }
            referingIndexId = referingIndex == null ? -1 : referingIndex.idxid;

            // find out how many commands need
            // to be run.

            // index to command being built
            if (referingIndexId < 0) {
                // build sql commands for creating the index
                String sqlStatement = "CREATE  INDEX " + IdentifierHandler.quote(getIndexName())
                        + " ON " + parent.getTableName() + " ( ";
                String colNames = null;
                for (String columnName : fkHandler.getLocalColumnNames()) {
                    colNames = colNames == null ? IdentifierHandler.quote(columnName)
                            : colNames + ", " + IdentifierHandler.quote(columnName);
                }
                sqlStatement = sqlStatement + colNames + ")";
                comm.add(sqlStatement);
            }

            if (!isSoftConstraint()) {
                StringBuffer theColList = new StringBuffer();
                for (String columnName : fkHandler.getLocalColumnNames()) {
                    theColList.append(IdentifierHandler.quote(columnName)).append(", ");
                }
                theColList.setLength(theColList.length() - 2);

                StringBuffer theColMapList = new StringBuffer();
                for (String columnName : fkHandler.getForeignColumnNames()) {
                    theColMapList.append(IdentifierHandler.quote(columnName)).append(", ");
                }
                theColMapList.setLength(theColMapList.length() - 2);
                HashMap<String, String> arguments = new HashMap<String, String>();
                arguments.put("table", IdentifierHandler.quote(table.getTableName()));
                arguments.put("col_map_list", theColMapList.toString());
                arguments.put("col_list", theColList.toString());
                arguments.put("constr_name", IdentifierHandler.quote(getConstraintName()));
                arguments.put("reftable", IdentifierHandler.quote(fkHandler.getForeignTableName()));

                sql = ParseCmdLine.substitute(
                        Props.XDB_SQLCOMMAND_ALTERTABLE_ADDFOREIGNKEY,
                        arguments);

                if (Props.XDB_SQLCOMMAND_ALTERTABLE_ADDFOREIGNKEY_TO_PARENT) {
                    parent.addCommonCommand(sql);
                } else {
                    comm.add(sql);
                }
            }
            if (isSoftConstraint()) {
                // TODO we have soft reference, integrity check is needed
            }
            commands = comm.toArray(new String[comm.size()]);

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

            // TODO Run integrity checker
            engine.executeDDLOnMultipleNodes(commands, parent.getNodeList(),
                    new SyncAlterTableForeignKey(this), client);
            return null;

        } finally {
            logger.exiting(method);
        }
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
