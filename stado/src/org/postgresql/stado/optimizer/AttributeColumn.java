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
package org.postgresql.stado.optimizer;

import org.postgresql.stado.exception.ColumnNotFoundException;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.parser.*;
import org.postgresql.stado.parser.handler.IdentifierHandler;


//-----------------------------------------------------
public class AttributeColumn implements IRebuildString { 

    private String tableName = "";

    public String columnName = "";

    private String tableAlias = "";

    public String columnAlias = "";

    // This may be set to handle duplicate names
    public String tempColumnAlias = "";
    
    public ExpressionType columnType = new ExpressionType();

    /*
     * After analysis -- This field should be filled with the queryNode to which
     * the column belongs
     */
    public RelationNode relationNode;

    public int columnGenre = 1;

    // A permanent column is a column which we actually have in a table
    public final static int PERMANENT = 2;

    // Mapped columns are thoes which the user creates by means of alias
    // for eg. select * from nation , (select * from region) r;

    // The columns from nation are Permanent where as thoes from region are
    // mapped
    public final static int MAPPED = 4;

    // Thoes column which are borrowed from another tree ( only parent tree) are
    // set as orphan columns
    public final static int ORPHAN = 8;

    /** the tree to which this column belongs */
    QueryTree parentQueryTree;

    /**
     * 
     * @return 
     */
    public QueryCondition getParentQueryCondition() {
        return ParentQueryCondition;
    }

    /**
     * 
     * @param parentQueryCondition 
     */
    public void setParentQueryCondition(QueryCondition parentQueryCondition) {
        ParentQueryCondition = parentQueryCondition;
    }

    // Tells the column which query condition it belongs to
    QueryCondition ParentQueryCondition = null;

    /**
     * Constructor
     */
    public AttributeColumn() {

    }

    /**
     * 
     * @param database 
     * @throws java.lang.IllegalArgumentException 
     * @throws org.postgresql.stado.exception.ColumnNotFoundException 
     * @return 
     */
    public ExpressionType getColumnType(SysDatabase database)
            throws IllegalArgumentException, ColumnNotFoundException {
        // This required for 
        // column type which are generated temp, and the underlying
        // Metadata has no information about it.
        if (this.columnType != null && this.columnType.type != 0) {
            return columnType;
        }

        if (tableName == null || columnName == null) {
            throw new XDBServerException(
                    ErrorMessageRepository.TABLE_COLUMN_NOT_FILLED, 0,
                    ErrorMessageRepository.TABLE_COLUMN_NOT_FILLED_CODE);
        } else {
            if (database != null) {
                setColInfo(database);
                return columnType;
            } else {
                throw new IllegalArgumentException("Database is NULL");
            }
        }
    }

    /**
     *
     */
    public void setColumnGenere() {
        if (relationNode != null
                && relationNode.getNodeType() == RelationNode.SUBQUERY_RELATION) {
            columnGenre |= MAPPED;
        } else if (relationNode == null) {
            columnGenre |= ORPHAN;
        }
    }

    /**
     * 
     * @param database 
     * @throws org.postgresql.stado.exception.ColumnNotFoundException 
     */
    private void setColInfo(SysDatabase database)
            throws ColumnNotFoundException {

        setColumnGenere();

        // If it is not mapped and it is not orphan ==> It is permanent
        if (((columnGenre & MAPPED) == 0) && ((columnGenre & ORPHAN) == 0)) {
            SysColumn aSysColumn = database.getSysTable(tableName)
                    .getSysColumn(columnName);
            if (aSysColumn == null) {
                throw new ColumnNotFoundException(columnName, tableName);
            }
            columnType.type = aSysColumn.getColType();
            columnType.length = aSysColumn.getColLength();
            columnType.precision = aSysColumn.getColPrecision();
            columnType.scale = aSysColumn.getColScale();
        }
        // If it is Mapped -- Incase it is mapped and it is orphan too, then we
        // are treating is as
        // MAPPED, since we would have already known its type
        else if ((columnGenre & MAPPED) == MAPPED) {
            SqlExpression aSqlExpression = relationNode
                    .getMatchingSqlExpression(this);
            columnType = aSqlExpression.getExprDataType();
        } else if ((columnGenre & ORPHAN) == ORPHAN) // if it is ORPHAN
        {
            // cant do much about it
            if (relationNode != null) {
                tableName = relationNode.getTableName();
                SysColumn aSysColumn = database.getSysTable(tableName)
                        .getSysColumn(columnName);
                columnType.type = aSysColumn.getColType();
                columnType.length = aSysColumn.getColLength();
                columnType.precision = aSysColumn.getColPrecision();
                columnType.scale = aSysColumn.getColScale();
            }
            return;
        } else {
            String errorMessage = ErrorMessageRepository.ILLEGAL_COLUMN_TYPE;
            throw new XDBServerException(errorMessage + "(" + columnName + ")",
                    0, ErrorMessageRepository.ILLEGAL_COLUMN_TYPE_CODE);
        }
    }

    /**
     * Determines if this AttributeColumn is equivalent with the one passed in.
     * Not via the same reference, but if table and column properties are the
     * same
     * @param anAttributeColumn 
     * @return 
     */
    public boolean isEquivalent(AttributeColumn anAttributeColumn) {
        if (anAttributeColumn.tableAlias.length() > 0) {
            if (this.tableAlias.length() > 0) {
                if (!anAttributeColumn.tableAlias
                        .equalsIgnoreCase(this.tableAlias)) {
                    return false;
                }
            } else {
                return false;
            }
        } else {
            if (this.tableName.length() > 0) {
                if (!anAttributeColumn.tableName
                        .equalsIgnoreCase(this.tableName)) {
                    return false;
                }
            } else {
                return false;
            }
        }

        // At this point, we know that the table alias/name is ok.
        // Now compare columns
        if (this.columnName.length() > 0) {
            if (!anAttributeColumn.columnName.equalsIgnoreCase(this.columnName)) {
                return false;
            }
        } else {
            return false;
        }

        // they look equivalent
        return true;
    }

    /**
     * 
     * @return 
     */
    public String rebuildString() {
        if (columnAlias.length() > 0) {
            return IdentifierHandler.quote(tableAlias) 
                    + "." + IdentifierHandler.quote(columnAlias);
        } else {
            return IdentifierHandler.quote(tableAlias) 
                    + "." + IdentifierHandler.quote(columnName);
        }
    }

    /**
     * 
     * @param aQueryTree 
     */
    public void setMyParentTree(QueryTree aQueryTree) {
        if (parentQueryTree == null) {
            parentQueryTree = aQueryTree;
        }
    }

    /**
     * Get the SysTable for this
     *
     * @param database the database to look up for this column
     *
     * @return the SysTable that this column belongs to
     */
    public SysTable getSysTable () 
                    throws IllegalArgumentException, XDBServerException {
        
        if (relationNode == null 
                || relationNode.getClient() == null) {
            throw new XDBServerException("Internal Error: relation node client not set.");
        }
        return relationNode.getClient().getSysDatabase().getSysTable(tableName);
    }

    /**
     * Get the SysColumn for this column
     *
     * @param database the database to look up for this column
     *
     * @return the SysColumn that corresponds to this column
     */
    public SysColumn getSysColumn (SysDatabase database) 
                    throws IllegalArgumentException, XDBServerException {
        
        if (database == null) {
            throw new IllegalArgumentException("Database is NULL");
        } else if (tableName == null || this.columnName == null) {
            throw new XDBServerException(
                    ErrorMessageRepository.TABLE_COLUMN_NOT_FILLED, 0,
                    ErrorMessageRepository.TABLE_COLUMN_NOT_FILLED_CODE);            
        } else {
            return database.getSysTable(tableName).getSysColumn(columnName);
        }
    }    
    
    /**
     * 
     * @return 
     */
    public QueryTree getMyParentTree() {
        return parentQueryTree;
    }

    /**
     * 
     * @return 
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * 
     * @param tableName 
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * 
     * @param tableAlias 
     */
    public void setTableAlias(String tableAlias) {
        this.tableAlias = tableAlias;
    }

    /**
     * 
     * @return 
     */
    public String getTableAlias() {
        return tableAlias;
    }

    /**
     * 
     * @return 
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * 
     * @param columnName 
     */
    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }
    
    /**
     * 
     * @return whether or not this is a partitioned column for the relation 
     */
    public boolean isPartitionColumn() {
        if (relationNode == null) {
            throw new XDBServerException("Internal error: relation node not set for column");
        }
        // Also handle WITHs
        if (relationNode.isWithDerived()) {
            if (columnName != null && columnName.equalsIgnoreCase(relationNode.getPartitionColumnName())) {
                return true;
            }            
        }
        if (!relationNode.isTable()) {
            return false;
        }
        SysTable sysTable = getSysTable();
        if (sysTable == null) {
            return false;
        }
        return sysTable.isPartitionedColumn(columnName);
    }

}
