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
/**
 *
 */
package org.postgresql.stado.parser.handler;

import java.util.List;

import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.partitions.PartitionMap;
import org.postgresql.stado.parser.core.syntaxtree.Constraint;
import org.postgresql.stado.parser.core.syntaxtree.ForeignKeyDef;
import org.postgresql.stado.parser.core.visitor.DepthFirstRetArguVisitor;

/**
 * This class provides the parser-level handling for a foriegn key column.
 *
 */
public class ForeignKeyHandler extends DepthFirstRetArguVisitor {
    private XDBSessionContext client;

    private String foreignKeyConstraintName;

    private String foreignTableName;

    private SysTable foreignTable;

    /**
     * The list of columns in the foreign Table which are invloved
     */
    private List<String> foreignconstraintColumnList;

    /**
     * The list of columns local to this table which are involved in the
     * foreignKey relationship
     */
    private List<String> localconstraintColumnList;

    /**
     *
     * @param client
     */
    public ForeignKeyHandler(XDBSessionContext client) {
        this.client = client;
    }

    /**
     * Grammar production:
     * f0 -> <CONSTRAINT_>
     * f1 -> Identifier(prn)
     */
    @Override
    public Object visit(Constraint n, Object argu) {
        foreignKeyConstraintName = (String) n.f1.accept(new IdentifierHandler(), argu);
        return null;
    }

    /**
     * Grammar production:
     * f0 -> <FOREIGNKEY_>
     * f1 -> "("
     * f2 -> ColumnNameList(prn)
     * f3 -> ")"
     * f4 -> <REFERENCES_>
     * f5 -> TableName(prn)
     * f6 -> "("
     * f7 -> ColumnNameList(prn)
     * f8 -> ")"
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(ForeignKeyDef n, Object argu) {
        // Get the Name List
        ColumnNameListHandler aNameListHandler = new ColumnNameListHandler();
        n.f2.accept(aNameListHandler, null);
        localconstraintColumnList = aNameListHandler.getColumnNameList();
        // refered tableName
        TableNameHandler aTableNameHandler = new TableNameHandler(client);
        n.f5.accept(aTableNameHandler, null);
        foreignTableName = aTableNameHandler.getTableName();
        // refered column name List
        aNameListHandler = new ColumnNameListHandler();
        n.f7.accept(aNameListHandler, null);
        foreignconstraintColumnList = aNameListHandler.getColumnNameList();
        
        return null;
    }

    private int unique = 0;

    /**
     * Constraint name already exists Clear to have it regenerated
     */
    public void clearConstraintName() {
        foreignKeyConstraintName = null;
        unique++;
    }

    /**
     * It returns foreign key constraint name. If constraint name is null, it auto-generates name
     * using table name and a unique counter.
     * @return A String value representing foreign key name.
     */
    public String getConstraintName() {
        if (foreignKeyConstraintName == null) {
            foreignKeyConstraintName = "FK_" + foreignTableName + "_" + unique;
        }
        return foreignKeyConstraintName;
    }

    /**
     * It returns list of table columns that make up the foreign key constraint.
     * @return A List corresponding to foreign key columns.
     */
    public List<String> getLocalColumnNames() {
        return localconstraintColumnList;
    }

    /**
     * It returns parent table name for the foreign key constraint.
     * @return A String value corresponding to table name.
     */
    public String getForeignTableName() {
        return foreignTableName;
    }

    /**
     *
     * @return
     */
    public SysTable getForeignTable() {
        if (foreignTable == null) {
            foreignTable = client.getSysDatabase()
                    .getSysTable(foreignTableName);
        }
        return foreignTable;
    }

    /**
     *
     * @return
     */
    public List<String> getForeignColumnNames() {
        return foreignconstraintColumnList;
    }

    /**
     *
     * @param partitionColumn
     * @param partitionMap
     * @return
     */
    public boolean isSoft(String partitionColumn, PartitionMap partitionMap) {
        if (getForeignTable().isLookup()) {
            return false;
        }
        return !partitionMap.equals(getForeignTable().getPartitionMap())
                || !(partitionColumn == null
                        && getForeignTable().getPartitionColumn() == null || localconstraintColumnList
                        .get(0).equalsIgnoreCase(partitionColumn)
                        && getForeignTable().isPartitionedColumn(
                                foreignconstraintColumnList.get(0)));
    }
}
