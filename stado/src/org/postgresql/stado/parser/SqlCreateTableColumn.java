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

import java.sql.Types;

import org.postgresql.stado.common.util.Property;
import org.postgresql.stado.optimizer.QueryCondition;
import org.postgresql.stado.optimizer.SqlExpression;
import org.postgresql.stado.parser.core.syntaxtree.CheckDef;
import org.postgresql.stado.parser.core.syntaxtree.ColumnDeclare;
import org.postgresql.stado.parser.core.syntaxtree.DefaultSpec;
import org.postgresql.stado.parser.core.syntaxtree.NodeChoice;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.DataTypeHandler;
import org.postgresql.stado.parser.handler.IdentifierHandler;
import org.postgresql.stado.parser.handler.QueryConditionHandler;
import org.postgresql.stado.parser.handler.RawImageHandler;


// -----------------------------------------------------------------
public class SqlCreateTableColumn extends DepthFirstVoidArguVisitor {
    public static String IDX_SERIAL_NAME = "IDX_SERIAL";

    public static final String XROWID_NAME = "xrowid";

    // Ingres must use decimal, max of 31 digits
    // Allow override with xdb.xrowid.type
    public static final SqlCreateTableColumn ROW_ID_COLUMN = new SqlCreateTableColumn(
            XROWID_NAME, new DataTypeHandler(Property.getInt(
                    "xdb.xrowid.SQLtype", Types.DECIMAL), Property.getInt(
                    "xdb.xrowid.length", 0), Property.getInt(
                    "xdb.xrowid.precision", 31), Property.getInt(
                    "xdb.xrowid.scale", 0)), false, null);

    private Command commandToExecute;

    // Name of the column
    public String columnName;

    private DataTypeHandler typeHandler;

    // If the type is null able
    public int isnullable = 1;

    // For "ALTER TABLE " USING exp
    // To indicate whether this column is a primary key or not.
    // Please note that this variable only gets set if the user
    // specifies that the primary key in the column specification
    // Todo- check if the user has specified any Primary key specification
    // Todo- at the global level specs.In that case we should throw a Illegal
    // Todo- argument function.
    protected boolean isPrimaryKey;

    // This variable will be used for holding reference information.
    protected SqlForeignReference aSqlForeignReference = null;

    // Query Condition for check condition
    protected QueryCondition checkCondition;

    // Default value
    String defaultValue;

    private String columnDef;

    // This class is a informaion class
    class SqlForeignReference {
        String tableName;

        String columnName;

        public String getTableName() {
            return tableName;
        }

        public String getColumnName() {
            return columnName;
        }

        public SqlForeignReference(String tableName, String columnName) {
            this.tableName = tableName;
            this.columnName = columnName;
        }
    };

    /**
     * @param columnName
     * @param typeHandler
     * @param isnullable
     */
    SqlCreateTableColumn(String columnName, DataTypeHandler typeHandler,
            boolean isnullable, String defaultValue) {
        this.columnName = columnName;
        this.typeHandler = typeHandler;
        this.isnullable = isnullable ? 1 : 0;
        this.defaultValue = defaultValue;
    }

    public SqlCreateTableColumn(SqlExpression sqlEx) {
    	this.columnName = sqlEx.getAlias();
        if (this.columnName == null || this.columnName.length() == 0) {
            if (sqlEx.isColumn()) {
                this.columnName = sqlEx.getColumn().getColumnName();
            }
        }
    	ExpressionType type = sqlEx.getExprDataType();
        this.typeHandler = new DataTypeHandler(type.type, type.length,
        		type.precision, type.scale);
    }

    // Commands to execute
    public SqlCreateTableColumn(Command commandToExecute) {
        this.commandToExecute = commandToExecute;
    }

    /**
     * The below function is responsible for building individual coulmns. each
     * column also has the original Definition and therefore should be remade
     * from the tokens.The approach taken here is to have each token here attach
     * its own string to the originalDefinition. Mind IT!! the original
     * definition is used by the Engine to run the query with out modification
     * .There fore this field needs to be worked on well--Later this might
     * change but at present we are going to do it this way
     */

    /**
     * Grammar production:
     * f0 -> Identifier(prn)
     * f1 -> types()
     * f2 -> [ <NOT_> <NULL_> | <NULL_> ]
     * f3 -> [ DefaultSpec(prn) ]
     * f4 -> [ <PRIMARYKEY_> ]
     * f5 -> [ <CHECK_> "(" SQLComplexExpression(prn) ")" ]
     */
    @Override
    public void visit(ColumnDeclare n, Object argu) {
        // get the column name
        columnName = (String) n.f0.accept(new IdentifierHandler(), argu);
        typeHandler = new DataTypeHandler();
        n.f1.accept(typeHandler, argu);
        // Call being intercepted in "NodeToken"
        if (n.f2.present()) {
            NodeChoice aNodeChoice = (NodeChoice) n.f2.node;
            switch (aNodeChoice.which) {
            case 0:
                this.isnullable = 0;
                break;
            case 1:
                this.isnullable = 1;
                break;
            }
        }
        // Call intercepted @ DefaultSpec()
        n.f3.accept(this, argu);
        if (n.f4.present()) {
            isPrimaryKey = true;
        }
        n.f5.accept(this, argu);
    }

    @Override
    public void visit(CheckDef n, Object argu) {
        QueryConditionHandler qch = new QueryConditionHandler(commandToExecute);
        n.f2.accept(qch, argu);
        checkCondition = qch.aRootCondition;
    }

    public String getTypeHandlerString() {
        return typeHandler.getTypeString();
    }

    public DataTypeHandler getTypeHandler() {
        return typeHandler;
    }

    public void setTypeHandler(DataTypeHandler typeHandler) {
        this.typeHandler = typeHandler;
    }

    public String getColumnAttrString() {
        String columnAttr = "";
        // Impying that it is true
        if (isnullable != 1) {
            columnAttr += " NOT NULL";
        }

        if (defaultValue != null) {
            columnAttr += " DEFAULT " + defaultValue;
        }

        /*
         * if (isPrimaryKey == true) { columnDef += " PRIMARY KEY "; }
         */

        if (aSqlForeignReference != null) {
            columnAttr += " " + " REFERENCES "
                    + aSqlForeignReference.getTableName() + " ( "
                    + aSqlForeignReference.getColumnName() + " ) ";
        }

        if (checkCondition != null) {
            columnAttr += " " + " CHECK " + " ( "
                    + checkCondition.rebuildString() + " ) ";
        }
        return columnAttr;
    }

    /**
     *
     * @return This function recreates the create table statement using the
     *         information contained in the information class
     */
    public String rebuildString() {
        if (columnDef == null) {
            columnDef = IdentifierHandler.quote(columnName) + " " + typeHandler.getTypeString();
            // Impying that it is true
            if (isnullable != 1) {
                columnDef += " NOT NULL";
            }

            if (defaultValue != null) {
                columnDef += " DEFAULT " + defaultValue;
            }

            if (aSqlForeignReference != null) {
                columnDef += " " + " REFERENCES "
                        + IdentifierHandler.quote(aSqlForeignReference.getTableName()) + " ( "
                        + IdentifierHandler.quote(aSqlForeignReference.getColumnName()) + " ) ";
            }

            if (checkCondition != null) {
                columnDef += " " + " CHECK " + " ( "
                        + checkCondition.rebuildString() + " ) ";
            }
        }
        return columnDef;
    }

    /**
     * Grammar production: f0 -> <DEFAULT_> f1 -> SQLSimpleExpression(prn)
     *
     * @param n
     * @param argu
     * @return
     */

    @Override
    public void visit(DefaultSpec n, Object argu) {
        RawImageHandler imageHandler = new RawImageHandler();
        n.f1.accept(imageHandler, argu);
        defaultValue = imageHandler.getImage();
    }

    // Get the default value for this , if set
    public String getDefaultValue() {

        return defaultValue;
    }

    // Get tge check condition as a string
    public String getcheckConditionString() {
        return checkCondition != null ? checkCondition.rebuildString() : null;
    }

    public boolean canBePartitioningKey() {
        return typeHandler.canBePartitioningKey();
    }

    public boolean isSerial() {
        return typeHandler.isSerial();
    }

    public int getColumnType() {
        return typeHandler.getSqlType();
    }

    public int getColumnLength() {
        return typeHandler.getLength();
    }

    public int getColumnPrecision() {
        return typeHandler.getPrecision();
    }

    public int getColumnScale() {
        return typeHandler.getScale();
    }
}
