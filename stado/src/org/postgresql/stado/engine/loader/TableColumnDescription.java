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
 * TableColumnDescription.java
 *
 * 
 *
 * 
 * 
 */

package org.postgresql.stado.engine.loader;

import org.postgresql.stado.optimizer.SqlExpression;

/**
 *
 * 
 */
public class TableColumnDescription {

    private String name;

    private int length;

    private boolean serial;

    private boolean nullable;

    private SqlExpression defaultExpr;

    /**
     * Creates a new instance of TableColumnDescription
     */
    public TableColumnDescription() {
    }

    public void setName(String n) {
        this.name = n ;
    }

    public String getName() {
        return this.name ;
    }

    public void setLength(int l) {
        this.length = l ;
    }

    public int getLength() {
        return this.length ;
    }

    public void setSerial(boolean v) {
        this.serial = v ;
    }

    public boolean isSerial() {
        return this.serial ;
    }

    public void setNullable(boolean v) {
        this.nullable = v ;
    }

    public boolean isNullable() {
        return this.nullable ;
    }

    public void setDefault(SqlExpression defaultExpr) {
        this.defaultExpr = defaultExpr;
    }

    public SqlExpression getDefault() {
        return defaultExpr;
    }
}
