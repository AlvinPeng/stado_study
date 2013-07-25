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

package org.postgresql.stado.engine;

import java.sql.Statement;
import java.util.Vector;

import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.optimizer.QueryTree;
import org.postgresql.stado.parser.handler.IdentifierHandler;

public class DeclaredCursor {

	private String CursorName;
	
	private QueryTree CursorTree;
	
	private boolean Materialized;
	
	private boolean Scrollable;
	
	private boolean Holdable;

    private SysTable materializedTable = null;
    
    private Vector<String> columnList = null;
    
    private Statement openStatement = null;

    public DeclaredCursor(String cursorName, QueryTree cursorTree, boolean isScrollable, boolean isHoldable) {
    	this.CursorName = cursorName;
    	this.CursorTree = cursorTree;
    	this.Scrollable = isScrollable;
    	this.Holdable = isHoldable;
    	this.Materialized = false;
    }

    public String getName() {
    	return CursorName;
    }
    
    public boolean isMaterialized() {
    	return Materialized;
    }
    
    public void setMaterialized(boolean value) {
    	Materialized = value;
    }
    
    public QueryTree getCursorTree() {
    	return CursorTree;
    }
    
    public SysTable getMaterializedTable() {
        return materializedTable;
    }

    public void setMaterializedTable(SysTable table) {
    	materializedTable = table;
    }
    
    public Vector<String> getColumnList() {
    	if (columnList == null) {
    		if (materializedTable != null) {
    			columnList = new Vector<String>();
    			for (SysColumn col : materializedTable.getColumns()) {
    				columnList.add(col.getColName());
    			}
    			
    			return columnList;
    		}
    		return null;
    	}
    	
    	return columnList;
    }
    
    public Statement getOpenStatement() {
    	return openStatement;
    }
    
    public void setOpenStatement(Statement stmt){
    	openStatement = stmt;
    }
}
