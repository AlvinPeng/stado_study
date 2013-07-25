/*****************************************************************************
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



/**
 *
 */
public class FromRelation {
    private String tableName;
    private String alias;
    private boolean isOuter;
    private int outerLevel;
    private boolean isOnly;
    
    /**
     * Creates a new instance of FromRelation
     *
     * @param tableName table to track
     * @param alias the alias for the table
     * @param isOuter whether or not this table is outered
     * @param outerLevel the nested outer level
     */
    public FromRelation(String tableName, String alias, boolean isOuter, 
            int outerLevel, boolean isOnly) {
        this.tableName = tableName;
        this.alias = alias;
        this.isOuter = isOuter;
        this.outerLevel = outerLevel;
    }
    
    /**
     * Creates a new instance of FromRelation
     *
     */
    public FromRelation() {
        
    }
    
    /**
     * 
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * 
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    
    /**
     * 
     */
    public String getAlias() {
        return alias;
    }
    
    /**
     * 
     */
    public void setAlias(String alias) {
        this.alias = alias;
    }
    
    /**
     * 
     */
    public boolean getIsOuter() {
        return isOuter;
    }

    /**
     * 
     */
    public void setIsOuter(boolean isOuter) {
        this.isOuter = isOuter;
    }
    
    /**
     * 
     */
    public int getOuterLevel() {
        return outerLevel;
    }
    
    /**
     * 
     */
    public void setOuterLevel(int outerLevel) {
        this.outerLevel = outerLevel;
    }
    
    /**
     * 
     */
    public boolean getIsOnly() {
        return isOnly;
    }

    /**
     * 
     */
    public void setIsOnly(boolean isOnly) {
        this.isOnly = isOnly;
    }
}
