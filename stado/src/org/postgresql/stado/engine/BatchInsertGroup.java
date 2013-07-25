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
 * BatchInsertGroup.java
 * 
 *  
 */
package org.postgresql.stado.engine;

import java.util.LinkedList;
import java.util.List;

import org.postgresql.stado.parser.SqlModifyTable;


/**
 *  
 */
public class BatchInsertGroup {
    private static final int GROUP_STATUS_CREATED = 0;

    private static final int GROUP_STATUS_EXECUTED = 1;

    private static final int GROUP_STATUS_FAILED = 2;

    private List<SqlModifyTable> members;

    private SqlModifyTable master;

    private int status;

    /**
     * 
     * @param master 
     */
    public BatchInsertGroup(SqlModifyTable master) {
        this.master = master;
        members = new LinkedList<SqlModifyTable>();
        members.add(master);
        status = GROUP_STATUS_CREATED;
    }

    /**
     * 
     * @param member 
     */
    public void addMember(SqlModifyTable member) {
        members.add(member);
    }

    /**
     * 
     * @param insert 
     * @return 
     */
    public boolean isMaster(SqlModifyTable insert) {
        return insert == master;
    }

    /**
     * 
     * @return 
     */
    public SqlModifyTable getMaster() {
        return master;
    }

    /**
     * 
     * @return 
     */
    public List<SqlModifyTable> getMembers() {
        return members;
    }

    /**
     * 
     * @return 
     */
    public boolean executed() {
        return status == GROUP_STATUS_EXECUTED || status == GROUP_STATUS_FAILED;
    }

    public void setExecuted() {
        status = GROUP_STATUS_EXECUTED;
    }

    /**
     * 
     * @return 
     */
    public boolean failed() {
        return status == GROUP_STATUS_FAILED;
    }

    public void setFailed() {
        status = GROUP_STATUS_FAILED;
    }
}
