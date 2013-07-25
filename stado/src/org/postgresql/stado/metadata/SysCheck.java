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
package org.postgresql.stado.metadata;

/**
 * 
 * 
 */
public class SysCheck {
    private SysTable table;

    private int checkID;

    private SysConstraint constraint;

    private int seqNo;

    private String checkDef;

    /**
     * 
     */
    public SysCheck(SysTable table, int checkID, int constID, int seqNo,
            String checkDef) {
        this.table = table;
        this.checkID = checkID;
        this.constraint = table.getConstraint(constID);
        this.seqNo = seqNo;
        this.checkDef = checkDef;
    }

    public SysTable getSysTable() {
        return table;
    }

    /**
     * @return Returns the checkDef.
     */
    public String getCheckDef() {
        return checkDef;
    }

    /**
     * @return Returns the checkID.
     */
    public int getCheckID() {
        return checkID;
    }

    /**
     * @return Returns the constraint.
     */
    public SysConstraint getConstraint() {
        return constraint;
    }

    /**
     * @return Returns the seqNo.
     */
    public int getSeqNo() {
        return seqNo;
    }

}
