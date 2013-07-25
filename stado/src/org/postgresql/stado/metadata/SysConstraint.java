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
 * SysConstraint.java
 *
 *  
 */

package org.postgresql.stado.metadata;

/**
 * class SysConstraint caches data from xsysconstraints (one record)
 * 
 * 
 */

public class SysConstraint {
    private int constid; // Unique constraint id

    private String constname; // name of the constraint

    private SysTable table; // defined on this table

    private char consttype; // type 'P'rtimary, 'U'nique, 'R'eference, 'C'heck

    private int idxid; // index id defined for this constraint

    private int issoft; // is the constraint enforced by underlying db(0) or
                        // software(1)

    public SysConstraint(SysTable table, int constid, String constname,
            char consttype, int idxid, int issoft) {
        this.table = table;
        this.constid = constid;
        this.constname = constname;
        this.consttype = consttype;
        this.idxid = idxid;
        this.issoft = issoft;
    }

    /**
     * @return Returns the constid.
     */
    public int getConstID() {
        return constid;
    }

    /**
     * @return Returns the constname.
     */
    public String getConstName() {
        return constname;
    }

    /**
     * @return Returns the tableid.
     */
    public int getTableID() {
        return table.getTableId();
    }

    /**
     * @return Returns the tableid.
     */
    public SysTable getSysTable() {
        return table;
    }

    /**
     * @return Returns the consttype.
     */
    public char getConstType() {
        return consttype;
    }

    /**
     * @return Returns the idxid.
     */
    public int getIdxID() {
        return idxid;
    }

    /**
     * @return Returns the issoft.
     */
    public int getIsSoft() {
        return issoft;
    }

    @Override
    public String toString() {
        StringBuffer result = new StringBuffer();
        if (constname != null && constname.length() > 0
                && !"null".equals(constname)) {
            result.append("constraint ").append(constname).append(" ");
        }
        switch (consttype) {
        case 'P':
            result.append("primary key ");
            break;
        case 'U':
            result.append("unique key ");
            break;
        case 'R':
            result.append("foreign key ");
            break;
        case 'C':
            result.append("check ");
            break;
        }
        result.append("on ").append(table.getTableName());
        return result.toString();
    }
}
