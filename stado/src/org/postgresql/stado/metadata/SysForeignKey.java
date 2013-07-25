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
 * SysForeignKey.java
 *
 *  
 */

package org.postgresql.stado.metadata;

import java.util.Enumeration;

/**
 * class SysForeignKeys caches data from xsysforeignkeys (one record)
 * 
 * 
 */

public class SysForeignKey {
    public int fkeyid;

    public int refid;

    public int fkeyseq;

    public int colid;

    public int refcolid;

    // Sends back the refering . syscolummn
    public SysColumn getReferringSysColumn(SysDatabase database) {
        for (Enumeration e = database.getAllTables(); e.hasMoreElements();) {
            SysTable table = (SysTable) e.nextElement();
            SysColumn col = table.getSysColumn(colid);
            if (col != null) {
                return col;
            }
        }
        return null;
    }

    // Sends back the referenced syscolumn
    public SysColumn getReferencedSysColumn(SysDatabase database) {
        for (Enumeration e = database.getAllTables(); e.hasMoreElements();) {
            SysTable table = (SysTable) e.nextElement();
            SysColumn col = table.getSysColumn(refcolid);
            if (col != null) {
                return col;
            }
        }
        return null;
    }

    // Get the column ID
    public int getColid() {
        return colid;
    }

    public int getRefcolid() {
        return refcolid;
    }

}