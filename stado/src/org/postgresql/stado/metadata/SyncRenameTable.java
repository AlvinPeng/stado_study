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
 * SyncRenameTable.java
 * 
 *  
 */
package org.postgresql.stado.metadata;

import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.parser.SqlRenameTable;

/**
 *  
 */
public class SyncRenameTable implements IMetaDataUpdate {
    private SysDatabase database;

    private SqlRenameTable renameTable;

    /**
     * 
     */
    public SyncRenameTable(SqlRenameTable aRenameTable) {
        renameTable = aRenameTable;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.MetaData.IMetaDataUpdate#execute(org.postgresql.stado.MetaData.SysDatabase)
     */
    public void execute(XDBSessionContext client) throws Exception {
        database = MetaData.getMetaData().getSysDatabase(client.getDBName());
        SysTable tableToRename = database.getSysTable(renameTable
                .getOldTableName());
        int tableid = tableToRename.getSysTableid();
        String sql = "update xsystables set tablename = '"
                + renameTable.getNewTableName() + "' where tableid = "
                + tableid;
        MetaData.getMetaData().executeUpdate(sql);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.MetaData.IMetaDataUpdate#refresh()
     */
    public void refresh() throws Exception {
        database.renameSysTable(renameTable.getOldTableName(), renameTable
                .getNewTableName());
    }

}
