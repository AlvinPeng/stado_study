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

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.parser.SqlAlterTableSpace;

/**
 * 
 * 
 */
public class SyncAlterTablespace implements IMetaDataUpdate {
    private static final XLogger logger = XLogger
            .getLogger(SyncDropTablespace.class);

    private SqlAlterTableSpace parent;

    /**
     * 
     */
    public SyncAlterTablespace(SqlAlterTableSpace parent) {
        this.parent = parent;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.MetaData.IMetaDataUpdate#execute(org.postgresql.stado.server.XDBSessionContext)
     */
    public void execute(XDBSessionContext client) throws Exception {
        final String method = "execute";
        logger.entering(method, new Object[] {});
        try {

            MetaData meta = MetaData.getMetaData();
            int tablespaceID = parent.getTablespace().getTablespaceID();
            String command = "UPDATE xsystablespaces SET tablespacename = '"
                    + parent.getNewName() + "' WHERE tablespaceid = "
                    + tablespaceID;
            meta.executeUpdate(command);

        } finally {
            logger.exiting(method);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.MetaData.IMetaDataUpdate#refresh()
     */
    public void refresh() throws Exception {
        MetaData meta = MetaData.getMetaData();
        SysTablespace tablespace = parent.getTablespace();
        meta.removeTablespace(tablespace);
        tablespace.setName(parent.getNewName());
        meta.addTablespace(tablespace);
    }

}
