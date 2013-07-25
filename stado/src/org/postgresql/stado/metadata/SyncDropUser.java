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
 * SyncDropUser.java
 * 
 *  
 */
package org.postgresql.stado.metadata;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.parser.SqlDropUser;

/**
 *  
 */
public class SyncDropUser implements IMetaDataUpdate {
    private static final XLogger logger = XLogger.getLogger(SyncDropUser.class);

    private SqlDropUser parent;

    /**
     * 
     */
    public SyncDropUser(SqlDropUser parent) {
        this.parent = parent;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.MetaData.IMetaDataUpdate#execute(org.postgresql.stado.server.XDBSessionContext)
     */
    public void execute(XDBSessionContext client) throws Exception {
        final String method = "execute";
        logger.entering(method, new Object[] { client });
        try {

            String commandStr = "delete from xsysusers where userid = "
                    + parent.getLogin().getLoginID();
            if (MetaData.getMetaData().executeUpdate(commandStr) != 1) {
                throw new XDBServerException(
                        "Failed to delete row from \"xsysusers\"");
            }

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
        final String method = "refresh";
        logger.entering(method);
        try {

            SysLogin login = parent.getLogin();
            MetaData.getMetaData().removeLogin(login);
            login.invalidate();

        } finally {
            logger.exiting(method);
        }
    }

}
