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
 * SyncCreateUser.java
 * 
 *  
 */
package org.postgresql.stado.metadata;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.parser.SqlCreateUser;


/**
 *  
 */
public class SyncCreateUser implements IMetaDataUpdate {
    private static final XLogger logger = XLogger
            .getLogger(SyncCreateUser.class);

    private SqlCreateUser parent;

    private int userid;

    /**
     * 
     */
    public SyncCreateUser(SqlCreateUser parent) {
        this.parent = parent;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.MetaData.IMetaDataUpdate#execute(org.postgresql.stado.server.XDBSessionContext)
     */
    public void execute(XDBSessionContext client) throws Exception {
        execute(client.getSysDatabase());
    }

    public void execute(SysDatabase database) throws Exception {
        final String method = "execute";
        logger.entering(method, new Object[] {});
        try {

            MetaData meta = MetaData.getMetaData();
            ResultSet rs = meta
                    .executeQuery("SELECT max(userid) FROM xsysusers");
            try {
                rs.next();
                userid = rs.getInt(1) + 1;
            } finally {
                rs.close();
            }
            // Let's use prepared statement to make password literal database
            // independent
            String commandStr = "INSERT INTO xsysusers"
                    + " (userid, username, userpwd, usertype)"
                    + " VALUES (?, ?, ?, ?)";
            PreparedStatement ps = meta.prepareStatement(commandStr);
            ps.setInt(1, userid);
            ps.setString(2, parent.getUserName());
            ps.setString(3, SysLogin.encryptPassword(parent.getPassword()));
            ps.setString(4, parent.getUserClassStr());
            if (ps.executeUpdate() != 1) {
                XDBServerException ex = new XDBServerException(
                        "Failed to insert row into \"xsysusers\"");
                logger.throwing(ex);
                throw ex;
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
        logger.entering(method, new Object[] {});
        try {

            SysLogin login = new SysLogin(userid, parent.getUserName(),
                    SysLogin.encryptPassword(parent.getPassword()), parent
                            .getUserClassStr());
            MetaData.getMetaData().insertLogin(login);

        } finally {
            logger.exiting(method);
        }
    }

}
