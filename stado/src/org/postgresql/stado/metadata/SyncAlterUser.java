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
 * SyncAlterUser.java
 * 
 *  
 */
package org.postgresql.stado.metadata;

import java.sql.PreparedStatement;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.parser.SqlAlterUser;


/**
 *  
 */
public class SyncAlterUser implements IMetaDataUpdate {
    private static final XLogger logger = XLogger
            .getLogger(SyncAlterUser.class);

    private SqlAlterUser parent;

    private String password;

    private String userClass;

    /**
     * 
     */
    public SyncAlterUser(SqlAlterUser parent) {
        this.parent = parent;
        password = parent.getPassword();
        userClass = parent.getUserClass();
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

            StringBuffer command = new StringBuffer("update xsysusers set ");
            if (password != null) {
                command.append("userpwd = ?");
                if (userClass != null) {
                    command.append(", usertype = ?");
                }
            } else {
                command.append("usertype = ?");
            }
            command.append(" where userid = ").append(
                    parent.getUser().getUserID());
            PreparedStatement ps = MetaData.getMetaData().prepareStatement(
                    command.toString());
            int current = 1;
            if (password != null) {
                ps.setString(current++, SysLogin.encryptPassword(password));
            }
            if (userClass != null) {
                ps.setString(current++, userClass);
            }
            if (ps.executeUpdate() != 1) {
                throw new XDBServerException(
                        "Failed to update row in \"xsysusers\"");
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

            SysUser user = parent.getUser();
            if (password != null) {
                user.getLogin().setPassword(password, true);
            }
            if (userClass != null) {
                user.getLogin().setUserClass(userClass);
            }
        } finally {
            logger.exiting(method);
        }
    }

}
