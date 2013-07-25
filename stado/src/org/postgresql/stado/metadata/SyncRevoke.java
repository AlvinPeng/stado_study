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
 * SyncRevoke.java
 * 
 * 
 */
package org.postgresql.stado.metadata;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.parser.SqlGrant;


/**
 * 
 */
public class SyncRevoke implements IMetaDataUpdate {
    private static final XLogger logger = XLogger.getLogger(SyncGrant.class);

    private int tabprivid = -1;

    private SqlGrant parent;

    private String updateCommand;

    /**
     * 
     */
    public SyncRevoke(SqlGrant parent) {
        this.parent = parent;
    }

    private void createDefaultPermissions(int tableID, int userID, boolean restrict) throws SQLException {
        if (tabprivid == -1) {
            ResultSet rs = MetaData.getMetaData().executeQuery(
            "SELECT max(privid) FROM xsystabprivs");
            try {
                rs.next();
                tabprivid = rs.getInt(1) + 1;
            } finally {
                rs.close();
            }
        }
        StringBuffer sbCreate = new StringBuffer("insert into xsystabprivs "
                + "(privid, userid, tableid, selectpriv, insertpriv, updatepriv, "
                + "deletepriv, referencespriv, indexpriv, alterpriv) values (");
        sbCreate.append(tabprivid++).append(", ");
        sbCreate.append(userID).append(", ");
        sbCreate.append(tableID).append(", ");
        sbCreate.append(restrict && parent.hasSelect() ? "'N', " : "'Y', ");
        sbCreate.append(restrict && parent.hasInsert() ? "'N', " : "'Y', ");
        sbCreate.append(restrict && parent.hasUpdate() ? "'N', " : "'Y', ");
        sbCreate.append(restrict && parent.hasDelete() ? "'N', " : "'Y', ");
        sbCreate.append(restrict && parent.hasReferences() ? "'N', " : "'Y', ");
        sbCreate.append(restrict && parent.hasIndex() ? "'N', " : "'Y', ");
        sbCreate.append(restrict && parent.hasAlter() ? "'N')" : "'Y')");
        if (MetaData.getMetaData().executeUpdate(sbCreate.toString()) != 1) {
            XDBServerException ex = new XDBServerException(
            "Failed to insert row into \"xsystabprivs\"");
            logger.throwing(ex);
            throw ex;
        }
    }

    private void updateFor(SysPermission permission, boolean keepRecord) {
        // Decide whether we should remove privilege or just update it
        boolean delete = !keepRecord && (parent.hasSelect() || permission.getSelect() != SysPermission.ACCESS_GRANTED)
        && (parent.hasInsert() || permission.getInsert() != SysPermission.ACCESS_GRANTED)
        && (parent.hasUpdate() || permission.getUpdate() != SysPermission.ACCESS_GRANTED)
        && (parent.hasDelete() || permission.getDelete() != SysPermission.ACCESS_GRANTED)
        && (parent.hasReferences() || permission.getReference() != SysPermission.ACCESS_GRANTED)
        && (parent.hasIndex() || permission.getIndex() != SysPermission.ACCESS_GRANTED)
        && (parent.hasAlter() || permission.getAlter() != SysPermission.ACCESS_GRANTED);
        if (delete) {
            MetaData.getMetaData().executeUpdate(
                    "delete from xsystabprivs where privid = "
                    + permission.getPermissionId());
        } else {
            if (updateCommand == null) {
                StringBuffer sbUpdate = new StringBuffer(
                "update xsystabprivs set ");
                if (parent.hasSelect()) {
                    sbUpdate.append("selectpriv = 'N', ");
                }
                if (parent.hasInsert()) {
                    sbUpdate.append("insertpriv = 'N', ");
                }
                if (parent.hasUpdate()) {
                    sbUpdate.append("updatepriv = 'N', ");
                }
                if (parent.hasDelete()) {
                    sbUpdate.append("deletepriv = 'N', ");
                }
                if (parent.hasReferences()) {
                    sbUpdate.append("referencespriv = 'N', ");
                }
                if (parent.hasIndex()) {
                    sbUpdate.append("indexpriv = 'N', ");
                }
                if (parent.hasAlter()) {
                    sbUpdate.append("alterpriv = 'N', ");
                }
                // Strip away last comma
                sbUpdate.setLength(sbUpdate.length() - 2);
                sbUpdate.append(" where privid = ");
                updateCommand = sbUpdate.toString();
            }
            MetaData.getMetaData().executeUpdate(
                    updateCommand + permission.getPermissionId());
        }
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

            for (SysTable table : parent.getTableList()) {
                if (table.getSysPermissions().isEmpty()) {
                    // Owner's permission are restricted by the command,
                    // take this into account
                    boolean restrict = parent.getGranteeList().remove(table.getOwner());
                    // Create default permissions
                    createDefaultPermissions(table.getTableId(),
                            table.getOwner().getUserID(), restrict);
                }
                for (SysUser user : parent.getGranteeList()) {
                    SysPermission permission = table.getSysPermission(user);
                    if (permission != null) {
                        updateFor(permission, user == table.getOwner());
                    }
                }
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

            for (SysTable table  : parent.getTableList()) {
                for (SysUser user : parent.getGranteeList()) {
                    if (user != null) {
                        user.removeGranted(table);
                    }
                }
                table.readPermissionsInfo();
            }

        } finally {
            logger.exiting(method);
        }
    }

}
