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
 * SyncGrant.java
 * 
 *  
 */
package org.postgresql.stado.metadata;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.parser.SqlGrant;


/**
 *  
 */
public class SyncGrant implements IMetaDataUpdate {
    private static final XLogger logger = XLogger.getLogger(SyncGrant.class);

    private SqlGrant parent;

    private int tabprivid = -1;

    private PreparedStatement stmtCreate;

    private PreparedStatement stmtUpdate;

    /**
     * 
     */
    public SyncGrant(SqlGrant parent) {
        this.parent = parent;
    }

    private void createDefaultPermissions(int tableID, int userID) throws SQLException {
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
        String sCreate = "insert into xsystabprivs "
            + "(privid, userid, tableid, selectpriv, insertpriv, "
            + "updatepriv, deletepriv, referencespriv, indexpriv, alterpriv) "
            + "values ("+ tabprivid++ + ", " + userID + ", "+ tableID
            + ", 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y')";
        if (MetaData.getMetaData().executeUpdate(sCreate) != 1) {
            XDBServerException ex = new XDBServerException(
                    "Failed to insert row into \"xsystabprivs\"");
            logger.throwing(ex);
            throw ex;
        }
    }

    private void createFor(int tableId, SysUser user) throws SQLException {
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
        if (stmtCreate == null) {
            StringBuffer sbCreate = new StringBuffer(
                    "insert into xsystabprivs "
                            + "(privid, userid, tableid, selectpriv, insertpriv, "
                            + "updatepriv, deletepriv, referencespriv, indexpriv, alterpriv) "
                            + "values (?, ?, ?, ");
            sbCreate.append(parent.hasSelect() ? "'Y', " : "'N', ");
            sbCreate.append(parent.hasInsert() ? "'Y', " : "'N', ");
            sbCreate.append(parent.hasUpdate() ? "'Y', " : "'N', ");
            sbCreate.append(parent.hasDelete() ? "'Y', " : "'N', ");
            sbCreate.append(parent.hasReferences() ? "'Y', " : "'N', ");
            sbCreate.append(parent.hasIndex() ? "'Y', " : "'N', ");
            sbCreate.append(parent.hasAlter() ? "'Y')" : "'N')");
            stmtCreate = MetaData.getMetaData().prepareStatement(sbCreate.toString());
        }
        stmtCreate.setInt(1, tabprivid++);
        if (user == null) {
            stmtCreate.setNull(2, Types.INTEGER);
        } else {
            stmtCreate.setInt(2, user.getUserID());
        }
        stmtCreate.setInt(3, tableId);
        if (stmtCreate.executeUpdate() != 1) {
            XDBServerException ex = new XDBServerException(
                    "Failed to insert row into \"xsystabprivs\"");
            logger.throwing(ex);
            throw ex;
        }
    }

    private void updateFor(int privId) throws SQLException {
        if (stmtUpdate == null) {
            StringBuffer sbUpdate = new StringBuffer("update xsystabprivs set ");
            if (parent.hasSelect()) {
                sbUpdate.append("selectpriv = 'Y', ");
            }
            if (parent.hasInsert()) {
                sbUpdate.append("insertpriv = 'Y', ");
            }
            if (parent.hasUpdate()) {
                sbUpdate.append("updatepriv = 'Y', ");
            }
            if (parent.hasDelete()) {
                sbUpdate.append("deletepriv = 'Y', ");
            }
            if (parent.hasReferences()) {
                sbUpdate.append("referencespriv = 'Y', ");
            }
            if (parent.hasIndex()) {
                sbUpdate.append("indexpriv = 'Y', ");
            }
            if (parent.hasAlter()) {
                sbUpdate.append("alterpriv = 'Y', ");
            }
            // Strip away last comma
            sbUpdate.setLength(sbUpdate.length() - 2);
            sbUpdate.append(" where privid = ?");
            stmtUpdate = MetaData.getMetaData().prepareStatement(
                    sbUpdate.toString());
        }
        stmtUpdate.setInt(1, privId);
        if (stmtUpdate.executeUpdate() != 1) {
            XDBServerException ex = new XDBServerException(
                    "Failed to update row in \"xsystabprivs\"");
            logger.throwing(ex);
            throw ex;
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
                    // Create default permissions
                    createDefaultPermissions(table.getTableId(), table.getOwner().getUserID());
                    // All permissions for table owner just have been granted
                    parent.getGranteeList().remove(table.getOwner());
                }
                for (SysUser user : parent.getGranteeList()) {
                    SysPermission permission = table.getSysPermission(user);
                    if (permission == null) {
                        createFor(table.getTableId(), user);
                    } else {
                        updateFor(permission.getPermissionId());
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

            for (SysTable table : parent.getTableList()) {
                table.readPermissionsInfo();
            }

        } finally {
            logger.exiting(method);
        }
    }

}
