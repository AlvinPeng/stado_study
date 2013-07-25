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
 *  
 *
 */
package org.postgresql.stado.metadata;


public class SysPermission {
    public static final short ACCESS_GRANTED = 1;

    // For future use (e.g. Columns)
    public static final short ACCESS_INHERITED = 0;

    public static final short ACCESS_DENIED = -1;

    public static final short PRIVILEGE_SELECT = 0;

    public static final short PRIVILEGE_INSERT = 1;

    public static final short PRIVILEGE_UPDATE = 2;

    public static final short PRIVILEGE_DELETE = 3;

    public static final short PRIVILEGE_REFERENCE = 4;

    public static final short PRIVILEGE_INDEX = 5;

    public static final short PRIVILEGE_ALTER = 6;

    // Subject to configure
    private static final short ACCESS_DEFAULT = ACCESS_DENIED;

    // For future use (e.g. Columns)
    private SysPermission parent;

    private Object metadataObject;

    private SysUser user;

    private int permissionId;

    private short select;

    private short insert;

    private short update;

    private short delete;

    private short reference;

    private short index;

    private short alter;

    /**
     * @param metadataObject
     * @param user
     * @param select
     * @param insert
     * @param update
     * @param delete
     */
    public SysPermission(Object metadataObject, SysUser user, int permissionId,
            String select, String insert, String update, String delete,
            String reference, String index, String alter) {
        this.metadataObject = metadataObject;
        this.user = user;
        this.permissionId = permissionId;
        this.select = strToShort(select);
        this.insert = strToShort(insert);
        this.update = strToShort(update);
        this.delete = strToShort(delete);
        this.reference = strToShort(reference);
        this.index = strToShort(index);
        this.alter = strToShort(alter);
    }

    private short strToShort(String code) {
        if (code == null || code.length() == 0) {
            return ACCESS_INHERITED;
        }
        switch (code.charAt(0)) {
        case 'Y':
        case 'y':
        case 'T':
        case 't':
        case '1':
            return ACCESS_GRANTED;
        case 'N':
        case 'n':
        case 'F':
        case 'f':
        case '-':
            return ACCESS_DENIED;
        default:
            return ACCESS_INHERITED;
        }
    }

    /**
     * @return Returns the delete.
     */
    public short getDelete() {
        if (delete == ACCESS_INHERITED) {
            return parent == null ? ACCESS_DEFAULT : parent.getDelete();
        }
        return delete;
    }

    /**
     * @return Returns the insert.
     */
    public short getInsert() {
        if (insert == ACCESS_INHERITED) {
            return parent == null ? ACCESS_DEFAULT : parent.getInsert();
        }
        return insert;
    }

    /**
     * @return Returns the parent.
     */
    public SysPermission getParent() {
        return parent;
    }

    /**
     * @param parent
     *            The parent to set.
     */
    public void setParent(SysPermission parent) {
        this.parent = parent;
    }

    /**
     * @return Returns the select.
     */
    public short getSelect() {
        if (select == ACCESS_INHERITED) {
            return parent == null ? ACCESS_DEFAULT : parent.getSelect();
        }
        return select;
    }

    /**
     * @return Returns the update.
     */
    public short getUpdate() {
        if (update == ACCESS_INHERITED) {
            return parent == null ? ACCESS_DEFAULT : parent.getUpdate();
        }
        return update;
    }

    /**
     * @return Returns the index.
     */
    public short getIndex() {
        if (index == ACCESS_INHERITED) {
            return parent == null ? ACCESS_DEFAULT : parent.getIndex();
        }
        return index;
    }

    /**
     * @return Returns the reference.
     */
    public short getReference() {
        if (reference == ACCESS_INHERITED) {
            return parent == null ? ACCESS_DEFAULT : parent.getReference();
        }
        return reference;
    }

    /**
     * @return Returns the alter.
     */
    public short getAlter() {
        if (alter == ACCESS_INHERITED) {
            return parent == null ? ACCESS_DEFAULT : parent.getAlter();
        }
        return alter;
    }

    /**
     * @return
     */
    public int getPermissionId() {
        return permissionId;
    }

    /**
     * @return Returns the target.
     */
    public Object getTarget() {
        return metadataObject;
    }

    /**
     * @return Returns the user.
     */
    public SysUser getUser() {
        return user;
    }

    boolean checkPermission(short privilege) {
        short access = ACCESS_DEFAULT;
        switch (privilege) {
        case PRIVILEGE_SELECT:
            access = getSelect();
            break;
        case PRIVILEGE_INSERT:
            access = getInsert();
            break;
        case PRIVILEGE_UPDATE:
            access = getUpdate();
            break;
        case PRIVILEGE_DELETE:
            access = getDelete();
            break;
        case PRIVILEGE_REFERENCE:
            access = getReference();
            break;
        case PRIVILEGE_INDEX:
            access = getIndex();
            break;
        case PRIVILEGE_ALTER:
            access = getAlter();
            break;
        }
        return access == ACCESS_GRANTED;
    }
}
