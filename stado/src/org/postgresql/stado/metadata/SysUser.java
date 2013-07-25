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

import java.util.HashSet;

import org.postgresql.stado.common.util.XLogger;



public class SysUser {
    private static final XLogger logger = XLogger.getLogger(SysUser.class);

    private int id;

    private SysLogin login;

    private SysDatabase database;

    private HashSet ownerOf;

    private HashSet permissionsOn;

    /**
     * 
     */
    public SysUser(int id, SysLogin login, SysDatabase database) {
        this.id = id;
        this.login = login;
        this.database = database;
        ownerOf = new HashSet();
        permissionsOn = new HashSet();
    }

    public int getUserID() {
        return id;
    }

    void setUserID(int userID) {
        if (id == -1) {
            id = userID;
        }
    }

    public SysLogin getLogin() {
        return login;
    }

    public String getName() {
        return login.getName();
    }

    public int getUserClass() {
        return login.getUserClass();
    }

    void addOwned(Object obj) {
        ownerOf.add(obj);
    }

    void removeOwned(Object obj) {
        ownerOf.remove(obj);
    }

    public String getOwnedStr() {
        if (ownerOf.isEmpty()) {
            return null;
        }
        StringBuffer owners = new StringBuffer();
        for (Object obj : ownerOf) {
            owners.append(obj).append(", ");
        }
        return owners.substring(0, owners.length() - 2);
    }

    void addGranted(Object obj) {
        permissionsOn.add(obj);
    }

    void removeGranted(Object obj) {
        permissionsOn.remove(obj);
    }

    public String getGrantedStr() {
        if (permissionsOn.isEmpty()) {
            return null;
        }
        StringBuffer permissions = new StringBuffer();
        for (Object obj : permissionsOn) {
            permissions.append(obj).append(", ");
        }
        return permissions.substring(0, permissions.length() - 2);
    }

    @Override
    public String toString() {
        return login + "@" + database.getDbname();
    }
}
