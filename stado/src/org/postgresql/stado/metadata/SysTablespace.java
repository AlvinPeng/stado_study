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

import java.util.Map;

/**
 *
 *
 */
public class SysTablespace {
    private int tablespaceID;

    private String tablespaceName;

    private Map<Integer,String> locations;

    private int ownerID;

    /**
     * @param locations
     * @param ownerid
     * @param tablespaceid
     * @param name
     */
    public SysTablespace(int tablespaceid, String name, int ownerid,
            Map<Integer,String> locations) {
        this.locations = locations;
        ownerID = ownerid;
        tablespaceID = tablespaceid;
        tablespaceName = name;
    }

    /**
     * @return Returns the locations.
     */
    public Map<Integer,String> getLocations() {
        return locations;
    }

    /**
     * @return Returns the ownerID.
     */
    public int getOwnerID() {
        return ownerID;
    }

    /**
     * @return Returns the tablespaceID.
     */
    public int getTablespaceID() {
        return tablespaceID;
    }

    /**
     * @return Returns the tablespaceName.
     */
    public String getTablespaceName() {
        return tablespaceName;
    }

    /**
     * @return Returns the tablespaceName for the specified node
     */
    public String getNodeTablespaceName(int nodeId) {
        // we generate the name, but long term this can be made to be
        // arbitrary and put in the locations Map
        return tablespaceName + "_" + nodeId;
    }
    
    /**
     * @param newName
     */
    void setName(String newName) {
        tablespaceName = newName;
    }
}
