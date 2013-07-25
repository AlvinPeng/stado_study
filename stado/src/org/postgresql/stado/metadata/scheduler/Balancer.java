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
 * Balancer.java
 *
 *  
 */

package org.postgresql.stado.metadata.scheduler;

import java.util.Collection;

import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.SysDatabase;

/**
 * 
 * 
 * 
 * We just use round-robin for now. Now that the rest of the work has been done,
 * we can change it later to least pending requests. Not sure if it makes sense
 * to tie this to the DB. It should probably be system wide, but I wanted to
 * keep it together with the Scheduler, in case it is more tightly integrated.
 */
public class Balancer {

    private int[] nodeIds;
    private int curr_index;

    /** Creates a new instance of Balancer */
    public Balancer(SysDatabase sysDatabase) {
        Collection<DBNode> nodeList = sysDatabase.getDBNodeList();
        int i = 0;
        nodeIds = new int[nodeList.size()];
        for (DBNode dbNode : nodeList)
            nodeIds[i++] = dbNode.getNodeId();
        curr_index = 0;
    }

    /** Get next node id to use */
    public synchronized int getNextNodeId() {
        if (++curr_index == nodeIds.length)
            curr_index = 0;

        return nodeIds[curr_index];
    }
}
