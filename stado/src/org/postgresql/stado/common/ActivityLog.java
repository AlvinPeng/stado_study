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
 * ActivityLog.java
 *
 */

package org.postgresql.stado.common;

import org.apache.log4j.Logger;
import org.postgresql.stado.common.util.Property;
import org.postgresql.stado.engine.XDBSessionContext;


/**
 *
 * 
 */
public class ActivityLog {
    
    /** Creates a new instance of ActivityLog */
    public ActivityLog() {
    }
    
    /** grid activity logger */
    public static final Logger activityLogger = Logger.getLogger("activity");
    
    public static void setAdditivity (boolean value)
    {
        activityLogger.setAdditivity (value);   
    }
    
    public static void startRequest (long requestId, String statement)
    {
        activityLogger.info(requestId + ",B," + statement);
    }

    public static void endRequest (long requestId)
    {
        activityLogger.info(requestId + ",E");
    }

    public static void startStep (long requestId, int sourceNodeId)
    {
        activityLogger.info(requestId + ",Q," + sourceNodeId);
    }

    public static void endStep (long requestId, int sourceNodeId)
    {
        activityLogger.info(requestId + ",E," + sourceNodeId);
    }
    
    public static void startShipRows (long requestId, int sourceNodeId, int destNodeId)
    {
        activityLogger.info(requestId + ",S," + sourceNodeId + "," + destNodeId);
    }  
    
    public static void endShipRows (long requestId, int sourceNodeId, 
            int destNodeId, long numShippedRows)
    {
        activityLogger.info(requestId + ",F," + sourceNodeId + "," + destNodeId + "," + numShippedRows);
    }      
}
