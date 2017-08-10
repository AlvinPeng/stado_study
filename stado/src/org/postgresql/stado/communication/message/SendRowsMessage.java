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
 * SendRowsMessage.java
 *
 */

package org.postgresql.stado.communication.message;

/**
 *
 * 
 */
public class SendRowsMessage extends NodeMessage {
    
    /** Parameterless constructor required for serialization */
    public SendRowsMessage() {
    }

    //private static final long serialVersionUID = 2294439851011666205L;

    private boolean isStartMessage = true;
    
    private long numRowsSent = 0;
    
    private int destNodeForRows = -1;

    /**
     * @param messageType
     */
    protected SendRowsMessage(int messageType)
    {
        super(messageType);
    }
   
    public void setIsStartMessage(boolean value)
    {
        isStartMessage = value;
    }  
    
    public boolean getIsStartMessage()
    {
        return isStartMessage;
    }    
    
    public void setNumRowsSent(long value)
    {
        numRowsSent = value;
    }  
    
    public long getNumRowsSent()
    {
        return numRowsSent;
    }       
    
    public void setDestNodeForRows(int value)
    {
        destNodeForRows = value;
    }  
    
    public int getDestNodeForRows()
    {
        return destNodeForRows;
    }      
}

