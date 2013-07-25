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
 * XDBIllegalStateException.java
 * 
 *  
 */
package org.postgresql.stado.exception;

/**
 * Raised in org.postgresql.stado.Engine.NodeThread and org.postgresql.stado.Engine.NodeProdocerThread
 * when it can not do requested operation due to unexpected state
 * 
 *  
 */
public class XDBUnexpectedStateException extends XDBBaseException {
    /**
     * 
     */
    private static final long serialVersionUID = -7066065187595622523L;

    private int currentState;

    private int[] expectedStates;

    /**
     * 
     */
    public XDBUnexpectedStateException() {
        super();
        // TODO Auto-generated constructor stub
    }

    /**
     * @param nodeID the node ID
     * @param currentState encountered state ID 
     * @param expectedStates valid state IDs
     */
    public XDBUnexpectedStateException(int nodeID, int currentState,
            int[] expectedStates) {
        super(nodeID,
                "Could not perform requested operation due to unexpected state: "
                        + currentState);
        this.currentState = currentState;
        this.expectedStates = expectedStates;
    }

    /**
     * @return Returns the currentState.
     */
    public int getCurrentState() {
        return currentState;
    }

    /**
     * @return Returns the expectedStates.
     */
    public int[] getExpectedStates() {
        return expectedStates;
    }
}
