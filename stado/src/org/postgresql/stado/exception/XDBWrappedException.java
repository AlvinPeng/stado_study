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
 * XDBWrappedException.java
 * 
 *  
 */
package org.postgresql.stado.exception;

/**
 *  
 */
public class XDBWrappedException extends XDBBaseException {
    /**
     * 
     */
    private static final long serialVersionUID = 8646201999174421550L;
    private StackTraceElement[] stackTrace;

    /**
     * 
     */
    public XDBWrappedException() {
        super();
        // TODO Auto-generated constructor stub
    }

    /**
     * @param nodeID the node ID
     * @param cause the cause
     */
    public XDBWrappedException(int nodeID, Throwable cause) {
        super(nodeID, cause.getClass().getName() + " : " + cause.getMessage());
        stackTrace = cause.getStackTrace();
    }

    /**
     * 
     * @return 
     */
    public StackTraceElement[] getCauseStackTrace() {
        return stackTrace;
    }
}
