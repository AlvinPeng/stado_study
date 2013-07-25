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
package org.postgresql.stado.exception;


/**
 * This is a Server Exception - All thoes exceptions which are not to be sent to
 * the client should use this exception.
 * 
 * 
 */
public class XDBServerException extends RuntimeException {

    /**
     * 
     */
    private static final long serialVersionUID = -2965393859623567628L;

    public static final int SEVERITY_HIGH = 8;

    public static final int SEVERITY_LOW = 2;

    public static final int SEVERITY_MEDIUM = 4;

    int severity = 0;

    /**
     * 
     * @param xdbOperation 
     * @param cause 
     * @param code 
     */
    public XDBServerException(String xdbOperation, Exception cause, int code) {
        super(xdbOperation, cause);
        log();

    }

    /**
     * 
     * @param xdbOperation 
     * @param cause 
     */
    public XDBServerException(String xdbOperation, Exception cause) {
        super(xdbOperation, cause);
        log();
    }

    /**
     * 
     * This function logs the exception depending on whether the user has asked
     * us to log it.
     */
    private void log() {
        // Log the exception -- Use Logj4 to do the logging

    }

    /**
     * 
     * @param message 
     */
    public XDBServerException(String message) {
        super(message);
        this.severity = XDBServerException.SEVERITY_HIGH;
    }

    /**
     * 
     * @param message 
     * @param severity 
     * @param errorCode 
     */
    public XDBServerException(String message, int severity, int errorCode) {
        super(message);
        this.severity = severity;
        log();
    }
}
