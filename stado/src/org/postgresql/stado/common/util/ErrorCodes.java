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
 * ErrorCodes.java
 *
 *  
 */

package org.postgresql.stado.common.util;

/**
 * XDB (vendor) error codes. This is part of the SQLException message
 * 
 *  
 */
public class ErrorCodes {
    public static final int METHOD_NOT_SUPPORTED = 5000;

    public static final int METHOD_NOT_SUPPORTED_FOR_JDK = 5001;

    /** this feature is not supported by the current driver version */
    public static final int DRIVER_FEATURE_NOT_SUPPORTED = 5003;

    /**
     * invalid operation, i.e going out of array bound or invalid sequence of
     * method calls
     */
    public static final int METHOD_INVALID_OPERATION = 5004;

    public static final int AN_ERROR_HAS_OCCURRED = 6000;

    /** database access error */
    public static final int CLIENT_UNABLE_TO_CONNECT_TO_DB = 6001;

    public static final int CONNECTION_NAME_ALREADY_USED = 6002;

    public static final int CONNECTION_DOES_NOT_EXIST = 6003;

    public static final int SERVER_REJECTED_NEW_CONNECTIONS = 6004;

    public static final int DATABASE_ACCESS_ERROR = 6005;

}
