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
package org.postgresql.stado.common;

import org.postgresql.stado.engine.io.MessageTypes;
import org.postgresql.stado.engine.io.ResponseMessage;

/**
 * 
 *  
 */
public class ErrorMessages {

    public static final int CONNECT_ERROR = 101;

    public static final int CONNECTION_TIMEOUT = 102;

    public static final int AN_ERROR_HAS_OCCURRED = 103;

    public static final int INVALID_COMMAND = 104;

    public static final int FEATURE_NOT_SUPPORTED = 105;

    public static final int SYNTAX_ERROR = 106;

    /**
     * 
     * @param t 
     * @return 
     */
    public static ResponseMessage getErrorMessage(Throwable t) {
        ResponseMessage response = new ResponseMessage(
                (byte) MessageTypes.RESP_ERROR_MESSAGE, 0);

        response.storeInt(AN_ERROR_HAS_OCCURRED);
        response.storeString(t.getMessage());
        for (t = t.getCause(); t != null; t = t.getCause()) {
            response.storeByte((byte) 1);
            response.storeInt(AN_ERROR_HAS_OCCURRED);
            response.storeString(t.getMessage());
        }
        response.storeByte((byte) 0);
        return response;
    }
}
