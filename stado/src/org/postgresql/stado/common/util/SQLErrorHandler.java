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
 * SQLErrorHandler.java
 *
 *  
 */

package org.postgresql.stado.common.util;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

/**
 * Use this class to retrieve SQL Exceptions, where each will contain: 1. vendor
 * code - integer, (see ErrorCodes.java) 2. sql state - string, (ansi) 3.
 * message - string
 * 
 * 
 *  
 */
public class SQLErrorHandler {
    // key -> code
    // value -> sqlstate, message
    private static Map<String, String[]> eTable = null;

    static {
        eTable = Collections.synchronizedMap(new HashMap<String, String[]>());

        try {
            // load up all error codes
            // only English for now
            ResourceBundle rb = ResourceBundle
                    .getBundle("org.postgresql.stado.common.util.SQLErrorMessages");
            Enumeration<String> keys = rb.getKeys();

            String key = null;
            String value = null;
            while (keys.hasMoreElements()) {
                key = keys.nextElement();
                value = rb.getString(key);
                eTable.put(key, new String[] { value.substring(0, 5),
                        value.substring(6) });
            }
        } catch (Exception e) {
        }
    }

    /** Creates a new instance of SQLErrorHandler */
    private SQLErrorHandler() {
    }

    /**
     * return the default error message assigned to this error code
     * 
     * @param vendorCode
     *            the vendor's specific code - see ErrorCodes.java
     * @return SQLException
     */
    public static SQLException getError(int vendorCode) {
        return getError(vendorCode, null);
    }

    /**
     * return the SQLExeption for the error code, but for the error message, use
     * the specified reason
     * 
     * @param vendorCode
     *            the XDB specific error code
     * @param reason
     *            if not null, use this message instead of the default
     * @return SQLException
     */
    public static SQLException getError(int vendorCode, String reason) {
        String k = "" + vendorCode;
        if (!eTable.containsKey(k)) {
            // code not setup yet, use generic msg
            k = "" + ErrorCodes.AN_ERROR_HAS_OCCURRED;
        }
        String[] v = eTable.get(k);
        if (v == null) {
            v = new String[] { "60000", "An error has occurred" };
        }
        return new SQLException(reason == null ? v[1] : reason, v[0], Integer
                .parseInt(k));
    }
}
