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
 * XDBSecurityException.java
 * 
 *  
 */
package org.postgresql.stado.exception;

import java.sql.SQLException;

/**
 *  
 */
public class XDBSecurityException extends SQLException {

    /**
     * 
     */
    private static final long serialVersionUID = -3649579622748499755L;

    /**
     * @param reason
     */
    public XDBSecurityException(String reason) {
        super(reason);
        // TODO Auto-generated constructor stub
    }

    /**
     * @param reason
     * @param SQLState
     */
    public XDBSecurityException(String reason, String SQLState) {
        super(reason, SQLState);
        // TODO Auto-generated constructor stub
    }

    /**
     * @param reason
     * @param SQLState
     * @param vendorCode
     */
    public XDBSecurityException(String reason, String SQLState, int vendorCode) {
        super(reason, SQLState, vendorCode);
        // TODO Auto-generated constructor stub
    }

}
