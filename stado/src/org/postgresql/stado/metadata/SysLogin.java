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
/**
 * 
 */
package org.postgresql.stado.metadata;

import java.util.Arrays;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.exception.XDBSecurityException;
import org.postgresql.stado.exception.XDBServerException;


/**
 * 
 * 
 */
public class SysLogin {
    private static final XLogger logger = XLogger.getLogger(SysLogin.class);

    public static final int USER_CLASS_DBA = 0;

    public static final int USER_CLASS_RESOURCE = 1;

    public static final int USER_CLASS_STANDARD = 2;

    public static final String USER_CLASS_DBA_STR = "DBA";

    public static final String USER_CLASS_RESOURCE_STR = "RESOURCE";

    public static final String USER_CLASS_STANDARD_STR = "STANDARD";

    private int id;

    private String name;

    private int userClass = -1;

    private String password;

    public static final String encryptPassword(String password) {
        try {
            java.security.MessageDigest md5 = java.security.MessageDigest
            .getInstance("MD5");
            if (md5 != null) {
                byte[] res = new byte[16];
                Arrays.fill(res, (byte) 0);
                md5.update(password.getBytes());
                md5.digest(res, 0, res.length);
                StringBuffer out = new StringBuffer(32);
                for (byte element : res) {
                    int high = element >> 4 & 0xf;
                    out
                    .append((char) (high < 10 ? '0' + high
                            : 'a' + high - 10));
                    int low = element & 0xf;
                    out.append((char) (low < 10 ? '0' + low : 'a' + low - 10));
                }
                return out.toString();
            }
        } catch (Exception e) {
            logger.catching(e);
        }
        return "";
    }

    public static String getUserClassStr(int userClass) {
        switch (userClass) {
        case SysLogin.USER_CLASS_DBA:
            return SysLogin.USER_CLASS_DBA_STR;
        case SysLogin.USER_CLASS_RESOURCE:
            return SysLogin.USER_CLASS_RESOURCE_STR;
        case SysLogin.USER_CLASS_STANDARD:
        default:
            return SysLogin.USER_CLASS_STANDARD_STR;
        }
    }
    
        /**
     * @return Returns the iUserClass.
     */
    public static int getUserClass(String sUserClass) {
        
        if (sUserClass.equalsIgnoreCase(USER_CLASS_DBA_STR)) {
            return USER_CLASS_DBA;
        } else if (sUserClass.equalsIgnoreCase(USER_CLASS_RESOURCE_STR)) {
            return USER_CLASS_RESOURCE;        
        } else {
            return USER_CLASS_STANDARD;
        }
            
    }

    /**
     * 
     */
    public SysLogin(int id, String name, String password, String userClass) {
        this.id = id;
        this.name = name;
        this.password = password;
        setUserClass(userClass);
    }

    public void setPassword(String newPassword, boolean plain)
    throws XDBSecurityException {
        if (newPassword == null || newPassword.length() == 0) {
            XDBSecurityException ex = new XDBSecurityException(
            "Failed to encrypt password");
            logger.throwing(ex);
            throw ex;
        }
        password = plain ? encryptPassword(newPassword) : newPassword;
    }

    public void setUserClass(String userClass) {
        String value = userClass.toUpperCase().trim();
        if (SysLogin.USER_CLASS_DBA_STR.equals(value)) {
            this.userClass = USER_CLASS_DBA;
        } else if (SysLogin.USER_CLASS_RESOURCE_STR.equals(value)) {
            this.userClass = SysLogin.USER_CLASS_RESOURCE;
        } else // USER_CLASS_STANDARD_STR
        {
            this.userClass = SysLogin.USER_CLASS_STANDARD;
        }
    }

    public void checkPassword(String password) throws XDBSecurityException {
        if (!this.password.equals(encryptPassword(password))) {
            throw new XDBSecurityException("Invalid login");
        }
    }

    public int getLoginID() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getUserClass() {
        if (id == -1) {
            throw new XDBServerException("User " + name + " was concurrently dropped");
        }
        return userClass;
    }

    /**
     * 
     * @return encrypted password
     */
    String getPassword() {
        return password;
    }

    /**
     * @param user_class_standard_str2
     */
    public void canSetUserClass(String userClass) throws XDBSecurityException {
        final String method = "canSetUserClass";
        logger.entering(method, new Object[] {});
        try {

            String value = userClass.toUpperCase().trim();
            if (this.userClass == USER_CLASS_DBA
                    && !SysLogin.USER_CLASS_DBA_STR.equals(value)) {
                boolean found = false;
                for (SysLogin login : MetaData.getMetaData().getSysLogins()) {
                    if (login == this) {
                        continue;
                    }
                    if (login.userClass == USER_CLASS_DBA) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    XDBSecurityException ex = new XDBSecurityException(
                            "System must have at least one "
                            + SysLogin.USER_CLASS_DBA_STR + " user");
                    logger.throwing(ex);
                    throw ex;
                }
            }
            if (SysLogin.USER_CLASS_STANDARD_STR.equals(value)) {
                for (SysDatabase database : MetaData.getMetaData().getSysDatabases()) {
                    // Load database metadata if not loaded
                    database.admin();
                    SysUser dbUser = database.getSysUser(name);
                    String owned = dbUser.getOwnedStr();
                    if (owned != null) {
                        XDBSecurityException ex = new XDBSecurityException(
                                "User " + name + " owns some objects: " + owned);
                        logger.throwing(ex);
                        throw ex;
                    }
                }
            }

        } finally {
            logger.exiting(method);
        }
    }

    void invalidate() {
        id = -1;
    }

    @Override
    public String toString() {
        return name + "[" + getUserClassStr(userClass) + "]";
    }
    
    public boolean isDBA() {
        return (userClass == USER_CLASS_DBA);
    }
}
