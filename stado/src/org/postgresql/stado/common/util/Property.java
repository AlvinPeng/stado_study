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
package org.postgresql.stado.common.util;

import java.io.FileInputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * This class provides static access to the XDB string properties. By default,
 * the System's properties has higher precedence over the file properties, i.e.
 * xdb.config.
 * 
 * The default config file is "xdb.config" - you can override this by setting
 * the system's "config.file.path" property.
 * 
 *  
 */
public class Property {
    private static final XLogger serverLogger = XLogger.getLogger("Server");
    private static java.util.Properties props = new java.util.Properties();
    static {
        load();
    }

    /**
     * Returns true if it is critical the specified parameter is the same in
     * server and agent configuration files Critical keys are
     * xdb.coordinator.host xdb.coordinator.port xdb.node.*.port
     * 
     * @param key
     *            key to check
     * @return
     */
    private static boolean isParameterImportant(String key) {
        if ("xdb.coordinator.host".equals(key))
            return true;
        if ("xdb.coordinator.port".equals(key))
            return true;
        if (key.matches("xdb\\.node\\.[1-9][0-9]*\\.port"))
            return true;
        return false;
    }

    /**
     * If value and oldValue differ, check if they still equivalent we would not
     * like to fail if one config file specifies 127.0.0.1 and other localhost
     * as an IP address etc.
     * 
     * @param key
     *            key is needed to have an idea what value is
     * @param oldValue
     *            value found in the config file
     * @param newValue
     *            value sent from server
     * @return true if values are equivalent
     */
    private static boolean checkThoroughly(String key, String oldValue,
            String newValue) {
        if (key.endsWith(".host")) {
            // The value is an IP address
            try {
                InetAddress oldAddress = InetAddress.getByName(oldValue);
                InetAddress newAddress = InetAddress.getByName(newValue);
                return newAddress.equals(oldAddress);
            } catch (UnknownHostException ex) {
                return false;
            }
        } else if (key.endsWith(".port")) {
            // The value is a number
            try {
                return Integer.parseInt(oldValue) == Integer.parseInt(oldValue);
            } catch (NumberFormatException nfe) {
                return false;
            }
        }
        return false;
    }

    /** Creates a new instance of Property */
    private Property() {
    }

    /**
     * returns the property indicated by the specified key. A null value is
     * returned if the property is not found.
     */
    public static String get(String key) {
        return props.getProperty(key);
    }

    /**
     * returns the property indicated by the specified key. If no such property
     * found, return the defaultVal
     */
    public static String get(String key, String defaultVal) {
        return props.getProperty(key, defaultVal);
    }

    public static int getInt(String key, int defaultVal) {
        int val = defaultVal;
        try {
            val = Integer.parseInt(get(key));
        } catch (Exception e) {
        }
        return val;
    }

    public static long getLong(String key, long defaultVal) {
        long val = defaultVal;
        try {
            val = Long.parseLong(get(key));
        } catch (Exception e) {
        }
        return val;
    }

    /**
     * true is defined as the string: 1, T, TRUE, Y, or YES
     */
    public static boolean getBoolean(String key, boolean defaultVal) {
        boolean val = defaultVal;
        try {
            String v = get(key, "").trim().toUpperCase();
            if (v.equals("1") || v.equals("TRUE") || v.equals("T")
                    || v.equals("Y") || v.equals("YES")) {
                val = true;
            } else if (v.equals("0") || v.equals("FALSE") || v.equals("F")
                    || v.equals("N") || v.equals("NO")) {
                val = false;
            }
        } catch (Exception e) {
        }
        return val;
    }

    /**
     * Makes a copy and returns a Map of all properties
     */
    public static java.util.Properties getProperties() {
        return new java.util.Properties(props);
    }

    public static void addLines(String[] args) {

        for (int i = 0; i < args.length; i++) {
            if (args[i] == null) {
                continue;
            }
            int n = args[i].indexOf("=");
            String key;
            String value;
            if (n == -1) {
                key = args[i];
                value = "";
            } else {
                key = args[i].substring(0, n);
                value = args[i].substring(n + 1);
            }
            String oldValue = props.getProperty(key);
            if (oldValue != null) {
                if (!oldValue.equals(value)) {
                    if (isParameterImportant(key)) {
                        if (!checkThoroughly(key, oldValue, value)) {
                            serverLogger
                                    .error(String
                                            .format("Server has sent new value \"%s\" for key \"%s\", "
                                                    + "while value in the config file is different: \"%s\"",
                                                    value, key, oldValue));
                            System.exit(1);
                        }
                    } else {
                        serverLogger
                                .warn(String
                                        .format("Server has sent new value \"%s\" for key \"%s\", "
                                                + "while value in the config file is different: \"%s\"",
                                                value, key, oldValue));
                    }
                }
            }
            props.setProperty(key, value);
            try {
                System.setProperty(key, value);
            } catch (SecurityException se) {
                // Ignore
            }
        }

    }

    public static void setProperty(String key, String value) {

        props.setProperty(key, value);
        try {
            System.setProperty(key, value);
        } catch (SecurityException se) {
            // Ignore
        }
    }

    private static void load() {
        synchronized (props) {
            parseFile();
            try {
                // Access to System properties are controlled by Security
                // Manager
                // This is important for Driver, that can be in Applet or
                // Application server
                props.putAll(System.getProperties());
                System.getProperties().putAll(props);
            } catch (SecurityException se) {
                // Ignore
            }
        }
    }

    private static void parseFile() {
        FileInputStream fis = null;
        try {
            String filePath = System.getProperty("config.file.path",
                    "stado.config");
            fis = new FileInputStream(filePath);
            props.load(fis);
        } catch (Exception e) {
            // Silently ignore exception, because we can access the class from
            // somewhere outside Server: (Agent, Client) and have them loaded
            // from System.getProperties() or somewhat else
            // e.printStackTrace();
        } finally {
            try {
                fis.close();
            } catch (Exception ignore) {
            }
        }
    }
}
