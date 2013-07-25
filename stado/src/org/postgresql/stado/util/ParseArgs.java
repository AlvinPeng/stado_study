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
 * ParseArgs.java
 *
 * Broke this out from ParseArgs, due to compilation of cmdline and it
 * including log4j
 *
 *  
 */

package org.postgresql.stado.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 * 
 */
public class ParseArgs {

    /** Creates a new instance of ParseArgs */
    public ParseArgs() {
    }

    /**
     * 
     * @param args 
     * @throws java.lang.Exception 
     * @return 
     */

    public static Map<String, List<String>> parse(String args[])
            throws Exception {

        return parse(args, null);
    }

    /**
     * 
     * @param args 
     * @param allowedArgs 
     * @throws java.lang.Exception 
     * @return 
     */

    public static Map<String, List<String>> parse(String args[],
            String allowedArgs) throws Exception {
        Map<String, List<String>> result = new HashMap<String, List<String>>();
        List<String> currentList = null;
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("-") &&
                // FB8121(Zahid): to handle the case where a user may specify a sequence of "-" as 
                // an option vlaue e.g. for comments prefix i.e. -r "--"    
                !args[i].matches("[-]+")) {
                String key = args[i].toLowerCase();

                if (allowedArgs != null
                        && allowedArgs.indexOf(key.substring(1)) < 0) {
                    throw new Exception("Unknown argument: " + key);
                }
                currentList = result.get(key);
                if (currentList == null) {
                    currentList = new ArrayList<String>();
                    result.put(key, currentList);
                }
            } else {
                if (currentList == null) {
                    currentList = new ArrayList<String>();
                    result.put(null, currentList);
                }
                currentList.add(args[i]);
            }
        }
        return result;
    }

    /**
     * 
     * @param m 
     * @param key 
     * @return 
     */
    public static String getStrArg(Map<String, List<String>> m, String key) {
        if (!key.startsWith("-")) {
            key = "-" + key;
        }
        List<String> args = m.get(key); // .remove(key)
        if (args == null) {
            return null;
        }
        // allow options to not be required to have extra args
        // allow us to differentiate by comparing to ""
        if (args.isEmpty()) {
            return "";
        }
        return args.get(0);
    }

}
