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
 * DbGateway.java
 */

package org.postgresql.stado.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.postgresql.stado.common.util.ParseCmdLine;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.StreamGobbler;
import org.postgresql.stado.metadata.NodeDBConnectionInfo;


/**
 *
 *
 */
public class DbGateway {

    private static final String VAR_NODEHOST = "dbhost";

    private static final String VAR_NODEPORT = "dbport";

    private static final String VAR_NODEDB = "database";

    private static final String VAR_DBUSERNAME = "dbusername";

    private static final String VAR_DBPASSWORD = "dbpassword";

    private static final String VAR_PSQLUTILNAME = "psql-util-name";

    private static final String VAR_PGCONFIGUTILNAME = "pgconfig-util-name";

    private boolean force = false;

    /**
     *
     */
    public DbGateway() {

    }

    /**
     *
     * executes the command on the nodes, using the template string.
     *
     * @param templateString
     *
     * @param valueMap
     *
     * @param nodeInfoList
     *
     * @throws java.io.IOException
     *
     */
    private void executeTemplateOnNodes(String templateString,
            HashMap<String, String> valueMap,
            NodeDBConnectionInfo[] nodeInfoList) throws IOException {
        executeTemplateOnNodes(templateString, valueMap, nodeInfoList, true);
    }

    /**
     *
     * executes the command on the nodes, using the template string.
     *
     * @param templateString
     *
     * @param valueMap
     *
     * @param nodeInfoList
     *
     * @param executeInParallel
     *
     * @throws java.io.IOException
     *
     */
    private List<String> executeTemplateOnNodes(String templateString,
            HashMap<String, String> valueMap,
            NodeDBConnectionInfo[] nodeInfoList, boolean executeInParallel)
            throws IOException {
        if (templateString == null) {
            return null;
        }
        String[] commandList = new String[nodeInfoList.length];
        List<String> ret = new ArrayList();

        // Loop through node list, and create on nodes
        for (int i = 0; i < nodeInfoList.length; i++) {
            commandList[i] = ParseCmdLine.substitute(templateString,
                    populateValueMap(nodeInfoList[i], valueMap));
        }

        if (executeInParallel) {
            ret.addAll(executeList(commandList));
        } else {
            String newCmdList[] = new String[1];

            for (String element : commandList) {
                newCmdList[0] = element;

                ret.addAll(executeList(newCmdList));
                
            }
        }
        
        return ret;
    }

    /**
     *
     * Sets the force variable (currently applicable when dropdb is called)
     *
     * @param toForce
     */
    public void setForce(boolean toForce)
    {
        force = toForce;
    }

    /**
     *
     * Adds the database to the specified node
     *
     * @param valueMap
     *
     * @param nodeInfoList
     *
     * @throws java.io.IOException
     *
     */
    public void createDbOnNodes(HashMap<String, String> valueMap,
            NodeDBConnectionInfo[] nodeInfoList) throws IOException {
        // Set parallel mode to false for this command
        executeTemplateOnNodes(Props.XDB_GATEWAY_CREATEDB, valueMap,
                nodeInfoList, false);
    }

    public void addStadoSchemaOnNodes(HashMap<String, String> valueMap,
            NodeDBConnectionInfo[] nodeInfoList) throws IOException {
        // Set parallel mode to false for this command
    	File f = new File(".");
    	String workingDir = f.getCanonicalPath();
    		
        valueMap.put("inputfile", workingDir + "/bin/share/stado_schema.sql");
        executeTemplateOnNodes(Props.XDB_GATEWAY_EXECSCRIPT, valueMap,
                    nodeInfoList, false);        	
    }

    public void addSpatialOnNodes(HashMap<String, String> valueMap,
            NodeDBConnectionInfo[] nodeInfoList) throws IOException {

    	File f = new File(".");
    	String workingDir = f.getCanonicalPath();
    		
        valueMap.put("inputfile", workingDir + "/bin/share/postgis.sql");
        executeTemplateOnNodes(Props.XDB_GATEWAY_EXECSCRIPT, valueMap,
                    nodeInfoList, false);        	

        valueMap.remove("inputfile");
        valueMap.put("inputfile", workingDir + "/contrib/postgis-1.5/spatial_ref_sys.sql");
        executeTemplateOnNodes(Props.XDB_GATEWAY_EXECSCRIPT, valueMap,
                nodeInfoList, false);        	
}

    /**
     *
     * Starts the database to the specified node
     *
     * @param valueMap
     *
     * @param nodeInfoList
     *
     * @throws java.io.IOException
     *
     */
    public void startDbOnNodes(HashMap<String, String> valueMap,
            NodeDBConnectionInfo[] nodeInfoList) throws IOException {
        executeTemplateOnNodes(Props.XDB_GATEWAY_STARTDB, valueMap,
                nodeInfoList);
    }

    /**
     *
     * Starts the database to the specified node
     *
     * @param valueMap
     *
     * @param nodeInfoList
     *
     * @throws java.io.IOException
     *
     */
    public void stopDbOnNodes(HashMap<String, String> valueMap,
            NodeDBConnectionInfo[] nodeInfoList) throws IOException {
        executeTemplateOnNodes(Props.XDB_GATEWAY_STOPDB, valueMap, nodeInfoList);
    }

    /**
     *
     *
     *
     * @param valueMap
     *
     * @param nodeInfoList
     *
     * @throws java.io.IOException
     *
     */
    public void dropDbOnNodes(HashMap<String, String> valueMap,
            NodeDBConnectionInfo[] nodeInfoList) throws IOException {
        try {
            stopDbOnNodes(valueMap, nodeInfoList);
        } catch (Exception e) {
            // ignore
        }
        executeTemplateOnNodes(Props.XDB_GATEWAY_DROPDB, valueMap, nodeInfoList);
    }

    /**
     *
     *
     *
     * @param valueMap
     *
     * @param nodeInfo
     *
     * @throws java.io.IOException
     *
     */
    public void dropDbOnNode(HashMap<String, String> valueMap,
            NodeDBConnectionInfo nodeInfo) throws IOException {
        NodeDBConnectionInfo[] nodeInfoList = new NodeDBConnectionInfo[1];
        nodeInfoList[0] = nodeInfo;
        dropDbOnNodes(valueMap, nodeInfoList);
    }

    /**
     *
     * Executes a script on a list of nodes
     *
     * @param valueMap
     *
     * @param nodeInfoList
     *
     * @throws java.io.IOException
     *
     */
    public void execScriptOnNodes(HashMap<String, String> valueMap,
            NodeDBConnectionInfo[] nodeInfoList) throws IOException {
        executeTemplateOnNodes(Props.XDB_GATEWAY_EXECSCRIPT, valueMap,
                nodeInfoList);
    }

    /**
     *
     * Executes the dbmcli command on the specified nodes
     *
     * @param commandStr
     *
     * @param valueMap
     *
     * @param nodeInfoList
     *
     * @throws java.io.IOException
     *
     */
    public void execOnNodes(String commandStr,
            HashMap<String, String> valueMap,
            NodeDBConnectionInfo[] nodeInfoList) throws IOException {
        executeTemplateOnNodes(commandStr, valueMap, nodeInfoList, true);
    }

    /**
     *
     *
     *
     * @param command
     *
     * @throws java.io.IOException
     *
     */
    private List<String> executeList(String command[]) throws IOException {
        Process[] processList = new Process[command.length];

        StreamGobbler out = null;
        StreamGobbler err = null;
        List<String> ret = new ArrayList();
        
        for (int i = 0; i < command.length; i++) {
            // Java Runtime spawn a thread to execute the command, so all
            // the commands in the list will be executed concurrently
            if (command[i] != null && command[i].length() > 0) {
                String cmd;
                if (Props.XDB_GATEWAY_PATH != null 
                        && Props.XDB_GATEWAY_PATH.length() > 0) {
                    cmd = new StringBuilder(Props.XDB_GATEWAY_PATH) 
                            .append(Props.XDB_GATEWAY_PATH_SEPARATOR)
                            .append(command[i]).toString();
                } else {
                    cmd = command[i];
                }
                Process proc = Runtime.getRuntime().exec(cmd);
                out = new StreamGobbler(proc.getInputStream(), null, null);
                err = new StreamGobbler(proc.getErrorStream(), null, null);
                // kick them off
                out.start();
                err.start();
                processList[i] = proc;
            } else {
                processList[i] = null;
            }
        }

        // now, wait for them to finish
        for (int i = 0; i < processList.length; i++) {
            try {
                if (processList[i] != null) {
                    int exitCode = processList[i].waitFor();
                    if (exitCode != 0 && force == false) {
                        throw new IOException("Failed to execute command \""
                                + command[i] + "\": exit code was " + exitCode);
                    }
                }
            } catch (InterruptedException ignore) {
            }
        }
        
        if (out != null && err != null) {
	        ret.add(out.getStreamValue());
	        ret.add(err.getStreamValue());
        }
        
        return ret;
    }

    /**
     *
     *
     *
     * @param nodeInfo
     *
     * @param valueMap
     *
     * @return
     *
     */

    public Map<String, String> populateValueMap(NodeDBConnectionInfo nodeInfo,
            Map<String, String> valueMap) {
        if (valueMap == null) {
            valueMap = new HashMap<String, String>();
        }

        valueMap.put(VAR_NODEHOST, nodeInfo.getDbHost());
        if (nodeInfo.getDbPort() > 0) {
            valueMap.put(VAR_NODEPORT, "" + nodeInfo.getDbPort());
        }
        valueMap.put(VAR_NODEDB, nodeInfo.getDbName());
        valueMap.put(VAR_DBUSERNAME, nodeInfo.getDbUser());
        valueMap.put(VAR_DBPASSWORD, nodeInfo.getDbPassword());
        for (Iterator it = nodeInfo.getProperties().entrySet().iterator(); it
                .hasNext();) {
            Map.Entry entry = (Map.Entry) it.next();
            valueMap.put((String) entry.getKey(), (String) entry.getValue());
        }

        valueMap.put(VAR_PSQLUTILNAME, Props.XDB_PSQL_UTIL_NAME);

        valueMap.put(VAR_PGCONFIGUTILNAME, Props.XDB_PGCONFIG_UTIL_NAME);

        return valueMap;
    }
}
