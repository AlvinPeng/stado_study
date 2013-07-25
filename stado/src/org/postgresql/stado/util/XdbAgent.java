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
 *  
 */
package org.postgresql.stado.util;

import java.io.File;
import java.util.ArrayList;
import java.util.Map;

import org.apache.log4j.PropertyConfigurator;
import org.postgresql.stado.common.util.Property;
import org.postgresql.stado.communication.NodeAgent;


/**
 * Simple launcher for remote nodes
 * 
 *  
 */
public class XdbAgent {
    private static final String USAGE = "Parameters: -n <node_numbers>\n"
            + "where <node_numbers> is space-separated list of Nodes to launch";

    private static void printUsage() {
        System.out.println(USAGE);
    }

    /**
     * Launch remote Nodes
     * 
     * @param args
     *            List of nodes to launch
     */
    public static void main(String[] args) {
        if (args.length == 0) {
            printUsage();
            System.exit(-1);
        }

        if (Property.get("log4j.configuration") != null
                && (new File(Property.get("log4j.configuration"))).exists()) {
            PropertyConfigurator.configure(Property.get("log4j.configuration"));
        } else if ((new File("log4j.properties")).exists()) {
            PropertyConfigurator.configure("log4j.properties");
        } else {
            PropertyConfigurator.configure(Property.getProperties());
        }

        Map commands = null;
        try {
            commands = ParseArgs.parse(args, "n");
        } catch (Exception e) {
            printUsage();
            System.exit(-1);
        }
        ArrayList nodes = (ArrayList) commands.get("-n");
        if (nodes == null || nodes.size() == 0) {
            printUsage();
            System.exit(-1);
        }

        try {
            for (int i = 0; i < nodes.size(); i++) {
                String nodeStr = (String) nodes.get(i);
                int nodeNum = Integer.parseInt(nodeStr);
                NodeAgent.getNodeAgent(nodeNum);
            }
        } catch (Exception e) {
            e.printStackTrace();
            printUsage();
            System.exit(-1);
        }
    }
}