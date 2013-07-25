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
package org.postgresql.stado.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import org.postgresql.stado.common.util.Property;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.communication.ConnectorFactory;
import org.postgresql.stado.exception.XDBServerException;


/**
 * SysAgent represents a group of nodes launched by XdbServer or XdbAgent and
 * Running in the same VM. Normally different agents run on different servers
 * (physical nodes) but they may share the same server. The class is initialized
 * based on information in the configuration file and used to verify
 * configuration and track runtime state of remote system components (connected
 * or disconnected)
 * 
 * @author Andrei Martsinchyk
 */
public class SysAgent {
    /**
     * Array to store agents. Index of the array is a node number, 0 means
     * coordinator. If nodes belong to the same agent, respective items of the
     * array point to the same instance, so condition (agents[n] == agents[m])
     * shows if nodes n and m are under the same agent
     */
    private static SysAgent[] agents = null;

    /**
     * Initialize the @link #agents array and verify configuration of the nodes
     */
    private static void init() {
        agents = new SysAgent[Props.XDB_NODECOUNT + 1];
        // Loop over all nodes, including coordinator
        for (int src = 0; src <= Props.XDB_NODECOUNT; src++) {
            if (agents[src] == null) {
                // Create an agent and determine nodes local to it
                agents[src] = new SysAgent(src == 0, Property.get("xdb."
                        + ((src == 0) ? "coordinator" : "node." + src)
                        + ".host", "localhost"));
                if (src > 0) {
                    agents[src].addNode(MetaData.getMetaData().getNode(src));
                }
                for (int dst = 0; dst <= Props.XDB_NODECOUNT; dst++) {
                    if (src == dst) {
                        continue;
                    }
                    if (ConnectorFactory.getConnectorType(src, dst) == ConnectorFactory.CONNECTOR_TYPE_LOCAL) {
                        if (agents[dst] == null) {
                            agents[dst] = agents[src];
                            agents[dst].addNode(MetaData.getMetaData().getNode(
                                    dst));
                        } else {
                            throw new XDBServerException(
                                    "Inconsistent configuration is detected: "
                                            + "agent launching node "
                                            + src
                                            + " launches also node "
                                            + (dst == 0 ? "coordinator"
                                                    : "node " + dst)
                                            + " which is launched by a different agent");
                        }
                    }
                }
            } else {
                for (int dst = 0; dst <= Props.XDB_NODECOUNT; dst++) {
                    if (src == dst) {
                        continue;
                    }
                    if (ConnectorFactory.getConnectorType(src, dst) == ConnectorFactory.CONNECTOR_TYPE_LOCAL) {
                        if (agents[dst] != agents[src]) {
                            throw new XDBServerException(
                                    "Inconsistent configuration is detected: "
                                            + "node "
                                            + src
                                            + " is using local connector to "
                                            + (dst == 0 ? "coordinator"
                                                    : "node " + dst)
                                            + " but launched by a different agent");
                        }
                    }
                    // else warn if src and dst under the same agent
                }
            }
        }
    }

    public static final SysAgent getAgent(int nodenum) {
        if (agents == null) {
            init();
        }
        return agents[nodenum];
    }

    public static final Collection<SysAgent> getNodeAgents() {
        if (agents == null) {
            init();
        }
        HashSet<SysAgent> result = new HashSet<SysAgent>();
        // Skip first (Coordinator Agent)
        for (int i = 1; i < agents.length; i++) {
            result.add(agents[i]);
        }
        return result;
    }

    private boolean coordinator;

    private String host;

    private boolean connected = false;

    /**
     * List of nodes running under this agent
     */
    private Collection<Node> nodes = null;

    /**
     * The constructor
     */
    private SysAgent(boolean coordinator, String host) {
        this.coordinator = coordinator;
        this.host = host;
        nodes = new ArrayList<Node>();
    }

    private void addNode(Node node) {
        nodes.add(node);
    }

    public boolean isCoordinator() {
        return coordinator;
    }

    public String getHost() {
        return host;
    }

    public Collection<Node> getNodes() {
        return nodes;
    }

    public Collection<DBNode> setConnected(boolean connected) {
        this.connected = connected;
        Collection<DBNode> result = new HashSet<DBNode>();
        for (Node node : nodes) {
            if (connected && !node.isUp()) {
                result.addAll(node.setUp());
            } else if (!connected && node.isUp()) {
                node.setDown();
            }
        }
        return result;
    }

    public boolean isConnected() {
        return connected;
    }
}