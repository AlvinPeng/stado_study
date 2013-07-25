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
 * NodeUsage.java
 *
 * This is used to show how a node participates in a given step-
 * if it is a producer, consumer, or both. 
 *
 * We need this because the current join table may exist on one set of
 * nodes, and the destination target for the results of these may be
 * on a different set of nodes.
 *
 */

package org.postgresql.stado.planner;

/**
 * 
 */
public class NodeUsage {

    public int nodeId;

    public boolean isProducer;

    public boolean isConsumer;

    /**
     * Creates a new instance of NodeUsage
     *
     * @param nodeId 
     * @param isProducer 
     * @param isConsumer 
     */
    public NodeUsage(int nodeId, boolean isProducer, boolean isConsumer) {
        this.nodeId = nodeId;
        this.isProducer = isProducer;
        this.isConsumer = isConsumer;
    }

    // BUILD_CUT_START
    /**
     * 
     * @return 
     */
    @Override
    public String toString() {
        StringBuffer sbUsage = new StringBuffer();

        sbUsage.append("  nodeId = ")
            .append(nodeId)
            .append('\n')
            .append("  isProducer = ")
            .append(isProducer)
            .append('\n')
            .append("  isConsumer = ")
            .append(isConsumer)
            .append('\n');
               
        return sbUsage.toString();
    }
    // BUILD_CUT_ALT
    // public String toString() { return null; }
    // BUILD_CUT_END
}
