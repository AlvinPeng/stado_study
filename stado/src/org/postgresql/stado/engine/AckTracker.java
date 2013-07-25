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
package org.postgresql.stado.engine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Level;
import org.postgresql.stado.common.util.Property;
import org.postgresql.stado.common.util.XLevel;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.exception.XDBServerException;


public class AckTracker {
    private static final XLogger logger = XLogger.getLogger(AckTracker.class);

    private static final int MAX_ATTEMPTS = Property.getInt(
            "xdb.broadcast.retries", 5);

    /**
     * List of packets to be sent
     */
    private Map packets;

    /**
     * Acknowlegements are received so far
     */
    private Map acknowlegements;

    private Collection nodeList = null;

    private boolean wasNewAck = false;

    private int attempt = 0;

    /**
     * Initializes tracker
     * 
     * @param nodeList 
     * @param packets to be sent
     * @throws XDBServerException when packets are already tracked
     */
    public synchronized void init(Map packets, Collection nodeList)
            throws XDBServerException {
        final String method = "init";
        logger.entering(method, new Object[] { packets, nodeList });
        try {

            if (this.packets != null && !this.packets.isEmpty()) {
                XDBServerException ex = new XDBServerException(
                        "Tracker not empty");
                logger.throwing(ex);
                throw ex;
            }
            this.packets = packets;
            this.nodeList = nodeList;
            acknowlegements = new HashMap();
            if (nodeList != null && !nodeList.isEmpty()) {
                for (Iterator it = packets.keySet().iterator(); it.hasNext();) {
                    acknowlegements.put(it.next(), new HashSet(nodeList));
                }
            }

        } finally {
            logger.exiting(method);
        }
    }

    public synchronized void reset() {
        final String method = "reset";
        logger.entering(method);
        try {

            packets.clear();
            acknowlegements.clear();
            notifyAll();

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * 
     * @param key 
     * @param packet 
     */
    public synchronized void addPacket(Object key, Object packet) {
        addPacket(key, packet, null);
    }

    /**
     * 
     * @param key 
     * @param packet 
     * @param nodeList 
     */
    public synchronized void addPacket(Object key, Object packet,
            Collection nodeList) {
        final String method = "addPacket";
        logger.entering(method, new Object[] { key, packet });
        try {

            if (nodeList != null && !nodeList.isEmpty()) {
                packets.put(key, packet);
                acknowlegements.put(key, new HashSet(nodeList));
            } else if (this.nodeList != null && !this.nodeList.isEmpty()) {
                packets.put(key, packet);
                acknowlegements.put(key, new HashSet(this.nodeList));
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Returns not acknowleged messages to re-send
     * 
     * @return Collecton of packets
     */
    public synchronized Collection getPackets() {
        final String method = "getPackets";
        logger.entering(method);
        Collection out = null;
        try {

            out = (packets == null ? new ArrayList() : new ArrayList(packets
                    .values()));
            return out;

        } finally {
            logger.exiting(method, out);
        }
    }

    /**
     * 
     * @return 
     */
    public synchronized int getUnackedKeyCount() {
        final String method = "getUnackedKeyCount";
        logger.entering(method);
        try {

            return acknowlegements.size();

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Registers received acknowlegement
     * 
     * @param key
     *            Key of the packet
     * @param nodeID
     *            ID of the node message was received from
     */
    public synchronized void register(Object key, int nodeID) {
        final String method = "register";
        logger.entering(method, new Object[] { key, new Integer(nodeID) });
        try {

            if (acknowlegements != null) {
                HashSet nodes = (HashSet) acknowlegements.get(key);
                if (nodes != null) {
                    if (nodes.remove(new Integer(nodeID))) {
                        wasNewAck = true;
                    }
                    if (nodes.isEmpty()) {
                        logger.log(Level.DEBUG, "Removing the key %0%",
                                new Object[] { key });
                        acknowlegements.remove(key);
                        packets.remove(key);
                        notifyAll();
                    }
                }
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Pause to wait for acknowlegements
     * 
     * @param timeout
     */
    public synchronized void waitAcks(long timeout) {
        final String method = "waitAcks";
        logger.entering(method, new Object[] { new Long(timeout) });
        try {

            wasNewAck = false;
            long start = System.currentTimeMillis();
            while (acknowlegements != null && !acknowlegements.isEmpty()) {
                try {
                    logger
                            .log(Level.DEBUG,
                                    "Start waiting for %0% acknowledgements",
                                    new Object[] { new Integer(acknowlegements
                                            .size()) });
                    wait(timeout);
                    logger
                            .log(Level.DEBUG,
                                    "Stop waiting for %0% acknowledgements",
                                    new Object[] { new Integer(acknowlegements
                                            .size()) });
                } catch (InterruptedException e) {
                }
                if (start + timeout < System.currentTimeMillis()) {
                    if (wasNewAck) {
                        attempt = 0;
                        return;
                    } else {
                        if (++attempt >= MAX_ATTEMPTS) {
                            attempt = 0;
                            logger.debug(acknowlegements);
                            throw new XDBServerException(
                                    "Any acknowlegement has not been received while waiting");
                        } else {
                            logger
                                    .debug("Any acknowlegement has not been received while waiting, attempt "
                                            + attempt);
                            return;
                        }
                    }
                }
            }
            // All required acknowledgements have been received
            // Drop packets that do not require acknowledgement
            packets = null;

        } finally {
            logger.exiting(method);
        }
    }
}
