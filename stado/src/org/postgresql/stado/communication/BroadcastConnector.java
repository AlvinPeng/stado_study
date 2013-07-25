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
package org.postgresql.stado.communication;

import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Level;
import org.postgresql.stado.common.util.Property;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLevel;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.communication.message.NodeMessage;
import org.postgresql.stado.engine.AckTracker;
import org.postgresql.stado.exception.XDBServerException;


/**
 * Broadcasts messages using Multicast UDP socket. Since UDP protocol does not
 * guarantee message delivery, the BroadcastConnector acknowleges reseived
 * packets and tracks acknowlegements from other connectors.
 * 
 *  
 */
public class BroadcastConnector extends AbstractConnector {
    private static final XLogger logger = XLogger
            .getLogger(BroadcastConnector.class);

    private static final XLogger chunkLog = XLogger.getLogger("Chunks");

    private static final InetSocketAddress[][] ACK_TARGET_TABLE;
    static {
        int j = 1;
        while (Property.get("xdb.multicastgroup.domain" + j + ".address") != null) {
            j++;
        }
        ACK_TARGET_TABLE = new InetSocketAddress[Props.XDB_NODECOUNT + 1][j];
        try {
            for (int i = 0; i < ACK_TARGET_TABLE.length; i++) {
                InetAddress host = InetAddress.getByName(Property.get("xdb."
                        + (i == 0 ? "coordinator" : "node." + i) + ".host"));
                for (j = 0; j < ACK_TARGET_TABLE[i].length; j++) {
                    String ackHostName = Property.get("xdb.broadcast.domain"
                            + j + ".ack.node" + i + ".address");
                    InetAddress ackHost = ackHostName == null ? host
                            : InetAddress.getByName(ackHostName);
                    int port = Property.getInt("xdb.broadcast.domain" + j
                            + ".ack.node" + i + ".port", -1);
                    if (port < 0) {
                        port = Property.getInt("xdb.multicastgroup.domain" + j
                                + ".port", 6987 + 2 * j) + 1;
                    }
                    ACK_TARGET_TABLE[i][j] = new InetSocketAddress(ackHost,
                            port);
                }
            }
        } catch (Throwable t) {
            logger.catching(t);
            System.exit(1);
        }
    }

    /**

     * 

     * @return 

     */

    static final int domainCount() {
        return ACK_TARGET_TABLE[0].length;
    }

    /**
     * ID of the node this connector belongs to
     */
    private int nodeID;

    /**
     * ID of the node this connector belongs to
     */
    private int domain;

    /**
     * Max size of UDP packet
     */
    private static final int UDP_PACKET_SIZE = Property.getInt(
            "xdb.broadcast.packetsize", 1024);

    /**
     * Size of internal acknowledgement UDP packet
     */
    private static final int ACK_UDP_PACKET_SIZE = 10;

    /**
     * Communication channel
     */
    private MulticastSocket channel = null;

    /**
     * Multicast group address
     */
    private InetAddress group = null;

    /**
     * Multicast group port
     */
    private int port = 0;

    /**
     * Acknowledgement channel
     */
    private DatagramSocket ackChannel = null;

    /**
     * Class to track acknowlegements
     */
    private AckTracker tracker;

    private LinkedHashSet<Integer> recentSenders;

    private LinkedBlockingQueue<byte[]> incomingPackets = new LinkedBlockingQueue<byte[]>();

    /*
     * STATISTICS
     */

    private long msgSent = 0;

    private long msgReceived = 0;

    private long msgFailed = 0;

    private long chunkCreated = 0;

    private long chunkSent = 0;

    private long chunkReceived = 0;

    private long sendTime = 0;

    private long lastDumped = System.currentTimeMillis();

    private void dumpStats() {
        if (lastDumped + 60000 > System.currentTimeMillis()) {
            return;
        }
        String msg = "Node: " + nodeID + " Domain: " + domain
                + " Messages (Sent, Received, Failed): " + msgSent + ", "
                + msgReceived + ", " + msgFailed
                + " Chunks (Created, Sent, Received): " + chunkCreated + ", "
                + chunkSent + ", " + chunkReceived + " Average send time, ms: "
                + sendTime / msgSent;
        logger.debug(msg);
        lastDumped = System.currentTimeMillis();
    }

    /**

     * The constructor

     * 

     * @param nodeID ID of the node this connector belongs to

     */
    public BroadcastConnector(int nodeID) {
        this(nodeID, 0);
    }

    /**

     * 

     * @param nodeID 

     * @param domain 

     */

    public BroadcastConnector(int nodeID, int domain) {
        final String method = "BroadcastConnector";
        logger.entering(method, new Object[] { new Integer(nodeID),
                new Integer(domain) });
        try {

            this.nodeID = nodeID;
            this.domain = domain;
            tracker = new AckTracker();

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * @throws UnknownHostException
     * @throws IOException
     */
    private void listen() throws UnknownHostException, IOException {
        String groupAddr = Property.get("xdb.multicastgroup.domain" + domain
                + ".address", "228.5.6.7");
        group = InetAddress.getByName(groupAddr);
        port = Property.getInt("xdb.multicastgroup.domain" + domain + ".port",
                6987 + (ACK_TARGET_TABLE.length + 1) * domain);
        ackChannel = new DatagramSocket(ACK_TARGET_TABLE[nodeID][domain]);
        channel = new MulticastSocket(port);
        channel.joinGroup(group);
    }

    /**
     * Starts connector's threads
     * 
     * @see org.postgresql.stado.communication.AbstractConnector#start()
     */
    @Override
    public void start() {
        // Check is it already initialized
        if (channel != null) {
            return;
        }
        recentSenders = new LinkedHashSet<Integer>();
        try {
            listen();
        } catch (IOException e) {
            logger.catching(e);
            System.exit(1);
        }
        /*
         * //boost priority of receiving thread to decrease chance to loose
         * packet Thread t = new Thread(new ReceiveChunksThread());
         * t.setPriority(Thread.MAX_PRIORITY); t.start(); t = new Thread(new
         * ReceiveAcksThread()); t.setPriority(Thread.MAX_PRIORITY); t.start();
         */
        // Those should not be interrupted
        addWorkerThread(new ReceiveChunksThread(channel));
        addWorkerThread(new ReceiveAcksThread(ackChannel));

        addWorkerThread(new ReceivingThread());
        addWorkerThread(new SendingThread());
        addWorkerThread(this);
    }

    /**
     * Closes socket and stops threads
     * 
     * @see org.postgresql.stado.communication.AbstractConnector#destroy()
     */
    @Override
    public void destroy() {
        super.destroy();
        channel.close();
        ackChannel.close();
    }

    /**
     * Generates unique message chunk key from senderID and chunkID
     * 
     * @param senderID
     * @param chunkID
     * @return key
     */
    private Object getKey(int senderID, int chunkID) {
        long lKey = (((long) senderID) << 32) + chunkID;
        return new Long(lKey);
    }

    /**

     * 

     * @param i 

     * @param dest 

     * @param offset 

     */

    private void packInt(int i, byte[] dest, int offset) {
        dest[offset] = (byte) ((i >> 24) & 0xFF);
        dest[offset + 1] = (byte) ((i >> 16) & 0xFF);
        dest[offset + 2] = (byte) ((i >> 8) & 0xFF);
        dest[offset + 3] = (byte) (i & 0xFF);
    }

    /**

     * 

     * @param buffer 

     * @param offset 

     * @return 

     */

    private int unpackInt(byte[] buffer, int offset) {
        return ((buffer[offset] & 0xFF) << 24)
                | ((buffer[offset + 1] & 0xFF) << 16)
                | ((buffer[offset + 2] & 0xFF) << 8)
                | (buffer[offset + 3] & 0xFF);
    }

    private static final int MAX_SENDERS_HISTORY = 1024;

    protected class ReceivingThread extends AbstractReceivingThread {
        private Map<Integer, byte[][]> incomingChunks = new HashMap<Integer, byte[][]>();

        /**

         * 

         * @throws java.lang.Exception 

         * @return 

         */

        @Override
        protected NodeMessage[] receive() throws Exception {
            byte[] buffer = incomingPackets.take();
            chunkReceived++;
            int senderID = unpackInt(buffer, 0);
            int chunkID = unpackInt(buffer, 4);
            chunkLog.log(XLevel.TRACE,
                    "Node %0% Domain %1% Chunk received: %2%", new Object[] {
                            new Integer(nodeID), new Integer(domain),
                            getKey(senderID, chunkID) });
            logger.log(XLevel.TRACE, "NodeID: %0%, MsgID: %1%, ChunkID: %2%",
                    new Object[] { new Integer(senderID >> 16),
                            new Integer(senderID & 0xFFFF),
                            new Integer(chunkID) });
            logger
                    .log(
                            XLevel.TRACE,
                            "Sending acknowlegement from %0% to %1%, domain %2% using %3%",
                            new Object[] { new Integer(nodeID),
                                    new Integer(senderID >> 16),
                                    new Integer(domain),
                                    ACK_TARGET_TABLE[senderID >> 16][domain] });
            byte[] ackb = new byte[10];
            System.arraycopy(buffer, 0, ackb, 0, 8);
            ackb[8] = (byte) (nodeID >> 8);
            ackb[9] = (byte) nodeID;
            DatagramPacket ackp = new DatagramPacket(ackb, 10,
                    ACK_TARGET_TABLE[senderID >> 16][domain]);
            ackChannel.send(ackp);
            // Send acknowlegement anyway
            if (recentSenders.contains(new Integer(senderID))) {
                logger.trace("We already have this message, skipping");
                return null;
            }
            byte[][] chunks = incomingChunks.get(senderID);
            if (chunks == null) {
                int total = unpackInt(buffer, 8);
                chunks = new byte[total][];
                incomingChunks.put(new Integer(senderID), chunks);
            }
            if (chunks[chunkID] == null) {
                chunks[chunkID] = new byte[buffer.length - 12];
                System.arraycopy(buffer, 12, chunks[chunkID], 0,
                        chunks[chunkID].length);
                boolean completed = true;
                for (int i = chunks.length - 1; i >= 0; i--) {
                    if (chunks[i] == null) {
                        completed = false;
                        break;
                    }
                }
                if (completed) {
                    logger.trace("Complete message here");
                    byte[] result = new byte[(UDP_PACKET_SIZE - 12)
                            * (chunks.length - 1)
                            + chunks[chunks.length - 1].length];
                    for (int i = 0; i < chunks.length; i++) {
                        System.arraycopy(chunks[i], 0, result,
                                (UDP_PACKET_SIZE - 12) * i, chunks[i].length);
                    }
                    while (recentSenders.size() >= MAX_SENDERS_HISTORY) {
                        // Remove oldest entry
                        Iterator it = recentSenders.iterator();
                        it.next();
                        it.remove();
                    }
                    recentSenders.add(senderID);
                    incomingChunks.remove(senderID);
                    NodeMessage msg = NodeMessage.decodeBytes(result);
                    if (msg.getNodeList().contains(new Integer(nodeID))) {
                        msgReceived++;
                        return new NodeMessage[] { msg };
                    }
                }
            }
            return null;
        }
    }

    protected abstract class ReceivePacketsThread implements Runnable {
        protected DatagramSocket theChannel;

        /**

         * 

         * @param theChannel 

         */

        public ReceivePacketsThread(DatagramSocket theChannel) {
            this.theChannel = theChannel;
        }

        public void run() {
            while (true) {
                try {
                    receive();
                } catch (IOException t) {
                    if (theChannel.isClosed()) {
                        break;
                    }
                    logger.catching(t);
                } catch (Throwable t) {
                    logger.catching(t);
                }
            }
        }

        /**

         * 

         * @throws java.lang.Exception 

         */

        protected abstract void receive() throws Exception;
    }

    protected class ReceiveAcksThread extends ReceivePacketsThread {
        /**

         * 

         * @param theChannel 

         */

        public ReceiveAcksThread(DatagramSocket theChannel) {
            super(theChannel);
        }

        /*
         * (non-Javadoc)
         * 
         * @see org.postgresql.stado.communication.BroadcastConnector.ReceiveThread#receive()
         */
        /**

         * 

         * @throws java.lang.Exception 

         */

        @Override
        protected void receive() throws Exception {
            DatagramPacket p = new DatagramPacket(
                    new byte[ACK_UDP_PACKET_SIZE], ACK_UDP_PACKET_SIZE);
            theChannel.receive(p);
            logger.trace("Received acknowlegement");
            byte[] buffer = p.getData();
            int senderID = unpackInt(buffer, 0);
            int chunkID = unpackInt(buffer, 4);
            logger.log(XLevel.TRACE, "NodeID: %0%, MsgID: %1%, ChunkID: %2%",
                    new Object[] { new Integer(senderID >> 16),
                            new Integer(senderID & 0xFFFF),
                            new Integer(chunkID) });
            int remoteID = ((buffer[8] & 0xFF) << 8) | (buffer[9] & 0xFF);
            chunkLog.log(XLevel.TRACE,
                    "Node %0% Domain %1% Chunk %2% acked from node %3%",
                    new Object[] { new Integer(nodeID), new Integer(domain),
                            getKey(senderID, chunkID), new Integer(remoteID) });
            tracker.register(getKey(senderID, chunkID), remoteID);
        }
    }

    protected class ReceiveChunksThread extends ReceivePacketsThread {
        /**

         * 

         * @param theChannel 

         */

        public ReceiveChunksThread(DatagramSocket theChannel) {
            super(theChannel);
        }

        /*
         * (non-Javadoc)
         * 
         * @see org.postgresql.stado.communication.BroadcastConnector.ReceiveThread#receive()
         */
        /**

         * 

         * @throws java.lang.Exception 

         */

        @Override
        protected void receive() throws Exception {
            DatagramPacket p = new DatagramPacket(new byte[UDP_PACKET_SIZE],
                    UDP_PACKET_SIZE);
            theChannel.receive(p);
            logger.trace("Received chunk");
            incomingPackets.offer(p.getData());
        }
    }

    protected class SendingThread extends AbstractSendingThread {
        private int senderID = nodeID << 16;

        /**

         * 

         * @param message 

         * @throws java.lang.Exception 

         */

        @Override
        protected void send(NodeMessage message) throws Exception {
            final String method = "send";
            logger.entering(method, new Object[] { message });
            long startSend = System.currentTimeMillis();
            try {

                byte[] msgBytes = NodeMessage.getBytes(message);
                Map<Object, byte[]> chunks = new HashMap<Object, byte[]>();
                int chunkCount = (msgBytes.length - 1) / (UDP_PACKET_SIZE - 12)
                        + 1;
                chunkCreated += chunkCount;
                for (int i = 0; i < chunkCount - 1; i++) {
                    byte[] chunk = new byte[UDP_PACKET_SIZE];
                    packInt(senderID, chunk, 0);
                    packInt(i, chunk, 4);
                    packInt(chunkCount, chunk, 8);
                    System.arraycopy(msgBytes, i * (UDP_PACKET_SIZE - 12),
                            chunk, 12, UDP_PACKET_SIZE - 12);
                    chunks.put(getKey(senderID, i), chunk);
                }
                byte[] chunk = new byte[(msgBytes.length - 1)
                        % (UDP_PACKET_SIZE - 12) + 13];
                packInt(senderID, chunk, 0);
                packInt(chunkCount - 1, chunk, 4);
                packInt(chunkCount, chunk, 8);
                System.arraycopy(msgBytes, (chunkCount - 1)
                        * (UDP_PACKET_SIZE - 12), chunk, 12, chunk.length - 12);
                chunks.put(getKey(senderID, chunkCount - 1), chunk);
                tracker.init(chunks, message.getNodeList());
                chunkLog.log(XLevel.TRACE,
                        "Node %0% Domain %1% Chunks created: %2%",
                        new Object[] { new Integer(nodeID),
                                new Integer(domain), chunks });
                logger.trace("Sending message");
                Collection packets = tracker.getPackets();
                logger.log(XLevel.TRACE, "Packets to send: %0%",
                        new Object[] { new Integer(packets.size()) });
                while (!packets.isEmpty()) {
                    chunkLog.log(Level.DEBUG,
                            "Node %0% Domain %1% Chunks sending: %2%",
                            new Object[] { new Integer(nodeID),
                                    new Integer(domain), packets });
                    for (Iterator it = packets.iterator(); it.hasNext();) {
                        byte[] packet = (byte[]) it.next();
                        DatagramPacket p = new DatagramPacket(packet,
                                packet.length, group, port);
                        logger.trace("Sending next packet, size: "
                                + packet.length);
                        channel.send(p);
                    }
                    chunkSent += packets.size();
                    try {
                        tracker.waitAcks(1000);
                    } catch (XDBServerException ex) {
                        logger.catching(ex);
                        tracker.reset();
                        logger.throwing(ex);
                        throw ex;
                    }
                    packets = tracker.getPackets();
                    logger.log(XLevel.TRACE, "Packets to send: %0%",
                            new Object[] { new Integer(packets.size()) });
                }
                msgSent++;
                logger.trace("Sending done");
                dumpStats();

            } finally {
                if (++senderID >> 16 != nodeID) {
                    senderID = nodeID << 16;
                }
                sendTime += System.currentTimeMillis() - startSend;
                logger.exiting(method);
            }
        }

        /**

         * 

         * @param message 

         * @param ex 

         */

        @Override
        protected void sendFailed(NodeMessage message, Exception ex) {
            msgFailed++;
            super.sendFailed(message, ex);
        }
    }
}
