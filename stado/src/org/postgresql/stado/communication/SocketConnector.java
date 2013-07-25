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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import org.apache.log4j.Level;
import org.postgresql.stado.common.util.Property;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.communication.message.NodeMessage;
import org.postgresql.stado.exception.XDBServerException;


/**
 *   It is assumed there is only one Socket Connector
 *         at the Node. It creates one ServerSocket to listen for incoming
 *         connections and one or more client sockets to send messages.
 */
public class SocketConnector extends AbstractConnector {

    private static final XLogger logger = XLogger
            .getLogger(SocketConnector.class);

    /**
     * Owning node ID
     */
    private int nodeID;

    /**
     * Port to listen to
     */
    private int port;

    /**
     * Active connections. Subject for pooling
     */
    private HashMap<Integer, SocketChannel> channels = new HashMap<Integer, SocketChannel>();

    private HashSet<SocketChannel> newChannels = new HashSet<SocketChannel>();

    private HashSet<SocketChannel> toRegister = new HashSet<SocketChannel>();

    /**
     * Tracks connections that are ready to proceed
     */
    private Selector selector = null;

    /**
     * Listening socket channel. Accepts incoming connections
     */
    private ServerSocketChannel server = null;

    private Selector serverSelector = null;

    /**
     *
     * @param nodeID
     */
    public SocketConnector(int nodeID) {
        final String method = "SocketConnector";
        logger.entering(method, new Object[] { new Integer(nodeID) });
        try {

            this.nodeID = nodeID;

        } finally {
            logger.exiting(method);
        }

    }

    /**
     * @throws IOException
     * @throws ClosedChannelException
     */
    private void listen() throws IOException, ClosedChannelException {
        String key = (nodeID == 0 ? "xdb.coordinator.port" : "xdb.node."
                + nodeID + ".port");
        port = Property.getInt(key, -1);
        if (port != -1) {
            serverSelector = Selector.open();
            server = ServerSocketChannel.open();
            server.configureBlocking(false);
            server.socket().bind(new InetSocketAddress(port));
            server.register(serverSelector, SelectionKey.OP_ACCEPT);
            // Do not pass it to addWorkerThread since it should not be
            // interrapted
            addWorkerThread(new AcceptingThread());
        }
    }

    /**
     * Starts Connector's threads
     *
     * @see org.postgresql.stado.communication.AbstractConnector#start()
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    @Override
    public void start() throws XDBServerException {
        if (server == null) {
            try {
                listen();
            } catch (IOException e) {
                logger.catching(e);
                XDBServerException ex = new XDBServerException(
                        "Failed to initialize listening, can not start", e);
                logger.throwing(ex);
                throw ex;
            }
            try {
                selector = Selector.open();
            } catch (IOException e) {
                logger.catching(e);
                XDBServerException ex = new XDBServerException(
                        "Failed to open selector, can not start", e);
                logger.throwing(ex);
                throw ex;
            }
            addWorkerThread(new SendingThread());
            addWorkerThread(new ReceivingThread());
            addWorkerThread(this);
        }
    }

    /**
     * Stops connector threads and closes all channels
     */
    @Override
    public void destroy() {
        final String method = "destroy";
        logger.entering(method);
        try {

            super.destroy();
            try {
                selector.close();
                serverSelector.close();
                server.close();
                synchronized (channels) {
                    for (SocketChannel socketChannel : newChannels) {
                  socketChannel.close();
               }
                    newChannels.clear();
                    for (SocketChannel socketChannel : channels.values()) {
                  socketChannel.close();
               }
                    channels.clear();
                }
            } catch (IOException e) {
                logger.catching(e);
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * @param ch
     */
    private void registerChannel(SocketChannel ch) {
        final String method = "registerChannel";
        logger.entering(method, new Object[] { ch });
        try {

            synchronized (channels) {
                logger.log(Level.DEBUG, "newChannels: %0%",
                        new Object[] { newChannels });
                newChannels.add(ch);
                toRegister.add(ch);
                logger.log(Level.DEBUG, "newChannels: %0%",
                        new Object[] { newChannels });
            }

            selector.wakeup();

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Register newly opened channel with selector
     *
     * @param targetID
     * @param ch
     */
    private void registerChannel(Integer targetID, SocketChannel ch) {
        final String method = "registerChannel";
        logger.entering(method, new Object[] { targetID, ch });
        try {

            synchronized (channels) {
                logger.log(Level.DEBUG, "channels: %0%",
                        new Object[] { channels });
                SocketChannel old = channels.put(targetID, ch);
                try {
                    if (old != null) {
                        old.close();
                    }
                } catch (IOException cce) {
                    logger.catching(cce);
                }
                toRegister.add(ch);
                logger.log(Level.DEBUG, "channels: %0%",
                        new Object[] { channels });
            }

            selector.wakeup();

        } finally {
            logger.exiting(method);
        }
    }

    private void updateSelector() {
        final String method = "updateSelector";
        logger.entering(method);
        try {

            synchronized (channels) {
                for (SocketChannel ch : toRegister) {
               try {
                ch.register(selector, SelectionKey.OP_READ);
               } catch (ClosedChannelException cce) {
                logger.catching(cce);
                closeChannel(ch);
               }
            }
                toRegister.clear();
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Close channel and remove it from the selector
     *
     * @param ch
     */
    private void closeChannel(SocketChannel ch) {
        final String method = "closeChannel";
        logger.entering(method, new Object[] { ch });
        try {

            synchronized (channels) {
                logger.log(Level.DEBUG, "newChannels: %0%",
                        new Object[] { newChannels });
                if (!newChannels.remove(ch)) {
                    logger.log(Level.DEBUG, "channels: %0%",
                            new Object[] { channels });
                    for (Iterator<Map.Entry<Integer, SocketChannel>> it = channels
                            .entrySet().iterator(); it.hasNext();) {
                        if (ch.equals(it.next().getValue())) {
                            it.remove();
                            break;
                        }
                    }
                    logger.log(Level.DEBUG, "channels: %0%",
                            new Object[] { channels });
                }
                logger.log(Level.DEBUG, "newChannels: %0%",
                        new Object[] { newChannels });
            }
            try {
                ch.close();
            } catch (IOException ioe) {
                logger.catching(ioe);
            }

        } finally {
            logger.exiting(method);
        }
    }

    protected class ReceivingThread extends AbstractReceivingThread {
        private boolean hasData = false;

        /**
         *
         * @throws java.lang.Exception
         * @return
         */
        @Override
        protected NodeMessage[] receive() throws Exception {
            LinkedList<NodeMessage> out = new LinkedList<NodeMessage>();
            int selCount = selector.select();
            updateSelector();
            if (selCount > 0) {
                Iterator<SelectionKey> keysIter = selector.selectedKeys().iterator();
                while (keysIter.hasNext()) {

                    SelectionKey key = keysIter.next();
                    keysIter.remove();
                    if (key.isReadable()) {
                        hasData = false;
                        try {
                            NodeMessage msg = read(key);
                            logger
                                    .log(
                                            Level.DEBUG,
                                            "Read data from channel %0%, got message %1%",
                                            new Object[] { key.channel(), msg });
                            if (msg != null) {
                                synchronized (channels) {
                                    logger.log(Level.DEBUG, "newChannels: %0%",
                                            new Object[] { newChannels });
                                    SocketChannel ch = (SocketChannel) key
                                            .channel();
                                    if (newChannels.remove(ch)) {
                                        logger.log(Level.DEBUG,
                                                "channels: %0%",
                                                new Object[] { channels });
                                        SocketChannel old = channels
                                                .put(new Integer(msg
                                                        .getSourceNodeID()), ch);
                                        try {
                                            if (old != null) {
                                                old.close();
                                            }
                                        } catch (IOException e) {
                                            logger.catching(e);
                                        }
                                        logger.log(Level.DEBUG,
                                                "channels: %0%",
                                                new Object[] { channels });
                                    }
                                    logger.log(Level.DEBUG, "newChannels: %0%",
                                            new Object[] { newChannels });
                                }
                            }
                            while (msg != null) {
                                out.add(msg);
                                msg = read(key);
                            }
                        } finally {
                            if (!hasData) {
                                closeChannel((SocketChannel) key.channel());
                            }
                        }
                    }
                }
            }
            return out.toArray(new NodeMessage[out.size()]);
        }

        /**
         *
         * @param key
         * @return
         */
        private NodeMessage read(SelectionKey key) {
            final String method = "read";
            logger.entering(method);
            try {

                int size;
                SocketChannel ch = (SocketChannel) key.channel();
                try {
                    ByteBuffer buffer = (ByteBuffer) key.attachment();
                    if (buffer == null) {
                        // Read new message
                        // Read message size
                        buffer = ByteBuffer.allocate(4);
                    }
                    if (buffer.capacity() == 4) {
                        if (ch.read(buffer) > 0) {
                            hasData = true;
                        }
                        if (buffer.position() == 4) {
                            // We can now restore message size
                            byte[] data = buffer.array();
                            size = ((data[0] & 0xFF) << 24)
                                    | ((data[1] & 0xFF) << 16)
                                    | ((data[2] & 0xFF) << 8)
                                    | (data[3] & 0xFF);
                            // and prepare new message buffer
                            byte[] newBuffer = new byte[size + 4];
                            System.arraycopy(data, 0, newBuffer, 0, 4);
                            buffer = ByteBuffer.wrap(newBuffer, 4, size);
                        } else {
                            // Do not have all data available, continue later
                            key.attach(buffer);
                            return null;
                        }
                    }
                    // Now read message body
                    if (ch.read(buffer) > 0) {
                        hasData = true;
                    }
                    if (buffer.position() == buffer.limit()) {
                        // Got complete message, decode
                        key.attach(null);
                        return NodeMessage.decodeBytes(buffer.array(), 4,
                                buffer.position() - 4);
                    } else {
                        // Do not have all data available, continue later
                        key.attach(buffer);
                        return null;
                    }
                } catch (IOException ioe) {
                    closeChannel(ch);
                    return null;
                } catch (Throwable t) {
                    logger.catching(t);
                    closeChannel(ch);
                    return null;
                }

            } finally {
                logger.exiting(method);
            }
        }

    }

    protected class SendingThread extends AbstractSendingThread {
        /**
         *
         * @param message
         * @throws java.lang.Exception
         */
        @Override
        protected void send(NodeMessage message) throws Exception {
            final String method = "send";
            logger.entering(method, new Object[] { message });
            try {

                Integer targetID = message.getTargetNodeID();
                SocketChannel ch = null;
                boolean connected = true;
                try {
                    synchronized (channels) {
                        ch = channels.get(targetID);
                    }
                    if (ch == null) {
                        int targetNode = targetID.intValue();
                        String key = (targetNode == 0 ? "xdb.coordinator"
                                : "xdb.node." + targetNode);
                        String host = Property.get(key + ".host");
                        int port = Property.getInt(key + ".port", -1);
                        InetAddress addr = InetAddress.getByName(host);
                        ch = SocketChannel.open();
                        ch.configureBlocking(false);
                        connected = ch
                                .connect(new InetSocketAddress(addr, port));
                        registerChannel(targetID, ch);
                    } else {
                        logger.log(Level.DEBUG,
                                "Found existing channel to send: %0%",
                                new Object[] { ch });
                    }
                    while (!connected) {
                        connected = ch.finishConnect();
                        Thread.sleep(1);
                    }
                    write(ch, message);
                } catch (Exception e) {
                    if (ch != null) {
                        closeChannel(ch);
                    }
                    throw e;
                }

            } finally {
                logger.exiting(method);
            }
        }

        /**
         *
         * @param ch
         * @param msg
         * @throws java.lang.Exception
         */
        private void write(SocketChannel ch, NodeMessage msg) throws Exception {
            final String method = "write";
            logger.entering(method, new Object[] { ch, msg });
            try {

                logger.debug("Writing " + msg + " to " + ch);
                byte[] msgBytes = NodeMessage.getBytes(msg);
                byte[] buf = new byte[4 + msgBytes.length];
                buf[0] = (byte) ((msgBytes.length >> 24) & 0xFF);
                buf[1] = (byte) ((msgBytes.length >> 16) & 0xFF);
                buf[2] = (byte) ((msgBytes.length >> 8) & 0xFF);
                buf[3] = (byte) (msgBytes.length & 0xFF);
                System.arraycopy(msgBytes, 0, buf, 4, msgBytes.length);
                ByteBuffer bb = ByteBuffer.wrap(buf);
                ch.write(bb);
                while (bb.hasRemaining()) {
                    ch.write(bb);
                }

            } finally {
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
            final String method = "sendFailed";
            logger.entering(method, new Object[] { message, ex });
            try {

                if (message.getMessageType() == NodeMessage.MSG_NODE_UP) {
                    while (true) {
                        try {
                            Thread.sleep(1000);
                            send(message);
                            break;
                        } catch (InterruptedException e) {
                            break;
                        } catch (Exception ignore) {
                            // Coordinator has not been started yet, continue
                        }
                    }
                } else {
                    super.sendFailed(message, ex);
                }

            } finally {
                logger.exiting(method);
            }
        }
    }

    protected class AcceptingThread implements Runnable {
        public void run() {
            final String method = "run";
            logger.entering(method);
            try {

                while (true) {
                    try {
                        if (serverSelector.select() > 0) {
                            Iterator<SelectionKey> keysIter = serverSelector
                                    .selectedKeys().iterator();
                            while (keysIter.hasNext()) {
                                SelectionKey key = keysIter.next();
                                keysIter.remove();
                                if (key.isAcceptable()) {
                                    ServerSocketChannel sch = (ServerSocketChannel) key
                                            .channel();
                                    SocketChannel ch = sch.accept();
                                    ch.configureBlocking(false);
                                    registerChannel(ch);
                                }
                            }
                        }
                    } catch (IOException e) {
                        if (Thread.currentThread().isInterrupted()) {
                            break;
                        } else {
                            logger.catching(e);
                        }
                    }
                }

            } finally {
                logger.exiting(method);
            }
        }
    }
}
