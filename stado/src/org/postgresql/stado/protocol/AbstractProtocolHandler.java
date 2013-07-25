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
package org.postgresql.stado.protocol;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.Executor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.postgresql.stado.common.util.XLogger;


/**
 * The class ProtocolHandler manages the XDBServer side protocol.
 *
 * It runs a thread in which it waits for client requests. When it receives a
 * client request; it puts that request onto a queue. This queue is backed by a
 * ThreadPool, from which a thread services the request.
 *
 * By varying the size of the queue and number of threads in the pool, we can
 * obtain desired throughput.
 *
 *
 */

// This class could have been clubbed with the Server, but
// I prefer to keep it out ( better object orientedness)
public abstract class AbstractProtocolHandler<T> implements ProtocolManager {

    /** the logger for ProtocolHandler */
    private static XLogger logger = XLogger
            .getLogger(AbstractProtocolHandler.class);

    /** hash table of clients indexed by the channel */
    private HashMap<SocketChannel, T> clients;

    /** the queue backed by a thread pool */
    private Executor serviceQueue;

    /**
     * Creates a new instance of ProtocolHandler and start connection handling
     * loop
     *
     * @throws java.io.IOException
     */
    protected AbstractProtocolHandler() throws IOException {
        clients = new HashMap<SocketChannel, T>();
        serviceQueue = new ThreadPoolExecutor(1, // Have at least one thread
                // ready to run
                Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, // Discard thread if
                // it idle for 1 min
                new SynchronousQueue<Runnable>() // Do not queue
        );
    }

    /**
     *
     * @param channel
     * @param clientContext
     */
    public synchronized void addClient(SocketChannel channel) {
        try {
            T client = createClient(channel);
            clients.put(channel, client);
            if (client instanceof Runnable) {
                serviceQueue.execute((Runnable) client);
            }
        } catch (Exception ex) {
            logger.catching(ex);
            try {
                channel.close();
            } catch (IOException ignore) {
            }
        }
    }

    /**
     *
     * @param channel
     */
    public synchronized void removeClient(SocketChannel channel) {
        T clientContext = clients.remove(channel);
        closeClient(clientContext);
        logger.debug("connectedClients=" + clients.size());
    }

    /**
     * Must be externally synchronized !
     *
     * @return
     */
    protected Iterator<T> clientIterator() {
        return clients.values().iterator();
    }

    protected abstract T createClient(SocketChannel channel) throws Exception;

    protected abstract void closeClient(T clientContext);
}
