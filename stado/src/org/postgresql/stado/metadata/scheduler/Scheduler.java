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
package org.postgresql.stado.metadata.scheduler;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.postgresql.stado.common.util.Property;
import org.postgresql.stado.engine.XDBSessionContext;


/**
 * Request handling sequence: 1. Request is enqueued. XDBSessionContext
 * maintains a queue of requests, but while we does not support multiple
 * concurrent requests within one connection there is at most one entry in the
 * queue. 2. Request is scheduled. First request in queue is registered with
 * scheduler. If the request is enqueued and queue has been empty it is
 * scheduled immediately. 3. Scheduler notifies XDBSessionContext that request
 * can be executed. XDBSessionContext tries and acquires locks needed by calling
 * LockManager, then reports result back to Scheduler. There are three possible
 * results: a. LockManager succeeded and locks are acquired. Request is removed
 * from Scheduler. Go to Step 4. b. LockManager failed (there are conflicting
 * locks) and no locks are acquired. Scheduler moves to another session, request
 * is waiting for next time. c. Exception is thrown (deadlock is detected) and
 * no locks are acquired. Request is removed from Scheduler, error response is
 * written to client. 4. Scheduler allocates thread and executes request
 * prepared on Step 3 within it. 5. Locks are released by calling LockManager.
 * It is possible that LockManager does not release the locks immediately. If
 * there is active transaction LockManager will held the locks until transaction
 * is committed or rolled back. 6. XDBSessionContext is cleaned and become ready
 * for next request. If there is one in the queue it is scheduled.
 */
public class Scheduler implements Runnable {

    private static final int QUERY_LIMIT = Property.getInt(
            "xdb.jdbc.pool.query.count", Property.getInt(
                    "xdb.jdbc.pool.maxsize", 10));

    private static final int LARGE_QUERY_LIMIT = Property.getInt(
            "xdb.jdbc.pool.largequery.count", 2);

    private final TreeMap<RequestCost, XDBSessionContext> serviceQueue = new TreeMap<RequestCost, XDBSessionContext>();

    private final AtomicInteger queryCount;

    private final AtomicInteger largeQueryCount;

    public Scheduler() {
        queryCount = new AtomicInteger(0);
        largeQueryCount = new AtomicInteger(0);
    }

    public void addRequest(RequestCost cost, XDBSessionContext client) {
        boolean fallThru;
        synchronized (this) {
            fallThru = serviceQueue.isEmpty();
            serviceQueue.put(cost, client);
            notifyAll();
        }
        if (!fallThru) {
            synchronized (client) {
                try {
                    client.wait(1000);
                } catch (InterruptedException ie) {
                    // ignore
                }
            }
        }
        queryCount.incrementAndGet();
        if (cost.isLarge()) {
            largeQueryCount.incrementAndGet();
        }
    }

    public XDBSessionContext removeRequest(RequestCost cost) {
        queryCount.decrementAndGet();
        if (cost.isLarge()) {
            largeQueryCount.decrementAndGet();
        }
        XDBSessionContext client;
        synchronized (this) {
            client = serviceQueue.remove(cost);
            notifyAll();
        }
        if (client != null) {
            synchronized (client) {
                client.notifyAll();
            }
        }
        return client;
    }

    public void holdRequest(RequestCost cost) {
        XDBSessionContext client = removeRequest(cost);
        if (client != null) {
            synchronized (this) {
                try {
                    wait(1000);
                } catch (InterruptedException ie) {
                    // ignore
                }
            }
            addRequest(cost, client);
        }
    }

    public synchronized void run() {
        while (true) {
            for (Map.Entry<RequestCost, XDBSessionContext> entry : serviceQueue
                    .entrySet()) {
                if (QUERY_LIMIT <= queryCount.get()) {
                    break;
                }
                if (entry.getKey().isLarge()
                        && LARGE_QUERY_LIMIT <= largeQueryCount.get()) {
                    continue;
                }
                synchronized (entry.getValue()) {
                    entry.getValue().notifyAll();
                }
            }
            try {
                wait(1000);
            } catch (InterruptedException ie) {
                // ignore
            }
        }
    }
}
