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
package org.postgresql.stado.engine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.exception.XDBServerException;


/**
 * Abstract class to pool objects, like Threads, JDBC connections, etc.
 * ObjectPool is assumed homogenous, that is when object is requested, no matter
 * which is returned. Class is thread-safe
 *
 *  
 */
public abstract class ObjectPool<T> {
    private static final XLogger logger = XLogger.getLogger(ObjectPool.class);

    // Threshold for maximum lifetime of object before destroying
    public static final long DEFAULT_MAX_LIFETIME = 600000L;// 10 min
    
    public static final long DEFAULT_RELEASE_TIMEOUT = 300000L;// 5 min

    public static final long DEFAULT_GET_TIMEOUT = 60000L;// 1 min

    private int minSize;

    private int maxSize;

    private boolean destroyed;

    private long maxLifetime = DEFAULT_MAX_LIFETIME;
    
    private long releaseTimeout = DEFAULT_RELEASE_TIMEOUT;

    private long getTimeout = DEFAULT_GET_TIMEOUT;

    private LinkedList<PoolEntry<T>> buffer;
  
    // Store the object as a key and the other pool entry container
    // as the value
    private HashMap<T,PoolEntry> out;
    
    private Runnable cleanupAgent = null;

    /**
     *
     * @param minSize
     * @param maxSize
     */
    public ObjectPool(int minSize, int maxSize) {
        final String method = "ObjectPool";
        logger.entering(method, new Object[] { new Integer(minSize),
                new Integer(maxSize) });
        try {

            this.minSize = minSize;
            this.maxSize = maxSize;
            destroyed = false;
            buffer = new LinkedList<PoolEntry<T>>();
            out = new HashMap<T,PoolEntry>();

        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    public void initBuffer() throws XDBServerException {
        final String method = "initBuffer";
        logger.entering(method);
        try {

            for (int i = buffer.size() + out.size(); i < minSize; i++) {
                try {                    
                    PoolEntry<T> poolEntry = createPoolEntry();
                    buffer.addLast(poolEntry);
                } catch (XDBServerException e) {
                    logger.catching(e);
                    XDBServerException ex = new XDBServerException(
                            "Failed to initialize pool", e);
                    logger.throwing(ex);
                    throw ex;
                }
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @throws org.postgresql.stado.exception.XDBServerException
     * @return
     */
    public synchronized T getObject() throws XDBServerException {
        final String method = "getObject";
        logger.entering(method);
        try {

            logger.debug(getClass().getName() + ".getObject(): Buffer has "
                    + buffer.size() + " objects, " + out.size()
                    + " is out there.");
            // long start = System.currentTimeMillis();
            // while (true)
            // {
            if (destroyed) {
                XDBServerException ex = new XDBServerException(
                        "Can not serve object - pool is destroyed");
                logger.throwing(ex);
                throw ex;
            }
            // Because of possible memory issues on the backend,
            // aggresively clean up stale objects first
            packBuffer();
            if (!buffer.isEmpty()) {
                PoolEntry<T> poolEntry = buffer.removeFirst();
                out.put(poolEntry.entry, poolEntry);
                return poolEntry.entry;
            }
            if (out.size() < maxSize) {
                PoolEntry<T> poolEntry = createPoolEntry();
                out.put(poolEntry.entry, poolEntry);
                return poolEntry.entry;
            }
            if (cleanupAgent != null) {
                new Thread(cleanupAgent).start();
            }

            /*
             * Always create an entry anyway. We have a problem
             * where objects may not always be being returned to the pool 
             * in a timely manner.
             * TODO: Fix this
             */
            PoolEntry<T> poolEntry = createPoolEntry();
            out.put(poolEntry.entry, poolEntry);
            return poolEntry.entry;
            /*
             * Commented out as part of retry wait loop until we can 
             * debug above issue.
             * try { long toWait =
             * start + getTimeout - System.currentTimeMillis(); if (toWait <= 0) {
             * XDBServerException ex = new XDBServerException( "Can not serve
             * object - timeout expired"); logger.throwing (ex); throw ex; }
             * wait(toWait); } catch (InterruptedException e) { }
             */
            // }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @return whether or not the object is "out"
     * @param entry
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    public synchronized boolean isOut(Object entry) throws XDBServerException {
        final String method = "isOut";
        logger.entering(method, new Object[] { entry });
        try {
            boolean retValue = out.containsKey(entry);
            notifyAll();
            return retValue;
        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @param entry
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    public synchronized void releaseObject(T entry) throws XDBServerException {
        final String method = "releaseObject";
        logger.entering(method, new Object[] { entry });
        try {
            PoolEntry poolEntry = out.remove(entry);

            if (poolEntry == null) {
                XDBServerException ex = new XDBServerException(
                        "Attempt to release unknown object");
                logger.throwing(ex);
                throw ex;
            }
            if (destroyed) {
                destroyEntry(entry);
            } else {
                if (buffer.size() + out.size() <= maxSize) {
                    poolEntry.pool_timestamp = System.currentTimeMillis();
                    buffer.addLast(poolEntry);
                    packBuffer();
                } else {
                    destroyEntry(entry);
                    packBuffer();
                }
            }
            notifyAll();
            logger.debug(getClass().getName() + ".releaseObject(): Buffer has "
                    + buffer.size() + " objects, " + out.size()
                    + " is out there.");

        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @param entry
     * @param finalize
     * @throws org.postgresql.stado.exception.XDBServerException
     */


    public synchronized void destroyObject(T entry, boolean finalize)
            throws XDBServerException {
        final String method = "destroyObject";
        logger.entering(method, new Object[] { entry, new Boolean(finalize) });
        try {

            PoolEntry poolEntry = out.remove(entry);

            if (poolEntry == null) {
                XDBServerException ex = new XDBServerException(
                        "Attempt to release unknown object");
                logger.throwing(ex);
                throw ex;
            }
            if (finalize) {
                destroyEntry(entry);
            }
            if (buffer.size() + out.size() < minSize) {
                initBuffer();
            }
            notifyAll();
            logger.debug(getClass().getName() + ".releaseObject(): Buffer has "
                    + buffer.size() + " objects, " + out.size()
                    + " is out there.");

        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     */
    public synchronized void destroy() {
        destroyed = true;
        while (!buffer.isEmpty()) {
            destroyEntry(buffer.removeFirst().entry);
        }
        for (T t : out.keySet()) {
            destroyEntry(t);
        }
        out.clear();
        notifyAll();
    }

    /**
     *
     * @throws org.postgresql.stado.exception.XDBServerException
     * @return
     */
    protected abstract T createEntry() throws XDBServerException;

    /**
     * Create a new pool entry, including creating the actual 
     * pool item by calling abstract function createEntry()
     * @return 
     */
    private PoolEntry createPoolEntry() {
        PoolEntry<T> poolEntry = new PoolEntry<T>();
        poolEntry.create_timestamp = System.currentTimeMillis();
        poolEntry.pool_timestamp = poolEntry.create_timestamp;
        poolEntry.entry = createEntry();

        return poolEntry;
    }
    
    /**
     *
     * @param entry
     */
    protected void destroyEntry(T entry) {
    }

    protected synchronized void packBuffer() {
        Iterator<PoolEntry<T>> poolIterator = buffer.listIterator();
        while (poolIterator.hasNext()) {            
            PoolEntry<T> poolEntry = poolIterator.next();
            if (poolEntry.pool_timestamp + releaseTimeout < System.currentTimeMillis() ||
                    poolEntry.create_timestamp + maxLifetime < System.currentTimeMillis()) {
                
                poolIterator.remove();
                destroyEntry(poolEntry.entry);
            } else {
                return;
            }
        }
    }

    /**
     * @return the timeout
     */
    public synchronized long getGetTimeout() {
        return getTimeout;
    }

    /**
     * @return the max size of the pool
     */
    public synchronized int getMaxSize() {
        return maxSize;
    }

    /**
     * @return the min size of the pool
     */
    public synchronized int getMinSize() {
        return minSize;
    }

    /**
     * @return the objects which have been taken from the pool
     */
    public synchronized Collection<T> getObjectsInUse() {
        return new ArrayList<T>(out.keySet());
    }

    /**
     * @return the release timeout
     */
    public synchronized long getMaxLifetime() {
        return maxLifetime;
    }
    
    /**
     * @return the release timeout
     */
    public synchronized long getReleaseTimeout() {
        return releaseTimeout;
    }

    /**
     * @param l the timeout
     */
    public synchronized void setGetTimeout(long l) {
        getTimeout = l;
    }

    /**
     * @param i the max size of the pool
     */
    public synchronized void setMaxSize(int i) {
        maxSize = i;
    }

    /**
     * @param i the min size of the pool
     */
    public synchronized void setMinSize(int i) {
        minSize = i;
    }

    /**
     * @param l the maximum target lifetime for the object.
     * If it is in use, it will not be interrupted.
     */
    public synchronized void setMaxLifetime(long l) {
        maxLifetime = l;
    }
    
    /**
     * @param l the release timeout
     */
    public synchronized void setReleaseTimeout(long l) {
        releaseTimeout = l;
    }

    /**
     *
     * @param agent
     * @return
     */
    public synchronized Runnable setCleanupAgent(Runnable agent) {
        Runnable oldAgent = cleanupAgent;
        cleanupAgent = agent;
        return oldAgent;
    }

    protected class PoolEntry<E> {
        public long create_timestamp;
        public long pool_timestamp;

        public E entry;
    }
}
