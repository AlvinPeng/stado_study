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

package org.postgresql.stado.engine.loader;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.exception.XDBDataReaderException;


/**
 * Puropose:
 *
 * The main purpose of this class instance is to act as thread-safe data-buffer for
 * the data-reader thread and data-processor threads so that they can easily act as
 * producer-consumer against some data-source (e.g. file, result set, and socket).
 *
 *
 */
public class DataReaderAndProcessorBuffer<T> {
    private static final XLogger logger = XLogger.getLogger(DataReaderAndProcessorBuffer.class);
    private long waitForFullBuffer = 0;
    private long waitForEmptyBuffer = 0;


    private T[] rowsBuffer;
    private String[] groupByHashBuffer;
    private String currentGroupByHashString;
    
    private int in ;
    private int out ;

    private int bufferLimit ;
    private int currentSize ;

    private boolean hasFinished ;

    /**
     * Creates a new instance of PCBuffer
     */
    public DataReaderAndProcessorBuffer(int capacity, Loader.DATA_SOURCE source) {
        initialize(capacity, source) ;
    }

    /**
     * Initialize *this* instance of PCBuffer
     */
    public void initialize(int capacity, Loader.DATA_SOURCE source) {
        if (capacity <= 0) {
            throw new IllegalArgumentException();
        }

        bufferLimit = capacity ;
        rowsBuffer = (T[]) new Object[capacity];
        groupByHashBuffer = new String[capacity];
        in = 0 ;
        out = 0 ;
        hasFinished = false ;
    }

    /**
     * Mark *this* instance of PCBuffer Finished, so that we could stop waiting once
     * rowsBuffer is empty and hasFinished is true.
     */
    public synchronized void markFinished() {
        hasFinished = true ;
        notify() ;
        if (waitForFullBuffer > 0) {
            logger.debug("Wait for full buffer: " + waitForFullBuffer);
        }
        if (waitForEmptyBuffer > 0) {
            logger.debug("Wait for empty buffer: " + waitForEmptyBuffer);
        }
    }

    /**
     * Put row value in the rowsBuffer.
     */
    public synchronized void putRowValue(T value, String hashString)
    throws XDBDataReaderException {
        try {
            while ( !hasFinished && currentSize == bufferLimit ) {
                try {
                    long curTime = System.currentTimeMillis();
                    wait() ;
                    waitForFullBuffer += System.currentTimeMillis() - curTime;
                }
                catch (InterruptedException ex) {
                    // keep waiting till our buffer has any free space.
                }
            }
            if (hasFinished) {
                throw new XDBDataReaderException("Cancelled by user");
            }
            if ( in==bufferLimit ) { in = 0 ; }
            currentSize++ ;
            groupByHashBuffer[in] = hashString;
            rowsBuffer[in++] = value ;
        }
        finally {
            notify() ;
        }
    }

    /**
     * Put row value in the rowsBuffer.
     */
    public synchronized void putRowValue(T value) throws XDBDataReaderException {
        try {
            putRowValue(value, null);
        }
        finally {
            notify() ;
        }
    }
    
    /**
     * Fetch the buffered row value.
     */
    public synchronized T getNextRowValue() {
        try {
            while ( currentSize==0 ) {
                if ( hasFinished ) {
                    return null ;
                }
                else {
                    try {
                        long curTime = System.currentTimeMillis();
                        wait() ;
                        waitForEmptyBuffer += System.currentTimeMillis() - curTime;
                    }
                    catch (InterruptedException ex) {
                        // keep waiting till our buffer has got some data.
                    }
                }
            }
            if ( out==bufferLimit ) { out = 0 ; }
            currentSize-- ;
            // set this so it can be obtained via getNextGroupByHashString
            currentGroupByHashString = groupByHashBuffer[out];
            return rowsBuffer[out++] ;
        }
        finally {
            notify() ;
        }
    }
    
    /**
     * Fetch the group by hash string. getNextRowValue() should be
     * called before this one.
     *
     */
    public synchronized String getNextGroupByHashString() {        
        try {
            return currentGroupByHashString;            
        }
        finally {
            notify() ;
        }
    }    
}
