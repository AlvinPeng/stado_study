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
package org.postgresql.stado.misc.combinedresultset;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Level;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;


/**
 * 
 */
public class ResultSetPosManager implements Runnable, Comparable<ResultSetPosManager> {
    private static final XLogger logger = XLogger
            .getLogger(ResultSetPosManager.class);

    /**
     * This value is put into buffer when fetching is finished
     */
    private static final List<Object> END_MARK = new ArrayList<Object>();

    /**
     * Underlying result set
     */
    private ResultSet rs;

    /**
     * Column count To speed up access
     */
    private int colCount;

    /**
     * We are sort data using TreeSet It does not allow equal entries, so rows
     * that equal to some other row will be skipped. That is what we want if we
     * are producing distinct ResultSet. But if we want to keep duplicates, we
     * should answer that records are differs, even they are equal by specified
     * criteria. To fulfill this requirement ServerResultSet should assign
     * different distinctModifiers to its PosManagers
     */
    private int distinctModifier;

    private List<SortCriteria> sortCriteriaList;

    /**
     * Last value returned by #getObject() Used by #lastValueWasNull()
     */
    private Object lastValue;

    /**
     * Current "ordering" value, matching to sortCriteriaList Updated if the
     * "ResultSet" advanced to next record.
     */
    private Object[] currentValue = null;

    /**
     * Current record of this "ResultSet" It is taken from the buffer when
     * next() is called.
     */
    private List<Object> currentRecord = null;

    // this may be used a lot, so use array for faster access
    int sortColPosition[];

    /**
     * Current row is taken from beginning, new lines are added to the end.
     * Linked list is the best storage. Another approach could be cyclic buffer
     * over array
     */
    private LinkedBlockingQueue<List<Object>> rowBuffer = new LinkedBlockingQueue<List<Object>>(
            Props.XDB_COMBINED_RESULTSET_BUFFER);

    /**
     * Are the manager and underlying ResultSet closed
     */
    private volatile boolean closed = false;

    private static Executor fetchPool;
    static {
        fetchPool = Executors.newCachedThreadPool();
    }

    /* Statistics */
    private long rowCount = 0;

    private volatile long fetchTime = -1;

    private long waitForNextRow = 0;

    private long waitForClient = 0;

    public ResultSetPosManager(ResultSet rs, List<SortCriteria> sortedOrderList,
            int distinctModifier) {
        this.rs = rs;
        try {
            colCount = rs.getMetaData().getColumnCount();
        } catch (SQLException ignore) {
        }

        sortCriteriaList = sortedOrderList;
        this.distinctModifier = distinctModifier;

        sortColPosition = new int[sortedOrderList.size()];
        int i = 0;

        for (SortCriteria aSortCriteria : sortCriteriaList) {
            int colpos = aSortCriteria.getColumnPosition();
            // TODO compare to colCount
            // Convert column number to list index
            sortColPosition[i++] = colpos - 1;
        }
        fetchPool.execute(this);
    }

    /**
     * Remove first record from buffer and make it "current"
     * 
     * @return true if there is current record, false otherwise (end of the
     *         result set)
     */
    public boolean next() {
        long startWait = System.currentTimeMillis();
        try {
            currentRecord = null;
            while (currentRecord == null) {
                currentRecord = rowBuffer.poll(5,
                        java.util.concurrent.TimeUnit.SECONDS);
            }
        } catch (InterruptedException ie) {
            currentRecord = null;
            return false;
        }
        waitForNextRow += (System.currentTimeMillis() - startWait);
        if (currentRecord == END_MARK) {
            currentRecord = null;
            return false;
        }
        // Update sort value
        currentValue = new Object[sortColPosition.length];
        for (int i = 0; i < sortColPosition.length; i++) {
            currentValue[i] = currentRecord.get(sortColPosition[i]);
        }
        return true;
    }

    /**
     * Get value from specified column of the current record
     * 
     * @param index
     *            1 - based number of column
     * @return Requested value
     * @throws SQLException
     *             if method #next() has not been called or has returned <CODE>false</CODE>.
     * @throws ArrayIndexOutOfBoundsException
     *             if index less than 1 or greater than column count.
     */
    public Object getObject(int index) throws SQLException {
        if (index < 1 || index > colCount) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        if (currentRecord == null) {
            throw new SQLException("No current record");
        }
        lastValue = currentRecord.get(index - 1);
        return lastValue;
    }

    /**
     * 
     * @return <CODE>true</CODE> if last value returned by #getObject(int) was
     *         <CODE>NULL</CODE>, <CODE>false</CODE> otherwise
     */
    public boolean lastValueWasNull() {
        return lastValue == null;
    }

    /**
     * Stop fetching and close underlying ResultSet
     * 
     */
    public void close() {
        closed = true;
        rowBuffer.clear();
        // Thread safety note: This class should not be used by multiple
        // threads.
        // If fetching is finished and one thread is in next() it may encounter
        // empty buffer and freeze forever waiting for END_MARK, that has been
        // removed by rowBuffer.clear();

        // Ensure fetching is stopped before closing result set
        while (fetchTime < 0) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignore) {
            }
        }            
        
        try {
            rs.close();
        } catch (SQLException se) {
            logger.catching(se);
        }
        // Dump statistics
        logger
                .log(
                        Level.DEBUG,
                        "Performance statistics:\n"
                                + "Row count: %0%\n"
                                + "Overall fetch time: %1%ms\n"
                                + "Total time spent by client waiting for next record: %2%ms\n"
                                + "Total time spent by Fetch Thread while buffer is full: %3%ms",
                        new Object[] { new Long(rowCount), new Long(fetchTime),
                                new Long(waitForNextRow),
                                new Long(waitForClient) });
    }

    /**
     * Fetch data from underlying ResultSet into buffer
     * 
     * @see java.lang.Runnable#run()
     */
    public void run() {
        try {
            long startFetch = System.currentTimeMillis();
            try {
                while (rs.next()) {
                    if (closed) {
                        return;
                    }
                    List<Object> row = new ArrayList<Object>(colCount);
                    for (int i = 1; i <= colCount; i++) {
                        row.add(rs.getObject(i));
                    }
                    long startWait = System.currentTimeMillis();
                    try {
                        rowBuffer.put(row);
                    } catch (InterruptedException ignore) {
                    }
                    waitForClient += (System.currentTimeMillis() - startWait);
                    rowCount++;
                }
            } finally {
                fetchTime = System.currentTimeMillis() - startFetch;
                if (fetchTime < 0) {
                    fetchTime = 0;
                }
                // Do not close, RS metadata may be still in use
                // rs.close();
            }
        } catch (SQLException se) {
            logger.catching(se);
        } finally {
            while (!rowBuffer.offer(END_MARK)) {
                ;
            }
        }
    }

    /**
     * Compare current record of this ResultSetPosManager to current record of
     * another ResultSetPosManager according to sort criteria.
     * 
     * @param obj2
     * @return
     */
    public int compareTo(ResultSetPosManager obj2) {
        ResultSetPosManager aResultSetPosManager1 = this;
        ResultSetPosManager aResultSetPosManager2 = obj2;

        // TODO do not scroll
        Object[] value1 = aResultSetPosManager1.currentValue;
        Object[] value2 = aResultSetPosManager2.currentValue;

        int count = 0;

        for (SortCriteria criteria : sortCriteriaList) {
            // Check is there nulls:
            if (value1[count] == null && value2[count] == null) {
                count++;
                continue;
            }
            if (value1[count] == null) {
                switch (Props.XDB_COMBINED_RESULTSET_SORT_NULLS_STYLE) {
                case Props.SORT_NULLS_AT_END:
                    return -1;
                case Props.SORT_NULLS_AT_START:
                    return 1;
                case Props.SORT_NULLS_LOW:
                    return criteria.getDirection() == SortCriteria.DESCENDING ? -1
                            : 1;
                case Props.SORT_NULLS_HIGH:
                default:
                    return criteria.getDirection() == SortCriteria.DESCENDING ? 1
                            : -1;
                }
            }
            if (value2[count] == null) {
                switch (Props.XDB_COMBINED_RESULTSET_SORT_NULLS_STYLE) {
                case Props.SORT_NULLS_AT_END:
                    return 1;
                case Props.SORT_NULLS_AT_START:
                    return -1;
                case Props.SORT_NULLS_LOW:
                    return criteria.getDirection() == SortCriteria.DESCENDING ? 1
                            : -1;
                case Props.SORT_NULLS_HIGH:
                default:
                    return criteria.getDirection() == SortCriteria.DESCENDING ? -1
                            : 1;
                }
            }
            int k = compare(value1[count], value2[count]);
            if (k == 0) {
                count++;
                continue;
            } else {
                return criteria.getDirection() == SortCriteria.DESCENDING ? k
                        : -k;
            }
        }
        // If rows are equal one record will be thrown away, unless RS is
        // distinct
        return aResultSetPosManager1.distinctModifier == aResultSetPosManager2.distinctModifier ? 0
                : (aResultSetPosManager1.distinctModifier > aResultSetPosManager2.distinctModifier ? 1
                        : -1);
    }

    private int compare(Object obj1, Object obj2) {
        if (obj1 instanceof Number && obj2 instanceof Number) {
            boolean compareAsDouble = obj1 instanceof Float
                    || obj1 instanceof Double || obj1 instanceof BigDecimal
                    || obj2 instanceof Float || obj2 instanceof Double
                    || obj2 instanceof BigDecimal;
            if (compareAsDouble) {
                double val1 = ((Number) obj1).doubleValue();
                double val2 = ((Number) obj2).doubleValue();
                if (val1 == val2) {
                    return 0;
                } else {
                    return val1 > val2 ? 1 : -1;
                }
            } else // whole numbers, compare as long
            {
                long val1 = ((Number) obj1).longValue();
                long val2 = ((Number) obj2).longValue();
                if (val1 == val2) {
                    return 0;
                } else {
                    return val1 > val2 ? 1 : -1;
                }
            }
        } else if (obj1 instanceof Date && obj2 instanceof Date) {
            return ((Date) obj1).compareTo((Date) obj2);
        } else if (obj1 instanceof Boolean && obj2 instanceof Boolean) {
            // JDK 1.5 feature
            // return ((Boolean) obj1).compareTo((Boolean) obj2);
            return (obj1.equals(obj2)) ? 0
                    : (((Boolean) obj1).booleanValue() ? 1 : -1);
        } else {
            // Compare string representations
            String str1 = obj1.toString();
            String str2 = obj2.toString();
            if (Props.XDB_COMBINED_RESULTSET_SORT_TRIM) {
                str1 = str1.trim();
                str2 = str2.trim();
            }
            int count = Math.min(str1.length(), str2.length());
            for (int i = 0; i < count; i++) {
                char chr1 = str1.charAt(i);
                char chr2 = str2.charAt(i);
                if (!Props.XDB_COMBINED_RESULTSET_SORT_CASE_SENSITIVE) {
                    chr1 = Character.toLowerCase(chr1);
                    chr2 = Character.toLowerCase(chr2);
                }
                if (chr1 == chr2) {
                    continue;
                } else {
                    return chr1 > chr2 ? 1 : -1;
                }
            }
            return str1.length() > count ? 1 : (str2.length() > count ? -1 : 0);
        }
    }

    @Override
    public boolean equals(Object tocheck) {
        return this == tocheck 
                || tocheck instanceof ResultSetPosManager && compareTo((ResultSetPosManager)tocheck) == 0;
    }

    @Override
    public String toString() {
        String str = "";
        int count = 0;
        for (SortCriteria aSortCriteria : sortCriteriaList) {
            str += ("Column Postion  : " + aSortCriteria.getColumnPosition()
                    + "        Value : " + currentValue[count]);
            count++;
        }
        return str;
    }
}
