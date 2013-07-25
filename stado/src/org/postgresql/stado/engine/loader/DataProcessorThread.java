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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.log4j.Level;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.XDBGeneratorException;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.partitions.PartitionMap;
import org.postgresql.stado.optimizer.SqlExpression;
import org.postgresql.stado.parser.ExpressionType;


/**
 * Purpose:
 *
 * The main purpose of this class is to get and process data that is available
 * in the buffer placed by data-reader from some data-source (e.g. backup
 * file, result set, or some socket). Once the data has been processed, it hands
 * over them to the the writers which finally write them in the target database.
 *
 *
 */
public abstract class DataProcessorThread<T> implements Callable<Integer> {
    private static final XLogger logger = XLogger.getLogger(DataProcessorThread.class);

    private static final Map<Integer,String> NO_AUTOINCREMENTS = Collections.emptyMap();

    protected int rowsLoaded = 0;

    protected final int threadId;

    protected HashMap<Integer, INodeWriter> writerThreadsList;

    private final DataReaderAndProcessorBuffer<T> loadBuffer;

    protected long maxSuppliedRowIDValue;

    protected long maxSuppliedSerialValue;

    private final INodeWriterFactory writersFactory;

    protected final PartitionMap gridPartitionMap;

    protected final SysTable table;

    private final List<TableColumnDescription> columns;
    
    private final int columnCount;

    protected final IUniqueValueProvider rowIdProvider;

    protected final IUniqueValueProvider serialProvider;

    // Destination table definitions
    protected final XDBSessionContext client;

    protected final char separator;

    protected final String NULLValue;

    // Serial for intermediate results
    protected final int serialColumnPosition;

    protected final int partColumn;

    protected ExpressionType hashDataType = null;

    protected final int serialColumn;

    protected final int xrowidColumn;

    // If we are handling complicated correlated subquery
    protected final boolean isDestTypeNodeId;

    protected final boolean suppresSendingNodeId;

    protected final boolean noParse;

    /**
     * creates a new instance of RPWThread
     */
    public DataProcessorThread(ILoaderConfigInformation confInfo, int id,
            DataReaderAndProcessorBuffer<T> b)
    throws Exception {
        this.threadId = id;
        if (b == null) {
            throw new Exception(
            "ReaderAndProcessorBuffer buffer instance is null.");
        }
        this.loadBuffer = b;

        /*
         * Using configuration information instance, set the data processor
         * configuration information.
         */
        this.writersFactory = confInfo.getWriterFactory();
        this.gridPartitionMap = confInfo.getPartitionMap();
        this.client = confInfo.getClient();
        this.table = confInfo.getTableInformation();
        this.columns = confInfo.getTableColumnsInformation();
        this.columnCount = confInfo.getTableColumnsCount();
        this.rowIdProvider = confInfo.getRowIdProvider();
        this.serialProvider = confInfo.getSerialProvider();

        this.separator = confInfo.getSeparator();

        this.partColumn = confInfo.getPartitionColumnSequence();
        this.hashDataType = confInfo.getHashDataType();
        this.serialColumnPosition = confInfo.getSerialColumnPosition();
        this.serialColumn = confInfo.getSerialColumnSequence();
        this.xrowidColumn = confInfo.getXRowidColumnSequence();

        this.noParse = confInfo.noParse();
        this.isDestTypeNodeId = confInfo.destinationTypeNodeId();
        this.suppresSendingNodeId = confInfo.suppressSendingNodeId();

        this.NULLValue = confInfo.getNULLValue();
        this.quoteChar = confInfo.getQuoteChar();
        this.quoteEscape = confInfo.getQuoteEscape();
    }

    // Taken from config and never changed
    private char quoteChar = '\0';

    private char quoteEscape = '\0';

    /**
     * @return Returns the quoteEscape.
     */
    protected char getQuoteEscape() {
        return quoteEscape == '\0' ? getQuoteChar() : (char) quoteEscape;
    }

    /**
     * @return Returns the quoteSymbol.
     */
    protected char getQuoteChar() {
        return quoteChar;
    }

    /**
     * Prepare a NodeWriter for each node using the writersFactory
     * @throws Exception
     */
    public void initializeDataWriters() throws Exception {

        this.writerThreadsList = new HashMap<Integer, INodeWriter>();

        INodeWriter newWriter;

        java.util.Collection<Integer> coll = gridPartitionMap.allPartitions();
        for (int nodeID : coll) {
            newWriter = writersFactory.createWriter(nodeID);
            if (newWriter == null) {
                throw new Exception("Can not setup loading on node " + nodeID);
            }
            if (writerThreadsList.put(nodeID, newWriter) != null) {
                throw new Exception("Duplicate node number");
            }
            newWriter.start();
        }
    }

    /**
     * Send to Node Writers signal that loading has been completed
     * @param success true if loading has been completed successfully
     * @throws Exception
     */
    public void finishLoad(boolean success) throws Exception {
        Exception error = null;
        Iterator<INodeWriter> it = writerThreadsList.values().iterator();
        while (it.hasNext()) {
            try {
                it.next().finish(success);
            } catch (Exception e) {
                if (success) {
                    success = false;
                    error = e;
                }
            }
        }
        if (error != null) {
            throw error;
        }
    }

    /**
     * Commit changes
     */
    public void commitLoad() {
        Iterator<INodeWriter> it = writerThreadsList.values().iterator();
        while (it.hasNext()) {
            try {
                it.next().commit();
            } catch (Exception e) {
            }
        }
    }

    /**
     * Rollback changes
     */
    public void rollbackLoad() {
        Iterator<INodeWriter> it = writerThreadsList.values().iterator();
        while (it.hasNext()) {
            try {
                it.next().rollback();
            } catch (Exception e) {
            }
        }
    }

    /**
     * Close the data processor and Node Writers
     */
    public void close() {
        Iterator<INodeWriter> it = writerThreadsList.values().iterator();
        while (it.hasNext()) {
            try {
                it.next().close();
            } catch (Exception e) {
            }
        }
        writersFactory.close();
    }

    /**
     *
     */
    public Integer call() throws Exception {
        try {
            return loadData();
        } catch (Exception ex) {
            // Make sure other parties quit immediately if error
            loadBuffer.markFinished();
            throw ex;
        }
    }

    /**
     * Process incoming data
     * should be overridden by descendants
     * @return
     * @throws Exception
     */
    protected Integer loadData() throws Exception {
        int rows = 0;
        while (parseRow()) {
            insertGeneratedValues(noParse ? NO_AUTOINCREMENTS : generateAutoincrements());
            String partValue = null;
            if (partColumn >= 0) {
                partValue = getValue(partColumn + 1);
            }
            // See if we need to send data based on XNODEID
            if (isDestTypeNodeId) {
                String nodeId = getValue(getColumnCount());
                INodeWriter aWriter = writerThreadsList.get(new Integer(nodeId));
                outputRow(aWriter);
            } else {
                String groupByHashString = getNextGroupByHashString();
                if (groupByHashString != null) {
                    Collection<Integer> nodeIdList = gridPartitionMap.getPartitions(
                            SqlExpression.createConstantExpression(
                                    groupByHashString, hashDataType).getNormalizedValue());
                    Integer[] nodeArray = (Integer[]) nodeIdList.toArray();
                    INodeWriter aWriter = writerThreadsList.get(new Integer(nodeArray[0]));
                    outputRow(aWriter);
                } else {
                    for (int nodeId : gridPartitionMap.getPartitions(partValue == null ? null
                            : SqlExpression.createConstantExpression(
                                    partValue, hashDataType).getNormalizedValue())) {
                        INodeWriter aWriter = writerThreadsList.get(new Integer(nodeId));
                        outputRow(aWriter);
                    }
                }
            }
            if (serialColumn > 0) {
                maxSuppliedSerialValue = Math.max(maxSuppliedSerialValue, Long.parseLong(getValue(serialColumn + 1)));
            }
            if (xrowidColumn > 0) {
                maxSuppliedRowIDValue = Math.max(maxSuppliedRowIDValue, Long.parseLong(getValue(xrowidColumn + 1)));
            }

            rows++;
            if (rows == 100000) {
                rowsLoaded += 100000;
                rows = 0;
                logger.log(Level.DEBUG, "DataProcessor: %0% > %1% rows processed so far...",
                        new Object[] {threadId, rowsLoaded});
            }
        }
        rowsLoaded += rows;
        logger.log(Level.DEBUG, "DataProcessor: %0% > %1% rows processed.",
                new Object[] {threadId, rowsLoaded});
        return rowsLoaded;
    }

    protected abstract boolean parseRow() throws Exception;

    protected abstract void insertGeneratedValues(Map<Integer,String> values)  throws Exception;

    protected abstract String getValue(int colIndex);

    protected abstract void outputRow(INodeWriter writer) throws IOException;

    protected int getColumnCount() {
        return columnCount;
    }

    private HashMap<Integer,String> autoincrements = new HashMap<Integer,String>();
    private Map<Integer,String> generateAutoincrements() throws XDBGeneratorException {
        if (serialColumnPosition > 0) {
            autoincrements.put(serialColumnPosition, serialProvider.getNextValue());
        } else if (serialColumn > 0) {
            autoincrements.put(serialColumn, serialProvider.getNextValue());
        }
        if (xrowidColumn > 0) {
            autoincrements.put(xrowidColumn, rowIdProvider.getNextValue());
        }
        // process defaults
        if (columns != null) {
            for (int i = 0; i < columns.size(); i++) {
                SqlExpression defaultExpr = columns.get(i).getDefault();
                if (defaultExpr != null) {
                    String valueStr = defaultExpr.rebuildString(client);
                    if (valueStr == null) {
                        valueStr = NULLValue;
                    } else if (valueStr.length() > 1
                            && valueStr.startsWith("'")
                            && valueStr.endsWith("'")) {
                        valueStr = valueStr.substring(1, valueStr.length() - 1).replace(
                                "''", "'");
                    } else if (valueStr.length() > 7
                            && (valueStr.startsWith("date '") || valueStr.startsWith("time '"))
                            && valueStr.endsWith("'")) {
                        valueStr = valueStr.substring(6, valueStr.length() - 1);
                    } else if (valueStr.length() > 12
                            && valueStr.startsWith("timestamp '")
                            && valueStr.endsWith("'")) {
                        valueStr = valueStr.substring(11, valueStr.length() - 1);
                    }
                    autoincrements.put(i, valueStr);
                }
            }
        }
        return autoincrements;
    }

    /**
     * Get next row
     * This function blocks execution until more data available
     * @return Count of processed rows
     */
    protected T getNextRowValue() {
        return loadBuffer.getNextRowValue();
    }

    /**
     * Get next hash Group By value
     */
    protected String getNextGroupByHashString() {
        return loadBuffer.getNextGroupByHashString();
    }

    public long getMaxSuppliedSerialValue() {
        return maxSuppliedSerialValue;
    }

    public long getMaxSuppliedRowIDValue() {
        return maxSuppliedRowIDValue;
    }

    private HashMap<Integer, Long> rowCountMap;

    /**
     * @return a HashMap containing the number of rows each writer wrote, with
     *         the key being the node number.
     */
    public HashMap<Integer, Long> getRowCountMap() {
        if (rowCountMap == null) {
            rowCountMap = new HashMap<Integer, Long>();

            Iterator<Integer> it = writerThreadsList.keySet().iterator();
            Integer nodeId;
            while (it.hasNext()) {
                nodeId = it.next();
                rowCountMap
                .put(nodeId, writerThreadsList.get(nodeId).getRowCount());
            }
        }
        return rowCountMap;
    }

    public void cancel() {
        loadBuffer.markFinished();
    }
}
