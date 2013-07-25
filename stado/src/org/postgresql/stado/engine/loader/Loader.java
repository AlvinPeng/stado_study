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
 * XDBLoader.java
 *
 *
 */
package org.postgresql.stado.engine.loader;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Level;
import org.postgresql.stado.common.util.ParseCmdLine;
import org.postgresql.stado.common.util.Property;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.engine.copy.CopyOut;
import org.postgresql.stado.exception.XDBDataReaderException;
import org.postgresql.stado.exception.XDBGeneratorException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.NodeDBConnectionInfo;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysRowIDHandler;
import org.postgresql.stado.metadata.SysSerialGenerator;
import org.postgresql.stado.metadata.SysSerialIDHandler;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.partitions.PartitionMap;
import org.postgresql.stado.optimizer.SqlExpression;
import org.postgresql.stado.parser.ExpressionType;
import org.postgresql.stado.parser.SqlCreateTableColumn;
import org.postgresql.stado.parser.handler.IdentifierHandler;


/**
 *
 */
public class Loader implements ILoaderConfigInformation {
    private static final XLogger logger = XLogger.getLogger(Loader.class);

    public static enum DATA_SOURCE {
        COPY_OUT, RESULT_SET, TEXT, CSV, BINARY
    };

    // ////////////// Keys
    private static final String HEADER_COLUMN_SEPARATOR_KEY = "xdb.loader.header.columnseparator"; // Default

    // ","
    // (comma)

    private static final String HEADER_TEMPLATE_KEY = "xdb.loader.header.template"; // params:

    // {column.1},
    // ...,
    // {column.N},
    // {column_list}

    private static final String FOOTER_COLUMN_SEPARATOR_KEY = "xdb.loader.footer.columnseparator"; // Default

    // ","
    // (comma)

    private static final String FOOTER_TEMPLATE_KEY = "xdb.loader.footer.template"; // params:

    // {column.1},
    // ...,
    // {column.N},
    // {column_list}

    private static final String ROW_COLUMN_SEPARATOR_KEY = "xdb.loader.row.columnseparator"; // Default

    // ","
    // (comma)

    private static ExecutorService executorService;
    static {
        executorService = Executors.newCachedThreadPool();
    }

    private DataProcessorThread[] dpThreads;

    // Not null only if dataSource is RESULT_SET
    private ResultSet inputRS = null;

    // Not null only if dataSource is COPY_OUT
    private CopyOut copyOut = null;

    // Not null if dataSource is TEXT or CSV or BINARY
    private InputStream inputStream = null;

    private char separator = '\t';

    private String valueNULL = "\\N";

    // Parse mode CSV only
    private char quoteChar = '"';

    private char quoteEscape = '\0';

    private String forcenotnullinfo = Props.XDB_LOADER_NODEWRITER_CSV_FORCENOTNULLINFO_NONE;

    // in case we want to stream in values only for a subset of columns
    private String columninfo = Props.XDB_LOADER_NODEWRITER_COLUMNINFO_NONE;

    // Destination table definitions
    private XDBSessionContext client = null;

    private SysTable table = null;

    private NodeDBConnectionInfo[] nodeInfos;

    private String tableName;

    private List<TableColumnDescription> columns;

    private int partColumn;

    // For group by, list of expressions in ResultSet to hash by
    private int[] groupHashList;

    // Serial for intermediate results
    private int serialColumnPosition;

    // private long nextRowID;
    private PartitionMap partitionMap;

    private ExpressionType hashDataType;

    // Writers
    private INodeWriterFactory writerFactory;

    private HashMap<Integer, INodeWriter> writers;

    private long rowCount = 0;

    // Output stream options
    /** If we are handling complicated correlated subquery */
    private boolean isDestTypeNodeId = false;

    /** Whether or not we need to NOT append XNODEID values in ResultSets */
    private boolean suppressSendingNodeId = false;

    private int serialColumn = -1;

    private int xrowidColumn = -1;

    private boolean hasDefaults = false;

    private long maxSuppliedSerialValue = -1;

    private long maxSuppliedRowIDValue = -1;

    private IUniqueValueProvider rowIdProvider = null;

    private IUniqueValueProvider serialProvider = null;

    private HashMap<Integer, Long> rowCountMap;

    /**
     * Create a Loader with data source as ResultSet (intermediate results)
     * @param rs
     * @param columns
     */
    public Loader(ResultSet rs, String columns) {
        this.inputRS = rs;
        this.setColumns(columns);
        this.setFieldDelimiter(Props.XDB_LOADER_NODEWRITER_DEFAULT_DELIMITER);
        this.setNullValue(Props.XDB_LOADER_NODEWRITER_DEFAULT_NULL);
        dataSource = DATA_SOURCE.RESULT_SET;
    }

    /**
     * Create a Loader with data source as ResultSet (intermediate results)
     * @param copyOut
     * @param columns
     */
    public Loader(CopyOut copyOut, String columns) {
        this.copyOut = copyOut;
        this.setColumns(columns);
        this.setFieldDelimiter(Props.XDB_LOADER_NODEWRITER_DEFAULT_DELIMITER);
        this.setNullValue(Props.XDB_LOADER_NODEWRITER_DEFAULT_NULL);
        dataSource = DATA_SOURCE.COPY_OUT;
    }

    /**
     * Create a Loader without data source (text or csv format)
     * Data source must be initialized later
     * @param delimiter
     * @param nullValue
     */
    public Loader(String delimiter, String nullValue) {
        this.setFieldDelimiter(delimiter);
        this.setNullValue(nullValue);
    }

    /**
     * Set data source as a stream (file, socket, etc.)
     * Format is text or csv
     * @param inputStream
     */
    public void setDataSource(InputStream inputStream) {
        this.inputStream = inputStream;
        if (dataSource != DATA_SOURCE.CSV) {
            dataSource = DATA_SOURCE.TEXT;
        }
    }

    /**
     * Set data source as a file
     * @param fileName
     * @throws IOException
     */
    public void setDataSource(String fileName) throws IOException {
        setDataSource(new FileInputStream(fileName));
    }

    /**
     * Set quote character and character to escape quote
     * Implicitly sets source format to csv
     * @param quoteChar
     * @param quoteEscape
     */
    public void setQuotes(char quoteChar, char quoteEscape) {
        this.quoteChar = quoteChar;
        this.quoteEscape = quoteEscape;
        dataSource = DATA_SOURCE.CSV;
    }

    /**
     * Set string which represents null value
     * @param valueNull
     */
    public void setNullValue(String valueNull) {
        this.valueNULL = valueNull;
    }

    /**
     * Set field delimiter
     * @param delimiter
     */
    public void setFieldDelimiter(String delimiter) {
        if (delimiter == null || delimiter.length() == 0) {
            delimiter = Props.XDB_LOADER_NODEWRITER_DEFAULT_DELIMITER;
        }
        separator = delimiter.charAt(0);
    }

    /**
     * Sets the columns to use, in case we specify a subset of columns to use
     */
    public void setColumns(String columnString) {
        if (columnString == null || columnString.length() == 0) {
            columninfo = Props.XDB_LOADER_NODEWRITER_COLUMNINFO_NONE;
        } else {
            columninfo = ParseCmdLine.substitute(
                    Props.XDB_LOADER_NODEWRITER_COLUMNINFO,
                    Collections.singletonMap("columns", columnString));
        }
    }

    public void setLocalTableInfo(SysTable table, XDBSessionContext client,
            List<String> columnNames, boolean withOids)
    throws XDBDataReaderException, XDBGeneratorException {
        this.table = table;
        this.tableName = table.getTableName();
        this.partitionMap = table.getPartitionMap();
        Collection<DBNode> dbNodes = table.getNodeList();
        this.nodeInfos = new NodeDBConnectionInfo[dbNodes.size()];
        int i = 0;
        for (DBNode dbNode : dbNodes) {
            this.nodeInfos[i++] = dbNode.getNodeDBConnectionInfo();
        }
        if (columnNames == null || columnNames.size() == 0) {
            columninfo = Props.XDB_LOADER_NODEWRITER_COLUMNINFO_NONE;
            for (SysColumn col : table.getColumns()) {
                appendColumnDescription(col.getColName(), col.isSerial(),
                        col.getColLength(), col.isNullable(), null);
                if (SqlCreateTableColumn.XROWID_NAME.equalsIgnoreCase(col.getColName())
                        && !withOids) {
                    xrowidColumn = columns.size() - 1;
                    SysRowIDHandler xrowidHandler = table.getRowIDHandler();
                    xrowidHandler.allocateRange(0, client);
                    setRowIdProvider(new SerialGeneratorProvider(xrowidHandler));
                }
            }
        } else {
            StringBuilder sbColumns = new StringBuilder(512);
            for (String columnName : columnNames) {
                SysColumn col = table.getSysColumn(columnName);
                if (col == null) {
                    throw new XDBDataReaderException("Column " + columnName
                            + " does not exist in table " + tableName);
                }
                appendColumnDescription(col.getColName(), col.isSerial(),
                        col.getColLength(), col.isNullable(), null);
                sbColumns.append(IdentifierHandler.quote(columnName)).append(",");
            }
            SysColumn xrowidCol = table.getSysColumn(SqlCreateTableColumn.XROWID_NAME);
            if (xrowidCol != null) {
                appendColumnDescription(xrowidCol.getColName(),
                        xrowidCol.isSerial(), xrowidCol.getColLength(),
                        xrowidCol.isNullable(), null);
                sbColumns.append(SqlCreateTableColumn.XROWID_NAME).append(",");
                if (!withOids) {
                    xrowidColumn = columns.size() - 1;
                    SysRowIDHandler xrowidHandler = table.getRowIDHandler();
                    xrowidHandler.allocateRange(0, client);
                    setRowIdProvider(new SerialGeneratorProvider(xrowidHandler));
                }
            }
            // Append serial if presents in the table
            SysColumn col = table.getSerialColumn();
            if (col != null && !columnNames.contains(col.getColName())) {
                appendColumnDescription(col.getColName(), col.isSerial(),
                        col.getColLength(), col.isNullable(), null);
                serialColumn = columns.size() - 1;
                SysSerialIDHandler serialHandler = table.getSerialHandler();
                // update handler
                serialHandler.allocateRange(0, client);
                setSerialProvider(new SerialGeneratorProvider(serialHandler));
                sbColumns.append(col.getColName()).append(",");
            }
            // Append defaults if present
            for (SysColumn col1 : table.getColumns()) {
                if (col1.isSerial()
                        || SqlCreateTableColumn.XROWID_NAME.equals(col1.getColName())) {
                    // The column is already added
                    continue;
                }
                SqlExpression defaultExpr = col1.getDefaultExpr(client);
                // Force presence of partitioning column
                if (defaultExpr == null && col1 == table.getPartitionedColumn()) {
                    defaultExpr = SqlExpression.createConstantExpression(null,
                            new ExpressionType(col1));
                }
                if (defaultExpr != null
                        && !columnNames.contains(col1.getColName())) {
                    appendColumnDescription(col1.getColName(), false,
                            col1.getColLength(), col1.isNullable(), defaultExpr);
                    sbColumns.append(col1.getColName()).append(",");
                    hasDefaults = true;
                }
            }
            sbColumns.setLength(sbColumns.length() - 1);
            columninfo = ParseCmdLine.substitute(
                    Props.XDB_LOADER_NODEWRITER_COLUMNINFO,
                    Collections.singletonMap("columns", sbColumns.toString()));
        }
        SysColumn partCol = table.getPartitionedColumn();
        this.partColumn = -1;
        if (partCol != null) {
            hashDataType = new ExpressionType(partCol);
            for (int idx = 0; idx < columns.size(); idx++) {
                if (partCol.getColName().equals(columns.get(idx).getName())) {
                    this.partColumn = idx;
                    break;
                }
            }
        }
        this.client = client;
    }

    /**
     * Initialize fields: handle(optional) dbName tableName columns partColumn
     * partitionMap nodes nodeInfos
     */
    public void setTargetTableInfoExt(String tableName, int partColumn,
            int[] groupHashList, PartitionMap partMap,
            ExpressionType hashDataType,
            int serialColumnPosition, SysSerialGenerator serialGenerator,
            boolean isDestTypeNodeId, boolean suppressSendingNodeId,
            NodeDBConnectionInfo[] nodeInfos) {
        this.tableName = tableName;
        this.groupHashList = groupHashList;
        this.partColumn = partColumn;
        this.partitionMap = partMap;
        this.serialColumnPosition = serialColumnPosition;
        this.isDestTypeNodeId = isDestTypeNodeId;
        this.suppressSendingNodeId = suppressSendingNodeId;
        this.nodeInfos = nodeInfos;
        this.hashDataType = hashDataType;
        this.setSerialProvider(new SerialGeneratorProvider(serialGenerator));
    }

    private void appendColumnDescription(String colName, boolean isSerial,
            int length, boolean isNullable, SqlExpression defaultExpr) {
        if (columns == null) {
            columns = new LinkedList<TableColumnDescription>();
        }
        TableColumnDescription column = new TableColumnDescription();
        column.setName(colName);
        column.setSerial(isSerial);
        column.setLength(length);
        column.setNullable(isNullable);
        column.setDefault(defaultExpr);
        columns.add(column);
    }

    public void setGenerateSerial(boolean value) throws Exception {
        serialColumn = -1;
        if (value && columns != null) {
            int columnIndex = 0;
            Iterator<TableColumnDescription> it = columns.iterator();

            findSerialColumnIndexLoop: while (it.hasNext()) {
                if (it.next().isSerial()) {
                    serialColumn = columnIndex;
                    break findSerialColumnIndexLoop;
                }
                columnIndex++;
            }
        }
    }

    public void setVerbose(boolean verbose) {
    }

    public boolean getGenerateSerial() {
        return serialColumn >= 0;
    }

    public boolean getGenerateRowID() {
        return xrowidColumn >= 0;
    }

    /**
     * Param should specify WriterFactory Ignored while we have only one
     * WriterFactory
     *
     * @param address
     * @throws Exception
     */
    public void prepareLoad() throws Exception {
        if (Props.USE_JDBC_COPY_API) {
            writerFactory = new PostgresWriterFactory();
        } else {
            writerFactory = new DefaultWriterFactory();
        }
        if (nodeInfos != null) {
            writerFactory.setNodeConnectionInfos(nodeInfos);
        }

        HashMap<String, String> propertyMap = new HashMap<String, String>();
        propertyMap.put("table", IdentifierHandler.quote(tableName));
        String separator = null;
        String headerTemplate = Property.get(HEADER_TEMPLATE_KEY);
        if (headerTemplate != null && headerTemplate.length() > 0) {
            if (columns != null) {
                separator = Property.get(HEADER_COLUMN_SEPARATOR_KEY, ",");
                putColumnsToMap(separator, propertyMap);
            }
            propertyMap.put("outheader", ParseCmdLine.substitute(headerTemplate, propertyMap));
        }
        String footerTemplate = Property.get(FOOTER_TEMPLATE_KEY);
        if (footerTemplate != null && footerTemplate.length() > 0) {
            String footerSeparator = Property.get(FOOTER_COLUMN_SEPARATOR_KEY,
            ",");
            if (columns != null && !footerSeparator.equals(separator)) {
                separator = footerSeparator;
                putColumnsToMap(separator, propertyMap);
            }
            propertyMap.put("outfooter", ParseCmdLine.substitute(footerTemplate, propertyMap));
        }
        String rowSeparator = Property.get(ROW_COLUMN_SEPARATOR_KEY, ",");
        if (columns != null && !rowSeparator.equals(separator)) {
            separator = rowSeparator;
            putColumnsToMap(separator, propertyMap);
        }
        propertyMap.put("columninfo", columninfo);
        propertyMap.put("delimiter", ParseCmdLine.escape("" + this.separator,
                -1, false));
        propertyMap.put("null", ParseCmdLine.escape(valueNULL, -1, false));
        propertyMap.put("quote", ParseCmdLine.escape("" + quoteChar, -1, false));
        propertyMap.put("escape", ParseCmdLine.escape("" + quoteEscape, -1, false));
        propertyMap.put("forcenotnullinfo", forcenotnullinfo);
        writerFactory.setParams(dataSource, propertyMap);
    }

    /**
     * @param separator
     * @param propertyMap
     */
    private void putColumnsToMap(String separator,
            HashMap<String, String> propertyMap) {
        StringBuilder sb = new StringBuilder(1024);
        Iterator<TableColumnDescription> it = columns.iterator();

        while (it.hasNext()) {
            // String colName = ((ColumnDescription) it.next()).name;
            // propertyMap.put("column." + i, colName);
            // sb.append(colName).append(separator);
            // columnIndex++ ;
            sb.append(it.next().getName()).append(separator);
        }
        sb.setLength(sb.length() - separator.length()); // TODO: check this line
        // is serving the
        // purpose as intended.
        propertyMap.put("column_list", sb.toString());
    }

    /**
     * @return Returns the quoteEscapeSymbol.
     */
    public char getQuoteEscape() {
        return quoteEscape;
    }

    /**
     * @return Returns the quoteSymbol.
     */
    public char getQuoteChar() {
        return quoteChar;
    }

    public void setPartitionMap(PartitionMap partitionMap) {
        this.partitionMap = partitionMap;
    }

    public void setRowIdProvider(IUniqueValueProvider rowIdProvider) {
        this.rowIdProvider = rowIdProvider;
    }

    public void setSerialProvider(IUniqueValueProvider serialProvider) {
        this.serialProvider = serialProvider;
    }

    //
    // Getter methods for so that data processor instances could determine what
    // data loader configuration is.
    //

    public Loader getLoaderInstance() {
        return this;
    }

    public INodeWriterFactory getWriterFactory() {
        return writerFactory;
    }

    public PartitionMap getPartitionMap() {
        return partitionMap;
    }

    public XDBSessionContext getClient() {
        return client;
    }

    public SysTable getTableInformation() {
        return table;
    }

    public List<TableColumnDescription> getTableColumnsInformation() {
        return columns;
    }

    public IUniqueValueProvider getRowIdProvider() {
        return rowIdProvider;
    }

    public IUniqueValueProvider getSerialProvider() {
        return serialProvider;
    }

    public char getSeparator() {
        return separator;
    }

    public boolean noParse() {
        return serialColumn < 1 && xrowidColumn < 1 && serialColumnPosition < 1
                && !hasDefaults;
    }

    public boolean destinationTypeNodeId() {
        return isDestTypeNodeId;
    }

    public boolean suppressSendingNodeId() {
        return suppressSendingNodeId;
    }

    public int getTableColumnsCount() {
        try {
            if (inputRS != null) {
                return inputRS.getMetaData().getColumnCount();
            }
            if (copyOut != null) {
            	return copyOut.getFieldCount();
            }
            if (columns != null) {
            	return columns.size();
            }
        } catch (SQLException ex) {
        }
        return -1;
    }

    public int getSerialColumnPosition() {
        return serialColumnPosition;
    }

    public int getSerialColumnSequence() {
        return serialColumn;
    }

    public int getPartitionColumnSequence() {
        return partColumn;
    }

    public int getXRowidColumnSequence() {
        return xrowidColumn;
    }

    public String getNULLValue() {
        return valueNULL;
    }

    private DATA_SOURCE dataSource;

    public void runWriters() throws Exception {

        if (inputRS != null && partColumn >= 0 && hashDataType == null) {
            hashDataType = new ExpressionType();
            ResultSetMetaData rsmd = inputRS.getMetaData();
            hashDataType.setExpressionType(rsmd.getColumnType(partColumn + 1),
                    rsmd.getColumnDisplaySize(partColumn + 1),
                    rsmd.getPrecision(partColumn + 1),
                    rsmd.getScale(partColumn + 1));
        }

        Future<Boolean> drHandle;
        int dataProcCount = dataSource == DATA_SOURCE.CSV ? 1 : Props.XDB_LOADER_DATAPROCESSORS_COUNT;
        dpThreads = new DataProcessorThread[dataProcCount];
        Future<Integer>[] dpHandles = new Future[dataProcCount];
        /*
         * Tasks: - Determine the number of data-processor-threads we want
         * to run parallel. - Instantiate the data-reader-thread. - Call the
         * procedure to start the data reading and data processing tasks.
         */
        if (dataSource == DATA_SOURCE.RESULT_SET) {
            /*
             * Instantiate data buffer instance which will be shared b/w
             * data-reader-thread and data-processors-thread where
             * data-reader-thread will act as a 'producer' and
             * data-processors-thread will act as 'consumer'.
             *
             * Note: (re)check whether the buffer size (=Math.max(100,
             * PARALLEL_PROCESSOR_COUNT*20)) is OK?
             */
            DataReaderAndProcessorBuffer<String[]> buffer = new DataReaderAndProcessorBuffer<String[]>(
                    Props.XDB_LOADER_DATAPROCESSOR_BUFFER, dataSource);
            /*
             * When the data source is result set, we want at most 2
             * data-processors-threads. (Currently it is hard coded; we can
             * be make it configurable.)
             */
            ResultSetReaderThread drThread = new ResultSetReaderThread(inputRS,
                    buffer, groupHashList);
            drHandle = executorService.submit(drThread);
            for (int i = 0; i < dpThreads.length; i++) {
                dpThreads[i] = new ResultSetProcessorThread(this, i + 1, buffer);
                dpThreads[i].initializeDataWriters();
                dpHandles[i] = executorService.submit(dpThreads[i]);
            }
        } else if (dataSource == DATA_SOURCE.COPY_OUT) {
            DataReaderAndProcessorBuffer<byte[]> buffer = new DataReaderAndProcessorBuffer<byte[]>(
                    Props.XDB_LOADER_DATAPROCESSOR_BUFFER, dataSource);
            CopyOutReaderThread drThread = new CopyOutReaderThread(copyOut,
                    buffer, groupHashList);
            drHandle = executorService.submit(drThread);
            for (int i = 0; i < dpThreads.length; i++) {
                dpThreads[i] = new TextProcessorThread(this, i + 1, buffer);
                dpThreads[i].initializeDataWriters();
                dpHandles[i] = executorService.submit(dpThreads[i]);
            }
     	
        } else if (inputStream != null) {
            /*
             * Instantiate data buffer instance which will be shared b/w
             * data-reader-thread and data-processors-thread where
             * data-reader-thread will act as a 'producer' and
             * data-processors-thread will act as 'consumer'.
             *
             * Note: (re)check whether the buffer size (=Math.max(100,
             * PARALLEL_PROCESSOR_COUNT*20)) is OK?
             */
            DataReaderAndProcessorBuffer<byte[]> buffer = new DataReaderAndProcessorBuffer<byte[]>(
                    Props.XDB_LOADER_DATAPROCESSOR_BUFFER, dataSource);
            /*
             * When the data source is file, determine the number of
             * data-processors-threads from the configuration file.
             */
            StreamReaderThread drThread = new StreamReaderThread(inputStream, buffer, dataSource == DATA_SOURCE.CSV);
            drHandle = executorService.submit(drThread);
            for (int i = 0; i < dpThreads.length; i++) {
                if (dataSource == DATA_SOURCE.CSV) {
                    dpThreads[i] = new CsvProcessorThread(this, i + 1, buffer);
                } else {
                    dpThreads[i] = new TextProcessorThread(this, i + 1, buffer);
                }
                dpThreads[i].initializeDataWriters();
                dpHandles[i] = executorService.submit(dpThreads[i]);
            }
        } else {
            throw new Exception("Source is not specified");
        }
        /*
         * Wait for all the data-processors-threads till they finish. Once they
         * are finished, get the number of rows they have processed and
         * determine the final figure.
         */
        for (Future<Integer> element : dpHandles) {
            try {
                rowCount += element.get();
            } catch (Exception ex) {
                logger.throwing(ex);
                throw ex;
            }
        }
        try {
            drHandle.get();
        } catch (Exception ex) {
            logger.throwing(ex);
            throw ex;
        }

        /*
         * Take the maximum value of maxSuppliedRowIDValue and
         * maxSuppliedSerialValue from data-processors-threads, and choose the
         * max. of all them for current loader instance.
         */
        long tempValue;
        for (DataProcessorThread element : dpThreads) {
            tempValue = element.getMaxSuppliedRowIDValue();
            if (tempValue > maxSuppliedRowIDValue) {
                maxSuppliedRowIDValue = tempValue;
            }
            tempValue = element.getMaxSuppliedSerialValue();
            if (tempValue > maxSuppliedSerialValue) {
                maxSuppliedSerialValue = tempValue;
            }
        }

        logger.info("Data processing has finished.");
        logger.log(Level.INFO, " %0% input rows have been handed off to Node Writers.",
                new Object[] {rowCount});
    }

    public void finishLoad(boolean success) throws Exception {
        Exception error = null;
        if (dpThreads != null) {
            try {
                /*
                 * Signal all the data-processors-thread to perform any cleanup
                 * task they have to perform once all threads are finished.
                 */
                for (DataProcessorThread element : dpThreads) {
                    try {
                    	if (element != null) {
                    		element.finishLoad(success);
                    	}
                    } catch (Exception ex) {
                        if (success) {
                            error = ex;
                            success = false;
                        }
                    }
                }
                if (success) {
                    for (DataProcessorThread element : dpThreads) {
                    	if (element != null) {
                    		element.commitLoad();
                    	}
                    }
                } else {
                    for (DataProcessorThread element : dpThreads) {
                    	if (element != null) {
                    		element.rollbackLoad();
                    	}
                    }
                }
            } finally {
                for (DataProcessorThread element : dpThreads) {
                	if (element != null) {
                		element.close();
                	}
                }
            }
        } else {
            try {
                for (INodeWriter writer : writers.values()) {
                    try {
                        writer.finish(success);
                    } catch (Exception e) {
                        if (success) {
                            error = e;
                            success = false;
                        }
                    }
                }
                if (success) {
                    for (INodeWriter writer : writers.values()) {
                    	if (writer != null) {
                    		writer.commit();
                    	}
                    }
                    initRowCountMap();
                } else {
                    for (INodeWriter writer : writers.values()) {
                    	if (writer != null) {
                    		writer.rollback();
                    	}
                    }
                }
            } finally {
                writerFactory.close();
            }
        }
        if (error != null) {
            throw error;
        }
    }

    public long getMaxSuppliedSerialValue() {
        return maxSuppliedSerialValue;
    }

    public long getMaxSuppliedRowIDValue() {
        return maxSuppliedRowIDValue;
    }

    private static class SerialGeneratorProvider implements
    IUniqueValueProvider {
        private SysSerialGenerator serialGenerator;

        private SerialGeneratorProvider(SysSerialGenerator serialGenerator) {
            this.serialGenerator = serialGenerator;
        }

        /*
         * (non-Javadoc)
         *
         * @see org.postgresql.stado.engine.loader.IUniqueValueProvider#getNextValue()
         */
        public String getNextValue() throws XDBGeneratorException {
            return "" + serialGenerator.allocateValue();
        }
    }

    /**
     * Initializes the row count map, to track how many rows each writer wrote.
     */
    public void initRowCountMap() {
        rowCountMap = new HashMap<Integer, Long>();

        for (Object element : writers.keySet()) {
            Integer nodeInt = (Integer) element;
            INodeWriter writer = writers.get(nodeInt);
            rowCountMap.put(nodeInt, writer.getRowCount());
        }
    }

    /**
     * @return a HashMap containing the number of rows each writer wrote, with
     *         the key being the node number.
     */
    public HashMap<Integer,Long> getRowCountMap() {
        if (dpThreads != null) {

            if (rowCountMap == null) {
                rowCountMap = new HashMap<Integer, Long>();
            }

            Integer nodeId = -1;
            Long tempRowcount = 0L;
            Long rowcount = 0L;

            Iterator<Integer> it;
            HashMap<Integer, Long> tempRowCountMap;

            for (DataProcessorThread element : dpThreads) {

                tempRowCountMap = element.getRowCountMap();
                it = tempRowCountMap.keySet().iterator();

                while (it.hasNext()) {
                    nodeId = it.next();
                    rowcount = tempRowCountMap.get(nodeId);

                    tempRowcount = rowCountMap.get(nodeId);
                    rowCountMap.put(nodeId, (tempRowcount == null ? rowcount
                            : tempRowcount + rowcount));
                }
            }
            return rowCountMap;
        } else {
            return null;
        }
    }

    public long getRowCount() {
        return rowCount;
    }

    /* (non-Javadoc)
     * @see org.postgresql.stado.engine.loader.ILoaderConfigInformation#getHashDataType()
     */
    public ExpressionType getHashDataType() {
        return hashDataType;
    }

    public void setForceNotNullColumns(Collection<String> colNames) {
        if (colNames == null || colNames.size() == 0) {
            forcenotnullinfo = Props.XDB_LOADER_NODEWRITER_CSV_FORCENOTNULLINFO_NONE;
        } else {
            StringBuffer sb = new StringBuffer();
            for (String colName : colNames) {
                sb.append(colName).append(",");
                for (TableColumnDescription tcd : columns) {
                    if (colName.equalsIgnoreCase(tcd.getName())) {
                        tcd.setNullable(false);
                        break;
                    }
                }
            }
            forcenotnullinfo = ParseCmdLine.substitute(
                    Props.XDB_LOADER_NODEWRITER_CSV_FORCENOTNULLINFO,
                    Collections.singletonMap("columns", sb.substring(0, sb.length() - 1)));
        }
    }

    public void cancel() {
        if (dpThreads != null) {
            for (DataProcessorThread element : dpThreads) {
                if (element != null) {
                    element.cancel();
                }
            }
        }
    }

}
