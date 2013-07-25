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
 * ResultSetResponse.java
 *
 *
 */

package org.postgresql.stado.engine.io;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.postgresql.stado.common.ColumnMetaData;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.datatypes.BigDecimalType;
import org.postgresql.stado.engine.datatypes.BlobType;
import org.postgresql.stado.engine.datatypes.BooleanType;
import org.postgresql.stado.engine.datatypes.ByteArrayType;
import org.postgresql.stado.engine.datatypes.ByteType;
import org.postgresql.stado.engine.datatypes.ClobType;
import org.postgresql.stado.engine.datatypes.DateType;
import org.postgresql.stado.engine.datatypes.FloatType;
import org.postgresql.stado.engine.datatypes.IntegerType;
import org.postgresql.stado.engine.datatypes.LongType;
import org.postgresql.stado.engine.datatypes.ShortType;
import org.postgresql.stado.engine.datatypes.TimeType;
import org.postgresql.stado.engine.datatypes.TimestampType;
import org.postgresql.stado.engine.datatypes.VarcharType;
import org.postgresql.stado.engine.datatypes.XData;


/**
 *
 *
 */
public class ResultSetResponse extends ResponseMessage {

    public static final byte LAST_PACKET_TRUE = 1;

    public static final byte LAST_PACKET_FALSE = 0;

    private static XLogger logger = XLogger.getLogger(ResultSetResponse.class);

    // since time is stored in millis, parsing the micro seconds separately
    private final SimpleDateFormat datetimeFormat = new SimpleDateFormat(
            "yyyy-MM-dd HH:mm:ss");// support up to microsecs

    private int columnCount = 0;

    private ColumnMetaData[] columnMeta = null;

    private List<XData[]> rows = null;

    private short packetSequence = 0;// valid ones start from 1

    private boolean isLastPacket = false;

    private boolean isFirstTime = true;// set to false if we have read

    private String resultSetId = null;// id assigned to this result set

    private int fetchSize = 200;// 00;//let's default to 100 rows

    private ResultSet rs = null;

    private int packetSeq = 2;

    private boolean exhaustedData = false;

    private long totalRows = 0;// debugging

    private boolean[] columnIsBinary;

    public ResultSetResponse() {

    }

    public ResultSetResponse(int requestId, String rsID, ResultSet rs) {
        super((byte) MessageTypes.RESP_QUERY_RESULTS, requestId);
        setResultSetId(rsID);
        setResultSet(rs);
    }

    public ColumnMetaData[] getColumnMetaData() {
        return this.columnMeta;
    }

    public void setColumnMetaData(ColumnMetaData[] columnMeta) {
        this.columnMeta = columnMeta;
    }

    public List<XData[]> getRows() {
        return rows;
    }

    /**
     * Is this the last packet?
     */
    public boolean isLastPacket() {
        return isLastPacket;
    }

    // unique for this connection
    public String getResultSetId() {
        return this.resultSetId;
    }

    // unique for this connection
    public void setResultSetId(String key) {
        this.resultSetId = key;
    }

    public ResultSet getResultSet() {
        return rs;
    }

    public void setResultSet(ResultSet rs) {
        this.rs = rs;
        if (rs != null) {
            try {
                ResultSetMetaData rsmd = rs.getMetaData();
                columnMeta = new ColumnMetaData[rsmd.getColumnCount()];
                for (int i = 0; i < columnMeta.length; i++) {
                    short flags = 0;
                    if (rsmd.isAutoIncrement(i + 1)) {
                        flags |= ColumnMetaData.IS_AUTO_INCREMENT;
                    }
                    if (rsmd.isCaseSensitive(i + 1)) {
                        flags |= ColumnMetaData.IS_CASE_SENSITIVE;
                    }
                    if (rsmd.isCurrency(i + 1)) {
                        flags |= ColumnMetaData.IS_CURRENCY;
                    }
                    if (rsmd.isDefinitelyWritable(i + 1)) {
                        flags |= ColumnMetaData.IS_WRITABLE;
                    }
                    if (rsmd.isNullable(i + 1) == ResultSetMetaData.columnNullable) {
                        flags |= ColumnMetaData.IS_NULLABLE;
                    }
                    if (rsmd.isReadOnly(i + 1)) {
                        flags |= ColumnMetaData.IS_READ_ONLY;
                    }
                    if (rsmd.isSearchable(i + 1)) {
                        flags |= ColumnMetaData.IS_SEARCHABLE;
                    }
                    if (rsmd.isSigned(i + 1)) {
                        flags |= ColumnMetaData.IS_SIGNED_NUM;
                    }
                    if (rsmd.isWritable(i + 1)) {
                        flags |= ColumnMetaData.IS_WRITABLE;
                    }

                    columnMeta[i] = new ColumnMetaData(rsmd
                            .getColumnName(i + 1), rsmd.getColumnLabel(i + 1),
                            rsmd.getColumnDisplaySize(i + 1), rsmd
                                    .getColumnType(i + 1), rsmd
                                    .getPrecision(i + 1), rsmd.getScale(i + 1),
                            rsmd.getTableName(i + 1), flags, true);
                }
            } catch (SQLException se) {
                logger.catching(se);
            }
        }
    }

    public void setFetchSize(int size) {
        if (size > 0) {
            this.fetchSize = size;
        }
    }

    public int getFetchSize() {
        return this.fetchSize;
    }

    /** grab the next set of results, up to "size" rows */
    public ResponseMessage nextResults(int size) throws SQLException {
        // InputStream is = null;

        logger.debug("in nexResults - fetch size requested: " + size);
        int maxRows = size > 0 ? size : fetchSize;
        ++packetSeq;

        ResultSetMetaData meta = rs.getMetaData();
        // format the bytes using the generic response msg
        ResponseMessage response = new ResponseMessage(
                (byte) MessageTypes.RESP_QUERY_RESULTS, 0);

        // init message size to avoid allocs
        response.initSize(32767);

        int count;
        if (columnIsBinary == null) {
            count = meta.getColumnCount();
        } else {
            count = columnIsBinary.length;
        }
        logger.debug("db serv, resultsetid=" + resultSetId + " ,col count="
                + count + ", packetSeq=" + packetSeq);

        // last_packet? 0=false; 1=true
        if (exhaustedData) {
            // shouldn't really be here..
            // logger.warn("setting last packet true");
            response.storeByte(ResultSetResponse.LAST_PACKET_TRUE);
        } else {
            response.storeByte(ResultSetResponse.LAST_PACKET_FALSE);
        }
        response.storeString(resultSetId);
        response.storeShort(packetSeq);// packet sequence
        response.storeShort(count);// column counts

        if (columnIsBinary == null) {
            // First packet
            // log.debug("first packet, setting metadata");
            setMeta(response, meta);
        }

        // now populate row data
        int soFar = 0;
        boolean reachedMax = false;
        while (rs.next()) {
            totalRows++;
            response.storeByte((byte) 1);// more data in this packet?
            for (int i = 1; i <= count; i++) {
                if (columnIsBinary[i - 1]) {
                    response.storeBytes(rs.getBytes(i));
                } else {
                    response.storeString(rs.getString(i));
                }
            }
            if (++soFar >= maxRows) {
                // log.debug("reached specified fetchsize, save for next
                // packet");
                reachedMax = true;
                break;
            }
        }
        response.storeByte((byte) 0);// no more data in this packet?

        if (!reachedMax) {
            exhaustedData = true;
            response.getMessage()[0] = ResultSetResponse.LAST_PACKET_TRUE;
        }

        logger.debug("total rows read so far " + totalRows + ", total msg len="
                + response.getMessage().length);

        return response;
    }

    // grab the metadata and format to stream data
    private void setMeta(ResponseMessage res, ResultSetMetaData meta)
            throws SQLException {
        logger.debug("begin setMeta");
        int count = meta.getColumnCount();

        short flags = 0;

        // TODO: Need to handle IS_PRIMARY_KEY flag
        columnIsBinary = new boolean[count];
        for (int i = 1; i <= count; i++) {
            if (resultSetId == null) {
                // this is node result set put dummies
                res.storeString(null);
                res.storeString(null);
                res.storeString(null);
                res.storeShort(0);
                // Only datatype is significant
                int javaType = meta.getColumnType(i);
                res.storeShort((short) javaType);
                columnIsBinary[i - 1] = DataTypes.isBinary(javaType);
                res.storeShort(0);
                res.storeShort(0);
                res.storeShort(0);
            } else {
                if (meta.isAutoIncrement(i)) {
                    flags |= ColumnMetaData.IS_AUTO_INCREMENT;
                }
                if (meta.isCaseSensitive(i)) {
                    flags |= ColumnMetaData.IS_CASE_SENSITIVE;
                }
                if (meta.isCurrency(i)) {
                    flags |= ColumnMetaData.IS_CURRENCY;
                }
                if (meta.isDefinitelyWritable(i)) {
                    flags |= ColumnMetaData.IS_WRITABLE;
                }
                if (meta.isNullable(i) == ResultSetMetaData.columnNullable) {
                    flags |= ColumnMetaData.IS_NULLABLE;
                }
                if (meta.isReadOnly(i)) {
                    flags |= ColumnMetaData.IS_READ_ONLY;
                }
                if (meta.isSearchable(i)) {
                    flags |= ColumnMetaData.IS_SEARCHABLE;
                }
                if (meta.isSigned(i)) {
                    flags |= ColumnMetaData.IS_SIGNED_NUM;
                }
                if (meta.isWritable(i)) {
                    flags |= ColumnMetaData.IS_WRITABLE;
                }

                res.storeString(meta.getTableName(i));
                res.storeString(meta.getColumnName(i));
                res.storeString(meta.getColumnLabel(i));
                res.storeShort((short) meta.getColumnDisplaySize(i));
                int javaType = meta.getColumnType(i);
                res.storeShort((short) javaType);
                columnIsBinary[i - 1] = DataTypes.isBinary(javaType);
                res.storeShort(flags);
                // MonetDB driver does not implement those methods (exception is
                // thrown)
                short precision = -1;
                try {
                    precision = (short) meta.getPrecision(i);
                } catch (SQLException ignore) {
                }
                res.storeShort(precision);

                short scale = -1;
                try {
                    scale = (short) meta.getScale(i);
                } catch (SQLException ignore) {
                }
                res.storeShort(scale);

                logger.debug("table name="
                        + meta.getTableName(i)
                        + ",columnName="
                        + meta.getColumnName(i)
                        + ",columnLabel="
                        + meta.getColumnLabel(i)
                        + ",precision="
                        + precision
                        + ",java type="
                        + org.postgresql.stado.engine.io.DataTypes.getJavaTypeDesc(meta
                                .getColumnType(i)) + ",column size="
                        + meta.getColumnDisplaySize(i) + ",scale=" + scale);
            }
        }
        logger.debug("end META");
    }

    // reset: isLastPacket, packetSequence, columnCount
    private void reset() throws SQLException {
        // 1st
        boolean lastPacket = readByte() == LAST_PACKET_TRUE ? true : false;
        // 2nd
        this.resultSetId = readString();

        if (lastPacket) {
            // this way we can't go from true to false
            this.isLastPacket = lastPacket;
        }
        short seq = readShort();
        short cnt = readShort();

        if (isFirstTime || seq == packetSequence + 1 || seq == Short.MIN_VALUE
                && packetSequence == Short.MAX_VALUE) {
            this.packetSequence = seq;
        } else {
            throw new SQLException("invalid packet sequence. last=" + seq
                    + ", curr=" + packetSequence);
        }
        if (cnt < 1) {
            throw new SQLException("invalid column count=" + cnt);
        }
        if (isFirstTime) {
            columnCount = cnt;
        }
    }

    // column names will show on the first packet only
    private void readColumnNames() throws SQLException {
        // log.debug("reading column names, total column count: " +
        // columnCount);
        this.columnMeta = new ColumnMetaData[columnCount];

        for (int i = 0; i < columnCount; i++) {
            String tableName = readString();
            String column = readString();
            String alias = readString();
            // log.debug("doing column name '" + columns[i] + "'");

            short len = readShort();
            short type = readShort();
            short flags = readShort();
            short decimalPos = readShort();
            short scale = readShort();

            ColumnMetaData meta = new ColumnMetaData(column,// name
                    alias,// alias
                    len, type, decimalPos, scale, tableName, flags, true);
            this.columnMeta[i] = meta;
        }
    }

    // read data from inputstream. If no more data, set rows = null and
    // isLastPacket = true
    public void resetRows() throws SQLException {

        // log.info("in resetRows");
        if (getType() != MessageTypes.RESP_QUERY_RESULTS) {
            logger.warn("err response detected, type=" + getType());
            this.isLastPacket = true;
            throw new SQLException("invalid operation");// should never be here
        }

        // this shouldn't get called since we should check for lastpacket
        // beforehand, but heck
        if (isLastPacket) {
            // log.debug("resetRows ignored, is last packet");
            this.rows = null;
            return;
        }

        try {
            if (isFirstTime) {
                // log.debug("first time, reading column data");
                // init();
                reset();
                readColumnNames();
                isFirstTime = false;
            } else {
                reset();
            }
            // log.debug("Start reading raw row data");
            // now let's read the row data
            List<XData[]> list = new ArrayList<XData[]>();
            while (hasMoreDataToRead() && readByte() != 0) {

                String currentStr = null;
                XData[] row = new XData[columnCount];
                for (int idx = 0; idx < columnCount; idx++) {
                    XData data = null;
                    if (!DataTypes.isBinary(columnMeta[idx].javaSqlType)) {
                        currentStr = readString();
                    }
                    try {
                        switch (columnMeta[idx].javaSqlType) {
                        case Types.CHAR:
                            // Same as VARCHAR
                            // data = new CharType(currentStr);
                            // break;
                        case Types.VARCHAR:
                            data = new VarcharType(currentStr);
                            break;
                        case Types.DATE:
                            // string passed to driver is 2007-06-01
                            // hours, minutes, secs...will be set to zero
                            java.sql.Date dt = null;
                            if (currentStr != null) {
                                dt = new java.sql.Date(datetimeFormat.parse(
                                        currentStr + " 00:00:00").getTime());
                            }
                            data = new DateType(dt);
                            break;
                        case Types.FLOAT:
                        case Types.REAL:
                        case Types.DOUBLE:
                            data = new FloatType(currentStr);
                            data.setJavaType(columnMeta[idx].javaSqlType);
                            break;
                        case Types.INTEGER:
                            data = new IntegerType(currentStr);
                            break;
                        case Types.BIGINT:
                            data = new LongType(currentStr);
                            break;
                        case Types.DECIMAL:// nolimit
                            data = new BigDecimalType(currentStr);
                            data.setJavaType(Types.DECIMAL);
                            break;
                        case Types.NUMERIC:// nolimit
                            data = new BigDecimalType(currentStr);
                            data.setJavaType(Types.NUMERIC);
                            break;
                        case Types.SMALLINT:// 2 bytes
                            data = new ShortType(currentStr);
                            break;
                        case Types.TIMESTAMP:
                            // log.debug("reading timestamp");
                            java.sql.Timestamp ts = null;
                            String tzStr = null;
                            if (currentStr != null) {
                                ParsePosition pp = new ParsePosition(0);
                                ts = new java.sql.Timestamp(datetimeFormat
                                        .parse(currentStr, pp).getTime());
                                int i = pp.getIndex();
                                if (i < currentStr.length() && currentStr.charAt(i) == '.') {
                                    i++;
                                    int nanos = 0;
                                    for (int j = 0; j < 9; j++) {
                                        nanos *= 10;
                                        if (currentStr.charAt(i) >= '0' && currentStr.charAt(i) <= '9') {
                                            nanos += currentStr.charAt(i++) - '0';
                                        }
                                    }
                                    ts.setNanos(nanos);
                                }
                                tzStr = currentStr.substring(i);
                            }
                            data = new TimestampType(ts, tzStr);
                            break;
                        case Types.TIME:
                            // hh:mm:ss
                            java.sql.Time time = null;
                            tzStr = null;
                            if (currentStr != null) {
                                ParsePosition pp = new ParsePosition(0);
                                time = new java.sql.Time(datetimeFormat.parse(
                                        "1970-01-01 " + currentStr, pp).getTime());
                                tzStr = currentStr.substring(pp.getIndex() - 11);
                            }
                            data = new TimeType(time, tzStr);
                            break;
                        case Types.BOOLEAN:
                            data = new BooleanType(currentStr);
                            break;
                        case Types.TINYINT:
                            data = new ByteType(currentStr);
                            break;
                        case Types.BLOB:
                            int size = readInt();
                            data = new BlobType(readBytes(size));
                            break;
                        case Types.CLOB:
                            data = new ClobType(currentStr);
                            break;
                        case Types.BIT:
                        case Types.BINARY:
                        case Types.LONGVARBINARY:
                        case Types.VARBINARY:
                            size = readInt();
                            data = new ByteArrayType(readBytes(size));
                            data.setJavaType(columnMeta[idx].javaSqlType);
                            break;
                        case Types.OTHER:
                        case Types.NULL:
                        case java.sql.Types.ARRAY:
                        default:
                            data = new VarcharType(currentStr);
                        }
                    } catch (Exception e) {
                        // If parse is failed use Varchar as generic type
                        data = new VarcharType(currentStr);
                    }
                    row[idx] = data;
                    if (currentStr != null) {
                        columnMeta[idx].maxLength = currentStr.length() > columnMeta[idx].maxLength ? currentStr
                                .length()
                                : columnMeta[idx].maxLength;
                    }
                }// for
                list.add(row);
            }
            this.rows = list;
            // log.debug(list.size() + " rows read");
        } catch (Throwable t) {
            logger.catching(t);
            throw new SQLException(t.getMessage());
        }
        // log.debug("end of resetRows()");
    }

}
