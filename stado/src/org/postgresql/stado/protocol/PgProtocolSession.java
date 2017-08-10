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
/**
 *
 */
package org.postgresql.stado.protocol;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Level;
import org.postgresql.stado.common.ColumnMetaData;
import org.postgresql.stado.common.CommandLog;
import org.postgresql.stado.common.util.Property;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.engine.io.CopyResponse;
import org.postgresql.stado.engine.io.MessageTypes;
import org.postgresql.stado.engine.io.ResponseMessage;
import org.postgresql.stado.engine.io.ResultSetResponse;
import org.postgresql.stado.exception.XDBServerException;


/**
 *
 *
 */
public class PgProtocolSession implements Runnable {

    private static final XLogger logger = XLogger.getLogger(PgProtocolSession.class);

    protected static final String CHARSET_NAME = Property.get("xdb.charset",
            "ISO-8859-1");

    private static final int PROTOCOL_MAJOR = 3;

    private static final int PROTOCOL_MINOR = 0;

    static final int SESSION_STATE_DISCONNECTED = 0;

    static final int SESSION_STATE_HANDSHAKE = 1;

    static final int SESSION_STATE_READY = 2;

    static final int SESSION_STATE_QUERY = 3;

    static final int MAX_ERROR_MSG_SIZE = 511;

    private PgProtocolHandler manager;

    private XDBSessionContext client;

    private SocketChannel channel;

    private int sessionState = SESSION_STATE_DISCONNECTED;

    private boolean error = false;

    private int fetchSize = 0;

    private int backendKey = 0;
    
    private String connUser = null;
    
    private String connDatabase = null;

    private String FormatPgErrorMsg(String msg) {
    	String m = "";
    	
    	if (msg == null) 
    		return "Error";
    	
        if (msg.length() > MAX_ERROR_MSG_SIZE)
        {
            m = msg.substring(0,MAX_ERROR_MSG_SIZE - 3) + "...";
        } else
        {
        	m = msg;
        }

        // Split up the lines so we can handle them individually
    	String lines[] = m.split("\n");

    	// Remove the ERROR: prefix since we're going to put it back when we 
    	// generate the error message later
    	String f = lines[0].replaceFirst("ERROR: ", "");

    	// Include the HINT in the message
    	if (lines.length > 1) {
	    	if (lines[1].trim().startsWith("Hint:")) {
	    		f = f + "\n" + lines[1].trim().replaceFirst("Hint:", "HINT: ");
	    	}
    	}
    	
    	String marker = "org.postgresql.driver.util.PSQLException :";
    	int h = f.indexOf(marker);
    	if (h >=0) {
    		f = f.substring(h + marker.length() + 1);
    	}
    	
    	return f;
    }

    private String FormatPgValue(Object o) {
    	if (o == null) 
    		return null;
    	
    	// PostgreSQL returns a t or f over the wire for booleans not the 
    	// Java Boolean values of true or false
    	if (o instanceof Boolean) {
    		if ((Boolean)o == true) 
    			return "t";
    		else if ((Boolean)o == false)
    			return "f";
    	}
    	
    	// PostgreSQL returns the integer value over the wire if a float
    	// is really an integer ie 1.0 returns 1
    	if (o instanceof Double) { 
    		if (((Double)o).intValue() == ((Double)o).doubleValue())
    			return ((Integer)((Double)o).intValue()).toString();
    		else {
    			// format the result because PostgreSQL will only return 15 digits of precision
    			BigDecimal bd = BigDecimal.valueOf(((Double)o));
    			return removeTrailingZeros(bd.round(new MathContext(15)).toString()); 
    		}
    	}
    	
    	if (o instanceof byte[]) {
    		return new String(((byte[])o));
    	}
    	
    	return o.toString();
    }

    private String removeTrailingZeros(String str) {
    	if (str == null)
    		return null;
    	
    	char[] chars = str.toCharArray();int length,index ;length = str.length();
    	index = length -1;
    	for (; index >=0;index--) {
    		if (chars[index] != '0') {
    			break;
    		}
    	}
    	return (index == length-1) ? str :str.substring(0,index+1);
    }
    

    private PgProtocolMessage readRequest() throws IOException {
        ByteBuffer bbuf;
        byte msgType = PgProtocolMessage.MESSAGE_TYPE_INITIAL;
        if (sessionState == SESSION_STATE_DISCONNECTED) {
            bbuf = ByteBuffer.allocate(4);
            // read the header
            int totalRead = 0;
            do {
                int bytesRead = channel.read(bbuf);
                if (bytesRead == -1) {
                    // logger.debug("DNG: -1 bytesRead for header - return.");
                    throw new IOException("Connection is broken");
                }
                if (bytesRead == 0 && totalRead == 0) {
                    return null;
                }
                totalRead += bytesRead;
            } while (totalRead < 4);
            bbuf.flip();
        } else {
            bbuf = ByteBuffer.allocate(5);
            // read the header
            int totalRead = 0;
            do {
                int bytesRead = channel.read(bbuf);
                if (bytesRead == -1) {
                    throw new IOException("Connection is broken");
                }
                if (bytesRead == 0 && totalRead == 0) {
                    return null;
                }
                totalRead += bytesRead;
            } while (totalRead < 5);
            bbuf.flip();
            msgType = bbuf.get();
        }
        int msgSize = ((bbuf.get() & 0xff) << 24 | (bbuf.get() & 0xff) << 16
                | (bbuf.get() & 0xff) << 8 | bbuf.get() & 0xff) - 4;
        byte[] msgBody;
        if (msgSize > 0) {
            ByteBuffer dataBuff;
            try {
                dataBuff = ByteBuffer.allocate(msgSize);
            } catch (OutOfMemoryError e) {
                // Probably we are out of sync and receiving garbage
                logger.catching(e);
                channel.close();
                PgProtocolHandler.getProtocolManager().removeClient(channel);
                throw new IOException(
                        "Probably channel is out of sync and receiving garbage");
            }
            int totalRead = 0;
            do {
                int bytesRead = channel.read(dataBuff);
                if (bytesRead == -1) {
                    throw new IOException("Connection is broken");
                }
                totalRead += bytesRead;
            } while (totalRead < msgSize);
            msgBody = dataBuff.array();
        } else {
            msgBody = new byte[0];
        }
        return new PgProtocolMessage(msgType, msgBody);
    }

    /**
     *
     * @param rm
     * @param channel
     * @throws java.io.IOException
     */
    static void writeToChannel(PgProtocolMessage msg, SocketChannel channel) throws IOException {
        logger.log(Level.DEBUG, "Writing response: %0%", new Object[] {msg.getMessageType()});
        ByteBuffer aByteBuffer = msg.getAsByteBuffer(null);
        synchronized (channel) {
            do {
                channel.write(aByteBuffer);

                // Make sure we wrote everything. If not, wait and try again.
                // On next write, it will only write the part of the buffer
                // not yet written.
                if (aByteBuffer.remaining() > 0) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                    }
                    continue;
                }
                break;
            } while (true);
        }
    }

    PgProtocolSession(PgProtocolHandler manager, SocketChannel channel,
            XDBSessionContext client) throws Exception {
        this.manager = manager;
        this.channel = channel;
        this.client = client;
        channel.configureBlocking(true);
    }

    void close() {
        if (client != null) {
            try {
                client.close();
            } catch (Throwable t) {
                logger.error("Session has not been closed properly: "
                        + client.getSessionID());
                logger.catching(t);
            }
        }
    }

    XDBSessionContext getClient() {
        return client;
    }

    void setState(int newState) throws IOException {
        sessionState = newState;
        if (newState == SESSION_STATE_READY) {
            PgProtocolMessage ready = new PgProtocolMessage(
                    PgProtocolMessage.MESSAGE_TYPE_READY_FOR_QUERY,
                    new byte[] { client.isInTransaction() ? (byte) 'T'
                            : (byte) 'I' });
            writeToChannel(ready, channel);
        }
    }

    /**
     * Convert response to protocol message(s) and send them back to client
     * @param response
     * @return true if response is completely sent, false if it is suspended
     * @throws IOException
     */
    private boolean writeResponse(ExecutionResult response) throws IOException {
        switch (response.getContentType()) {
        case ExecutionResult.CONTENT_TYPE_EXCEPTION:
            Exception ex = response.getException();
            PgProtocolMessage error = new PgProtocolMessage(
                    PgProtocolMessage.MESSAGE_TYPE_ERROR_RESPONSE, null);
            // Severity - error
            error.putInt8('S');
            error.putString("ERROR");
            // SQLSTATE - internal error
            error.putInt8('C');
            error.putString("XX000");
            // Message
            error.putInt8('M');
            error.putString(FormatPgErrorMsg(ex.getMessage()));
            // End of parameter list marker
            error.putInt8(0);
            writeToChannel(error, channel);
            break;
        case ExecutionResult.CONTENT_TYPE_ROWCOUNT:
            PgProtocolMessage m1 = new PgProtocolMessage(
                    PgProtocolMessage.MESSAGE_TYPE_COMMAND_COMPLETE,
                    new byte[] {});
            if (response.getKind() == ExecutionResult.COMMAND_INSERT_TABLE) {
                m1.putString("INSERT 0 " + response.getRowCount());
            } else if (response.getKind() == ExecutionResult.COMMAND_UPDATE_TABLE) {
                m1.putString("UPDATE " + response.getRowCount());
            } else if (response.getKind() == ExecutionResult.COMMAND_DELETE_TABLE) {
                m1.putString("DELETE " + response.getRowCount());
            } else if (response.getKind() == ExecutionResult.COMMAND_COPY_IN
                    || response.getKind() == ExecutionResult.COMMAND_COPY_OUT) {
                m1.putString("COPY " + response.getRowCount());
            } else {
                m1.putString("OK " + response.getRowCount());
            }
            writeToChannel(m1, channel);
            break;
        case ExecutionResult.CONTENT_TYPE_EMPTY:
            if (response.getKind() == ExecutionResult.COMMAND_EMPTY_QUERY) {
                writeToChannel(
                        PgProtocolMessage.MSG_EMPTY_QUERY_RESPONSE,
                        channel);
            } else {
                PgProtocolMessage m2 = new PgProtocolMessage(
                        PgProtocolMessage.MESSAGE_TYPE_COMMAND_COMPLETE,
                        new byte[] {});
                switch (response.getKind()) {
            	case ExecutionResult.COMMAND_BEGIN_TRAN:
            		m2.putString("BEGIN");
            		break;
            	case ExecutionResult.COMMAND_COMMIT_TRAN:
            		m2.putString("COMMIT");
            		break;
            	case ExecutionResult.COMMAND_ROLLBACK_TRAN:
            		m2.putString("ROLLBACK");
            		break;
            	case ExecutionResult.COMMAND_CREATE_TABLE:
            		m2.putString("CREATE TABLE");
            		break;            		
            	case ExecutionResult.COMMAND_DROP_TABLE:
            		m2.putString("DROP TABLE");
            		break;            		
            	case ExecutionResult.COMMAND_CREATE_INDEX:
            		m2.putString("CREATE INDEX");
            		break;            		
            	case ExecutionResult.COMMAND_SET:
            		m2.putString("SET");
            		break;      
            	case ExecutionResult.COMMAND_DECLARE_CURSOR:
            		m2.putString("DECLARE CURSOR");
            		break;                  		
            	case ExecutionResult.COMMAND_CLOSE_CURSOR:
            		m2.putString("CLOSE CURSOR");
            		break;                  		
            	default:
            		m2.putString("OK");
                }
                writeToChannel(m2, channel);
            }
            break;
        case ExecutionResult.CONTENT_TYPE_SUBRESULTS:
            sendSubResults(response);
            break;
        case ExecutionResult.CONTENT_TYPE_RESULTSET:
            ResultSet rs = response.getResultSet();
            int rowCount = 0;
            try {
                int count = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    PgProtocolMessage msg = new PgProtocolMessage(
                            PgProtocolMessage.MESSAGE_TYPE_DATA_ROW,
                            new byte[] {});
                    msg.putInt16(count);
                    for (int i = 1; i <= count; i++) {
                    	if (!(rs.getObject(i) instanceof byte[])) {	                    		
	                        String value =  FormatPgValue(rs.getObject(i));
	                        if (value == null) {
	                            msg.putInt32(-1);
	                        } else {
	                            byte[] encoded = value.getBytes(CHARSET_NAME);
	                            msg.putInt32(encoded.length);
	                            msg.putBytes(encoded);
	                        }
                    	} else {
                    		byte[] b = (byte[]) rs.getObject(i);
	                        if (b == null) {
	                            msg.putInt32(-1);
	                        } else {
	                            msg.putInt32(b.length);
	                            msg.putBytes(b);
	                        }
                    	}
                    }
                    writeToChannel(msg, channel);
                    if (++rowCount == fetchSize && fetchSize > 0) {
                        writeToChannel(PgProtocolMessage.MSG_PORTAL_SUSPENDED,
                                channel);
                        return false;
                    }
                }
            } catch (SQLException e) {
                error = new PgProtocolMessage(
                        PgProtocolMessage.MESSAGE_TYPE_ERROR_RESPONSE, null);
                // Severity - error
                error.putInt8('S');
                error.putString(e.getMessage());
                // SQLSTATE - internal error
                error.putInt8('C');
                error.putString("XX000");
                // Message
                error.putInt8('M');
                error.putString(response.getException().getMessage());
                // End of parameter list marker
                error.putInt8(0);
                writeToChannel(error, channel);
            }
            m1 = new PgProtocolMessage(
                    PgProtocolMessage.MESSAGE_TYPE_COMMAND_COMPLETE,
                    new byte[] {});
            m1.putString("FETCH " + rowCount);
            writeToChannel(m1, channel);
            break;
        }
        return true;
    }

    /**
     * @param response
     * @throws UnsupportedEncodingException
     * @throws IOException
     */
    private void sendSubResults(ExecutionResult response)
            throws UnsupportedEncodingException, IOException {
        PgProtocolMessage error;
        PgProtocolMessage m1;
        boolean first = true;
        int rowCount = 0;
        ResultSet rs = null;
        for (Map.Entry<Integer,ExecutionResult> subResult : response.getSubResults().entrySet()) {
            try {
                // Validate
                if (first) {
                    first = false;
                    if (subResult.getValue().getContentType() == ExecutionResult.CONTENT_TYPE_RESULTSET) {
                        rs = subResult.getValue().getResultSet();
                    }
                } else {
                    if (subResult.getValue().getContentType() == ExecutionResult.CONTENT_TYPE_RESULTSET) {
                        if (rs == null) {
                            throw new SQLException("Results on different nodes do not match");
                        }
                    } else {
                        if (rs != null) {
                            throw new SQLException("Results on different nodes do not match");
                        }
                    }
                }
                // Encode
                if (rs == null) {
                    PgProtocolMessage msg = new PgProtocolMessage(
                            PgProtocolMessage.MESSAGE_TYPE_DATA_ROW,
                            new byte[] {});
                    msg.putInt16(2);
                    String value = "" + subResult.getValue().getRowCount();
                    byte[] encoded = value.getBytes(CHARSET_NAME);
                    msg.putInt32(encoded.length);
                    msg.putBytes(encoded);
                    value = "" + subResult.getKey();
                    encoded = value.getBytes(CHARSET_NAME);
                    msg.putInt32(encoded.length);
                    msg.putBytes(encoded);
                    writeToChannel(msg, channel);
                    rowCount++;
                } else {
                    rs = subResult.getValue().getResultSet();
                    int count = rs.getMetaData().getColumnCount();
                    while (rs.next()) {
                        PgProtocolMessage msg = new PgProtocolMessage(
                                PgProtocolMessage.MESSAGE_TYPE_DATA_ROW,
                                new byte[] {});
                        msg.putInt16(count + 1);
                        for (int i = 1; i <= count; i++) {
                            String value = rs.getString(i);
                            if (value == null) {
                                msg.putInt32(-1);
                            } else {
                                byte[] encoded = value.getBytes(CHARSET_NAME);
                                msg.putInt32(encoded.length);
                                msg.putBytes(encoded);
                            }
                        }
                        String value = "" + subResult.getKey();
                        byte[] encoded = value.getBytes(CHARSET_NAME);
                        msg.putInt32(encoded.length);
                        msg.putBytes(encoded);
                        writeToChannel(msg, channel);
                        rowCount++;
                    }
                }
            } catch (SQLException e) {
                error = new PgProtocolMessage(
                        PgProtocolMessage.MESSAGE_TYPE_ERROR_RESPONSE, null);
                // Severity - error
                error.putInt8('S');
                error.putString(e.getMessage());
                // SQLSTATE - internal error
                error.putInt8('C');
                error.putString("XX000");
                // Message
                error.putInt8('M');
                error.putString(response.getException().getMessage());
                // End of parameter list marker
                error.putInt8(0);
                writeToChannel(error, channel);
            }
        }
        m1 = new PgProtocolMessage(
                PgProtocolMessage.MESSAGE_TYPE_COMMAND_COMPLETE,
                new byte[] {});
        m1.putString("FETCH " + rowCount);
        writeToChannel(m1, channel);
    }

    private boolean cancelRequest(int sessionID, int secretKey) {
        if (client.getSessionID() == sessionID && secretKey == backendKey) {
            client.kill();
            return true;
        } else {
            return false;
        }
    }
    
    /*
     * (non-Javadoc)
     *
     * @see java.lang.Runnable#run()
     */
    public void run() {
        try {
            PgProtocolMessage request = readRequest();
            while (request != null) {

                try {
                    logger.log(Level.DEBUG, "Handle message: %0%",
                            new Object[] { request.getMessageType() });
                    switch (request.getMessageType()) {
                    case PgProtocolMessage.MESSAGE_TYPE_INITIAL:
                        int protocolMajor = request.getInt16();
                        int protocolMinor = request.getInt16();
                        if (protocolMajor == 1234 && protocolMinor == 5678) {
                            // Cancel Request
                            int sessionID = request.getInt32();
                            int secretKey = request.getInt32();
                            // Find a session to cancel
                            synchronized (manager) {
                                for (Iterator<PgProtocolSession> it = manager.clientIterator(); it.hasNext();) {
                                    if (it.next().cancelRequest(sessionID, secretKey)) {
                                        break;
                                    }
                                }
                            }
                            // Then exit
                            return;
                        } else if (protocolMajor == 1234
                                && protocolMinor == 5679) {
                            // SSL Request
                            writeToChannel(PgProtocolMessage.MSG_SSL_NO,
                                    channel);
                        } else if (protocolMajor == PROTOCOL_MAJOR
                                && protocolMinor == PROTOCOL_MINOR) {
                            while (request.hasMoreData()) {
                                String paramName = request.getString();
                                if ("".equals(paramName)) {
                                    break;
                                }
                                /* Save parameters until later when a password 
                                 * is sent.
                                 */
                                String paramValue = request.getString();
                                if ("database".equalsIgnoreCase(paramName)) {
                                    connDatabase = paramValue;
                                } else if ("user".equalsIgnoreCase(paramName)) {
                                    connUser = paramValue;
                                } else {
                                    client.setExtraParameter(paramName, paramValue);
                                }
                            }
                            setState(SESSION_STATE_HANDSHAKE);
                            writeToChannel(
                                    PgProtocolMessage.MSG_AUTHENTICATION_CLEARTEXT_PASSWORD,
                                    channel);
                        } else {
                            try {
                                writeResponse(ExecutionResult.createErrorResult(
                                		new XDBServerException("Unsupported protocol version")));
                            } catch (Throwable e) {
                                logger.catching(e);
                            }
                            // break session
                            return;
                        }
                        break;
                    case PgProtocolMessage.MESSAGE_TYPE_PASSWORD_MESSAGE:
                        client.login(connUser, connDatabase, 
                                MessageTypes.CONNECTION_MODE_NORMAL, 
                                request.getString());
                        writeToChannel(PgProtocolMessage.MSG_AUTHENTICATION_OK,
                                channel);
                        PgProtocolMessage backendKeyData = new PgProtocolMessage(
                                PgProtocolMessage.MESSAGE_TYPE_BACKEND_KEY_DATA,
                                null);
                        backendKeyData.putInt32(client.getSessionID());
                        Random rnd = new Random(System.currentTimeMillis());
                        backendKey = rnd.nextInt();
                        backendKeyData.putInt32(backendKey);
                        writeToChannel(backendKeyData, channel);
                        PgProtocolMessage paramStatus = new PgProtocolMessage(
                                PgProtocolMessage.MESSAGE_TYPE_PARAMETER_STATUS,
                                null);
                        paramStatus.putString("server_version");
                        paramStatus.putString("9.0.1");
                        writeToChannel(paramStatus, channel);
                            // Extra parameters
                            Set<String> extraParams = client
                                    .getExtraParamNames();
                            if (extraParams != null) {
                                for (String paramName : extraParams) {
                                    paramStatus = new PgProtocolMessage(
                                            PgProtocolMessage.MESSAGE_TYPE_PARAMETER_STATUS,
                                            null);
                                    paramStatus.putString(paramName);
                                    paramStatus.putString(client
                                            .getExtraParameter(paramName));
                                }
                            }
                        writeToChannel(paramStatus, channel);

                        setState(SESSION_STATE_READY);
                        break;
                    case PgProtocolMessage.MESSAGE_TYPE_QUERY:
                        setState(SESSION_STATE_QUERY);
                        try {
                            String query = request.getString().trim();
                            CommandLog.queryLogger.info(client.getSessionID()
                                    + " - " + query);
                            for (String cmd : splitSimpleQuery(query)) {
                                client.createStatement(
                                        "",
                                        RequestAnalyzer.getExecutableRequest(
                                                MessageTypes.REQ_EXECUTE,
                                                cmd, client));
                                try {
                                    client.bindStatement("", "", null);
                                    ResponseMessage rm = client.describeStatement(
                                            "", "");
                                    if (rm instanceof CopyResponse) {
                                        CopyResponse copyResponse = (CopyResponse) rm;
                                        if (copyResponse.getType() == MessageTypes.RESP_COPY_IN) {
                                            startCopyInProcess(copyResponse);
                                        } else { // RESP_COPY_OUT
                                            startCopyOutProcess(copyResponse);
                                        }
                                    }
                                    ExecutionResult result = client.executeRequest("");
                                    if (!(rm instanceof CopyResponse)) {
                                        sendRowDescription(rm instanceof ResultSetResponse ? (ResultSetResponse) rm
                                                : null);
                                    }
                                    if (result.getContentType() == ExecutionResult.CONTENT_TYPE_SUBRESULTS) {
                                        sendSubResults(result);
                                    } else {
                                        // fetchSize = 0;
                                        writeResponse(result);
                                    }
                                } finally {
                                    client.closeCursor("");
                                    client.closeStatement("");
                                }
                            }
                        } catch (Exception ex) {
                            logger.catching(ex);
                            writeResponse(ExecutionResult.createErrorResult(ex));
                        } finally {
                            setState(SESSION_STATE_READY);
                        }
                        break;
                    case PgProtocolMessage.MESSAGE_TYPE_PARSE:
                        setState(SESSION_STATE_QUERY);
                        String statementID = request.getString();
                        String statement = request.getString().trim();
                        
                        CommandLog.queryLogger.info(client.getSessionID()
                                + " - " + statement);
                        client.createStatement(statementID,
                                RequestAnalyzer.getExecutableRequest(
                                        MessageTypes.REQ_EXECUTE,
                                        statement, client));
                        int paramCount = request.getInt16();
                        if (paramCount > 0) {
                            int[] paramTypes = new int[paramCount];
                            for (int i = 0; i < paramCount; i++) {
                                paramTypes[i] = getJavaType(request.getInt32());
                            }
                            client.setParameterTypes(statementID,
                                    paramTypes);
                        }
                        writeToChannel(
                                PgProtocolMessage.MSG_PARSE_COMPLETE,
                                channel);
                        break;
                    case PgProtocolMessage.MESSAGE_TYPE_BIND:
                        if (!error) {
                            String cursorName = request.getString();
                            statementID = request.getString();
                            int[] paramCodes = request.getInt16Array(request.getInt16());
                            int argCount = request.getInt16();
                            byte[][] args = new byte[argCount][];
                            for (int i = 0; i < argCount; i++) {
                                args[i] = request.getBytes(request.getInt32());
                            }
                            // TODO support for return values

                            client.bindStatement(statementID, cursorName,
                                    decodeParameters(args, paramCodes));
                            writeToChannel(PgProtocolMessage.MSG_BIND_COMPLETE,
                                    channel);
                        }
                        break;
                    case PgProtocolMessage.MESSAGE_TYPE_DESCRIBE:
                        if (!error) {
                            char code = (char) request.getInt8();
                            String name = request.getString();
                            if (code == 'S') {
                                int[] paramTypes = client.getParameterTypes(name);
                                if (paramTypes != null && paramTypes.length > 0) {
                                    sendParameterDescription(paramTypes);
                                }
                            }
                            ResponseMessage rm = client.describeStatement(
                                    code == 'S' ? name : null,
                                    code == 'P' ? name : null);
                            sendRowDescription(rm instanceof ResultSetResponse ? (ResultSetResponse) rm
                                    : null);
                        }
                        break;
                    case PgProtocolMessage.MESSAGE_TYPE_EXECUTE:
                        if (!error) {
                            String cursorName = request.getString();
                            fetchSize = request.getInt32();
                            ResponseMessage rm = client.describeStatement(null,
                                    cursorName);
                            if (rm instanceof CopyResponse) {
                                CopyResponse copyResponse = (CopyResponse) rm;
                                if (copyResponse.getType() == MessageTypes.RESP_COPY_IN) {
                                    startCopyInProcess(copyResponse);
                                } else { // RESP_COPY_OUT
                                    startCopyOutProcess(copyResponse);
                                }
                            }
                            ExecutionResult result = client.executeRequest(cursorName);
                            if (writeResponse(result)) {
                                client.closeCursor(cursorName);
                            }
                        }
                        break;
                    case PgProtocolMessage.MESSAGE_TYPE_CLOSE:
                        char code = (char) request.getInt8();
                        String name = request.getString();
                        if (code == 'S') {
                            client.closeStatement(name);
                        } else {
                            client.closeCursor(name);
                        }
                        writeToChannel(PgProtocolMessage.MSG_CLOSE_COMPLETE,
                                channel);
                        break;
                    case PgProtocolMessage.MESSAGE_TYPE_SYNC:
                        error = false;
                        setState(SESSION_STATE_READY);
                        break;
                    case PgProtocolMessage.MESSAGE_TYPE_FUNCTION_CALL:
                        int functionOID = request.getInt32();
                        int[] paramCodes = request.getInt16Array(request.getInt16());
                        int argCount = request.getInt16();
                        byte[][] args = new byte[argCount][];
                        for (int i = 0; i < argCount; i++) {
                            args[i] = request.getBytes(request.getInt32());
                        }
                        decodeParameters(args, paramCodes);
                        switch (functionOID) {
                        default:
                            throw new XDBServerException(
                                    "Function is not supported, OID="
                                            + functionOID);
                        }
                        // setState(SESSION_STATE_QUERY);
                        // break;
                    case PgProtocolMessage.MESSAGE_TYPE_TERMINATE:
                        connUser = null;
                        connDatabase = null;
                        return;
                    default:
                        logger.debug("Unsupported message type: "
                                + request.getMessageType());
                    }// switch
                    logger.log(Level.DEBUG,
                            "Message handled successfully: %0%",
                            new Object[] { request.getMessageType() });
                } catch (Exception ex) {
                    logger.log(Level.DEBUG,
                            "Message handled with error: %0%",
                            new Object[] { request.getMessageType() });
                    logger.catching(ex);
                    try {
                        error = true;
                        writeResponse(ExecutionResult.createErrorResult(ex));
                    } catch (Throwable e) {
                        logger.catching(e);
                        // break session
                        return;
                    }
                }

                request = readRequest();
            }
        } catch (IOException ioe) {
            logger.catching(ioe);
        } finally {
            try {
	            channel.close();
	            manager.removeClient(channel);
            } catch (Throwable t) {
                logger.catching(t);
            }
        }
    }
    
    /**
     * @param rsr
     * @throws IOException
     */
    private void sendRowDescription(ResultSetResponse rsr) throws IOException {
        ColumnMetaData[] meta = null;
        if (rsr != null) {
            meta = rsr.getColumnMetaData();
        }
        if (meta == null) {
            writeToChannel(PgProtocolMessage.MSG_NO_DATA, channel);
        } else {
            PgProtocolMessage response = new PgProtocolMessage(
                    PgProtocolMessage.MESSAGE_TYPE_ROW_DESCRIPTION, new byte[0]);
            response.putInt16(meta.length);
            for (ColumnMetaData column : meta) {
                // Field name
                response.putString(column.alias);
                // table OID
                response.putInt32(0);
                // attribute number ?
                response.putInt16(0);
                // OID of the data type
                response.putInt32(getOID(column.javaSqlType));
                // data type size
                response.putInt16(column.maxLength);
                // The data type modifier
                response.putInt32(0);
                // Format code (text)
                response.putInt16(0);
            }
            writeToChannel(response, channel);
        }
    }

    private void sendParameterDescription(int[] paramTypes) throws IOException {
        PgProtocolMessage response = new PgProtocolMessage(
                PgProtocolMessage.MESSAGE_TYPE_PARAMETER_DESCRIPTION,
                new byte[0]);
        response.putInt16(paramTypes.length);
        for (int paramType : paramTypes) {
            // OID of the data type
            response.putInt32(getOID(paramType));
        }
        writeToChannel(response, channel);
    }

    /**
     * Setup data transfer from client for COPY FROM STDIN
     * @param copyResponse
     * @throws IOException
     */
    private void startCopyInProcess(CopyResponse copyResponse)
            throws IOException {
        PgProtocolMessage response = new PgProtocolMessage(
                PgProtocolMessage.MESSAGE_TYPE_COPY_IN_RESPONSE, new byte[0]);
        // Textual overall format
        response.putInt8(0);
        // Field count
        int fieldCount = copyResponse.getColumnMetaData().length;
        response.putInt16(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            // Textual field format
            response.putInt16(0);
        }
        writeToChannel(response, channel);
        copyResponse.setInputStream(new CopyInStream());
    }

    /**
     * Setup data transfer to client for COPY TO STDOUT
     * @param copyResponse
     * @throws IOException
     */
    private void startCopyOutProcess(CopyResponse copyResponse)
            throws IOException {
        PgProtocolMessage response = new PgProtocolMessage(
                PgProtocolMessage.MESSAGE_TYPE_COPY_OUT_RESPONSE, new byte[0]);
        // Textual overall format
        response.putInt8(0);
        // Field count
        int fieldCount = copyResponse.getColumnMetaData().length;
        response.putInt16(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            // Textual field format
            response.putInt16(0);
        }
        writeToChannel(response, channel);
        copyResponse.setOutputStream(new CopyOutStream());
    }

    private String[] decodeParameters(byte[][] paramData, int[] paramFormat) {
        String[] result = new String[paramData.length];
        int[] paramFormatNormalized;
        if (paramFormat == null || paramFormat.length == 0) {
            paramFormatNormalized = new int[paramData.length];
            Arrays.fill(paramFormatNormalized, 0); // Format TEXT (default)
        } else if (paramFormat.length == 1) {
            paramFormatNormalized = new int[paramData.length];
            Arrays.fill(paramFormatNormalized, paramFormat[0]);
        } else {
            paramFormatNormalized = paramFormat;
        }
        for (int i = 0; i < paramData.length; i++) {
            if (paramFormatNormalized[i] == 0) {
                result[i] = PgProtocolMessage.decodeString(paramData[i]);
            } else {
                throw new XDBServerException("Only text format is supported");
            }
        }
        return result;
    }

    private static String[] splitSimpleQuery(String query) {
        if (Props.XDB_ALLOW_MULTISTATEMENT_QUERY) {
            LinkedList<String> commands = new LinkedList<String>();
            boolean openQuote = false;
            int start = 0;
            for (int i = 0; i < query.length(); i++) {
                char ch = query.charAt(i);
                if (ch == '\'') {
                    openQuote = !openQuote;
                } else if (ch == ';' && !openQuote) {
                    commands.add(query.substring(start, i + 1));
                    start = i + 1;
                }
            }
            if (start < query.length()) {
                String last = query.substring(start).trim();
                if (last.length() > 0) {
                    commands.add(last);
                }
            }
            return commands.size() == 0 ? new String[] { "" }
                    : commands.toArray(new String[commands.size()]);
        } else {
            return new String[] { query };
        }
    }

    /*
     * Constants for well-known OIDs for the types we commonly use. Copy/paste
     * from org.postgresql.driver.core.Oid then edit
     */
    private static final int OID_INVALID = 0;

    private static final int OID_INT2 = 21;

    private static final int OID_INT4 = 23;

    private static final int OID_INT8 = 20;

    private static final int OID_TEXT = 25;

    private static final int OID_NUMERIC = 1700;

    private static final int OID_FLOAT4 = 700;

    private static final int OID_FLOAT8 = 701;

    private static final int OID_BOOL = 16;

    private static final int OID_DATE = 1082;

    private static final int OID_TIME = 1083;

    private static final int OID_TIMETZ = 1266;

    private static final int OID_TIMESTAMP = 1114;

    private static final int OID_TIMESTAMPTZ = 1184;

    private static final int OID_BYTEA = 17;

    private static final int OID_VARCHAR = 1043;

    private static final int OID_OID = 26;

    private static final int OID_BPCHAR = 1042;

    private static final int OID_MONEY = 790;

    private static final int OID_NAME = 19;

    private static final int OID_BIT = 1560;

    private static final int OID_VOID = 2278;

    private static int getOID(int javaType) {
        switch (javaType) {
        case Types.ARRAY:
            return OID_INVALID;
        case Types.BIGINT:
            return OID_INT8;
        case Types.BINARY:
            return OID_BYTEA;
        case Types.BIT:
            return OID_BIT;
        case Types.BLOB:
            return OID_OID;
        case Types.BOOLEAN:
            return OID_BOOL;
        case Types.CHAR:
            return OID_BPCHAR;
        case Types.CLOB:
            return OID_TEXT;
        case Types.DATALINK:
            return OID_INVALID;
        case Types.DATE:
            return OID_DATE;
        case Types.DECIMAL:
            return OID_MONEY;
        case Types.DISTINCT:
            return OID_INVALID;
        case Types.DOUBLE:
            return OID_FLOAT8;
        case Types.FLOAT:
            return OID_FLOAT4;
        case Types.INTEGER:
            return OID_INT4;
        case Types.JAVA_OBJECT:
            return OID_BYTEA;
        case Types.LONGVARBINARY:
            return OID_OID;
        case Types.LONGVARCHAR:
            return OID_TEXT;
        case Types.NULL:
            return OID_OID;
        case Types.NUMERIC:
            return OID_NUMERIC;
        case Types.OTHER:
            return OID_INVALID;
        case Types.REAL:
            return OID_FLOAT8;
        case Types.REF:
            return OID_NAME;
        case Types.SMALLINT:
            return OID_INT2;
        case Types.STRUCT:
            return OID_INVALID;
        case Types.TIME:
            return OID_TIME;
        case Types.TIMESTAMP:
            return OID_TIMESTAMPTZ;
        case Types.TINYINT:
            return OID_INT2;
        case Types.VARBINARY:
            return OID_BYTEA;
        case Types.VARCHAR:
            return OID_VARCHAR;

        default:
            return OID_INVALID;
        }
    }

    private static int getJavaType(int OID) {
        switch (OID) {
        case OID_BIT:
            return Types.BIT;
        case OID_BOOL:
            return Types.BOOLEAN;
        case OID_BPCHAR:
            return Types.CHAR;
        case OID_BYTEA:
            return Types.BINARY;
        case OID_DATE:
            return Types.DATE;
        case OID_FLOAT4:
            return Types.FLOAT;
        case OID_FLOAT8:
            return Types.DOUBLE;
        case OID_INT2:
            return Types.SMALLINT;
        case OID_INT4:
            return Types.INTEGER;
        case OID_INT8:
            return Types.BIGINT;
        case OID_INVALID:
            return Types.OTHER;
        case OID_MONEY:
            return Types.DECIMAL;
        case OID_NAME:
            return Types.REF;
        case OID_NUMERIC:
            return Types.NUMERIC;
        case OID_OID:
            return Types.REF;
        case OID_TEXT:
            return Types.LONGVARCHAR;
        case OID_TIME:
            return Types.TIME;
        case OID_TIMESTAMP:
            return Types.TIMESTAMP;
        case OID_TIMESTAMPTZ:
            return Types.TIMESTAMP;
        case OID_TIMETZ:
            return Types.TIME;
        case OID_VARCHAR:
            return Types.VARCHAR;
        case OID_VOID:
            return Types.NULL;

        default:
            return Types.OTHER;
        }
    }

    /**
     * Converts messages from client to byte stream readable by COPY command
     * @author amart
     */
    private class CopyInStream extends InputStream {
        ByteBuffer bbuf;
        boolean copydone = false;

        CopyInStream() {

        }

        /**
         * Get next message from the queue extract data and fill internal buffer
         * @return
         * @throws IOException
         */
        private boolean fill() throws IOException {
        	PgProtocolMessage message = null;
            while (!copydone) {
                message = readRequest();
                switch (message.getMessageType()) {
                case PgProtocolMessage.MESSAGE_TYPE_COPY_DATA:
                    bbuf = ByteBuffer.wrap(message.getBytes(message.getLength()));
                    return true;
                case PgProtocolMessage.MESSAGE_TYPE_COPY_DONE:
                    copydone = true;
                    break;
                case PgProtocolMessage.MESSAGE_TYPE_COPY_FAIL:
                    throw new IOException("Copy is failed");
                default:
                    throw new IOException("Copy is failed");
                }
            }
            return false;
        }

        /* (non-Javadoc)
         * @see java.io.InputStream#read()
         */
        @Override
        public int read() throws IOException {
            if (bbuf != null && bbuf.hasRemaining() || fill()) {
                return bbuf.get() & 0xff;
            }
            return -1;
        }

        /* (non-Javadoc)
         * @see java.io.InputStream#read()
         */
        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            int totalRead = 0;
            while (length > 0 && (bbuf != null && bbuf.hasRemaining() || fill())) {
                int toRead = Math.min(length, bbuf.remaining());
                bbuf.get(buffer, offset, toRead);
                totalRead += toRead;
                offset += toRead;
                length -= toRead;
            }
            return totalRead == 0 && copydone ? -1 : totalRead;
        }
    }

    /**
     * An OutputStream redirects COPY data to client's STDOUT
     * Server should write to the stream one line at time using write(byte[]) or
     * write(byte[], int, int) method.
     * The line is wrapped Postgres protocol message and is written to
     * client channel.
     * @author amart
     */
    private class CopyOutStream extends OutputStream {
        CopyOutStream() {
        }

        /* (non-Javadoc)
         * @see java.io.OutputStream#write(int)
         */
        @Override
        public void write(byte[] line, int offset, int length) throws IOException {
            PgProtocolMessage response = new PgProtocolMessage(
                    PgProtocolMessage.MESSAGE_TYPE_COPY_DATA,
                    new byte[0]);
            // Data line
            response.putBytes(line, offset, length);
            writeToChannel(response, channel);
        }

        /* (non-Javadoc)
         * @see java.io.OutputStream#write(int)
         */
        @Override
        public void write(int b) throws IOException {
            throw new IOException("Only single row can be output");
        }

        @Override
        public void close() throws IOException {
            writeToChannel(PgProtocolMessage.MSG_COPY_DONE, channel);
        }
    }

    /**
     * Allows us to execute an arbitrary non-SELECT SQL command internally
     * 
     * @param cmd
     */
    public static void executeInternal(String query, XDBSessionContext client) {

        for (String cmd : splitSimpleQuery(query)) {

            try {
                client.createStatement("", RequestAnalyzer
                        .getExecutableRequest(MessageTypes.REQ_EXECUTE, cmd,
                                client));
            } catch (Exception e) {
                client.closeCursor("");
                client.closeStatement("");
                throw new XDBServerException(e.getMessage());
            }

            try {
                client.bindStatement("", "", null);
                ResponseMessage rm = client.describeStatement("", "");

                ExecutionResult result = client.executeRequest("");

            } finally {
                client.closeCursor("");
                client.closeStatement("");
            }
        }
    }

}
