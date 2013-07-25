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
 * RequestAnalyzer.java
 * 
 *  
 */
package org.postgresql.stado.protocol;

import java.util.LinkedList;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.ExecutableRequest;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.engine.io.MessageTypes;
import org.postgresql.stado.engine.io.XMessage;
import org.postgresql.stado.misc.Timer;
import org.postgresql.stado.parser.IXDBSql;
import org.postgresql.stado.parser.Parser;
import org.postgresql.stado.parser.core.ParseException;


/**
 * To convert protocol request to ExecutableRequest. Protocol
 * request has request type and optional command.
 * 
 *  
 * @see org.postgresql.stado.engine.ExecutableRequest
 */
public class RequestAnalyzer {
    private static final XLogger logger = XLogger
            .getLogger(RequestAnalyzer.class);

    // BUILD_CUT_START
    private static Timer analyzeTimer = new Timer();

    // BUILD_CUT_END

    /**
     * Convert protocol request (request type only) to
     * ExecutableRequest.
     * 
     * @return the ExecutableRequest
     * @param messageType the request type
     * @param client associated session
     * @throws ParseException never thrown
     */
    public static ExecutableRequest getExecutableRequest(int messageType,
            XDBSessionContext client) throws ParseException {
        return getExecutableRequest(messageType, null, client);
    }

    /**
     * Convert protocol request (request type and command) to
     * ExecutableRequest.
     * 
     * @return the ExecutableRequest
     * @param messageType the request type
     * @param cmd the command
     * @param client associated session
     * @throws ParseException failed to parse command
     */
    public static ExecutableRequest getExecutableRequest(int messageType,
            String cmd, XDBSessionContext client) throws ParseException {
        final String method = "getExecutableRequest";
        logger.entering(method, new Object[] { new Integer(messageType), cmd,
                client });
        // BUILD_CUT_START
        analyzeTimer.startTimer();
        // BUILD_CUT_END
        ExecutableRequest request = new ExecutableRequest(cmd);
        Parser parser = new Parser(client);
        try {
            switch (messageType) {
            case MessageTypes.REQ_BULK_INSERT_NEXT:
                parser.parseBulkInsertNext(cmd);
                break;
            case MessageTypes.REQ_BULK_INSERT_START:
                parser.parseBulkInsert(cmd);
                break;
            case MessageTypes.REQ_METADATA:
                parser.parseAddDropNode(cmd);
                break;
            case MessageTypes.REQ_CLOSE_RESULTSET:
                parser.parseCloseResultSet(cmd);
                break;
            case MessageTypes.REQ_BATCH_EXEC:
                String templateStart = "";
                String templateEnd = "";
                int currentPos = 0;
                int endPos = cmd.indexOf(XMessage.ARGS_DELIMITER);
                if (cmd.startsWith("TEMPLATE_START:")) {
                    templateStart = cmd.substring(15, endPos);
                    currentPos = endPos + XMessage.ARGS_DELIMITER.length();
                    endPos = cmd.indexOf(XMessage.ARGS_DELIMITER, currentPos);
                    if (cmd.startsWith("TEMPLATE_END:", currentPos)) {
                        templateStart = cmd.substring(currentPos + 13, endPos);
                        currentPos = endPos + XMessage.ARGS_DELIMITER.length();
                        endPos = cmd.indexOf(XMessage.ARGS_DELIMITER,
                                currentPos);
                    }
                }
                LinkedList<IXDBSql> subRequests = new LinkedList<IXDBSql>();
                while (endPos >= currentPos) {
                    parser.parseStatement(templateStart
                            + cmd.substring(currentPos, endPos) + templateEnd);
                    subRequests.add(parser.getSqlObject());
                    currentPos = endPos + XMessage.ARGS_DELIMITER.length();
                    endPos = cmd.indexOf(XMessage.ARGS_DELIMITER, currentPos);
                }
                parser.parseStatement(templateStart + cmd.substring(currentPos)
                        + templateEnd);
                subRequests.add(parser.getSqlObject());
                request.setSubRequests(subRequests
                        .toArray(new IXDBSql[subRequests.size()]));
                return request;
            default:
                if (cmd != null) {
                    // try and extract fetch size
                    int pos = cmd.lastIndexOf(XMessage.ARGS_DELIMITER);
                    if (pos > 0) {
                        try {
                            int fetchSize = Integer.parseInt(cmd.substring(pos
                                    + XMessage.ARGS_DELIMITER.length()));
                            request.setFetchSize(fetchSize);
                            cmd = cmd.substring(0, pos);
                        } catch (NumberFormatException nfe) {
                            // Ignore error, use default
                        }
                    }
                    parser.parseStatement(cmd);
                }
            }
            IXDBSql sqlObject = parser.getSqlObject();
            request.setSQLObject(sqlObject);
            return request;

        } finally {
            // BUILD_CUT_START
            analyzeTimer.stopTimer();
            // BUILD_CUT_END
            logger.exiting(method);
        }
    }
}
