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
package org.postgresql.stado.engine.io;

/**
 * message constants
 *
 *
 */
public interface MessageTypes {
    public static final int UNKNOWN = 0;

    // request message types
    public static final int REQ_EXECUTE = 10;// execute() calls which return

    // boolean

    public static final int REQ_GET_MORE_ROWS = 11;// get the next set of

    // resultset

    public static final int REQ_METADATA = 12;// special meta data queries

    public static final int REQ_CLOSE_RESULTSET = 13;// special meta data

    // queries

    public static final int REQ_BATCH_EXEC = 14;// regular batch exec

    public static final int REQ_PREP_BATCH_EXEC = 15;// prepared statement

    // batch execution

    public static final int REQ_DB_START = 16;

    public static final int REQ_DB_STOP = 17;

    public static final int REQ_SHUTDOWN = 18;

    public static final int REQ_SERVER_PROPERTIES = 19;// special meta data

    // queries

    public static final int REQ_BULK_INSERT_START = 20;

    public static final int REQ_BULK_INSERT_NEXT = 21;

    public static final int REQ_BULK_INSERT_FINISH = 22;

    public static final int REQ_GET_NODE_DB_CONNECTION_INFO = 23;

    public static final int REQ_DB_CREATE = 24;

    public static final int REQ_DB_PERSIST = 25;

    public static final int REQ_DB_DROP = 26;

    public static final int REQ_GET_NEXT_RESULT = 27;// get the next result

    // for command producing
    // multiple results

    // response message types
    public static final int RESP_ERROR_MESSAGE = 65;

    public static final int RESP_QUERY_RESULTS = 66;

    public static final int RESP_UPDATE_RESULTS = 67;

    public static final int RESP_GENERIC_CMD = 68;// set isolation, create

    // table, etc

    public static final int RESP_GENERIC_EXECUTE = 69;

    public static final int RESP_BATCH_EXEC = 70;// for both types of batch

    // executions

    public static final int RESP_TEST_LINK = 71;// Ignore message

    public static final int RESP_SERIALIZED_OBJECT = 72;// special meta data

    // queries

    public static final int RESP_BULK_INSERT_OK = 73;

    public static final int RESP_GET_NODE_DB_CONNECTION_INFO = 74;

    // COPY FROM STDIN/TO STDOUT

    public static final int RESP_COPY_IN = 75;

    public static final int RESP_COPY_OUT = 76;

    // Connection modes
    public static final String CONNECTION_MODE_NORMAL = "N";

    public static final String CONNECTION_MODE_PERSISTENT = "P";

    public static final String CONNECTION_MODE_ADMIN = "A";

    public static final String CONNECTION_MODE_CREATE = "C";

    public static final String CONNECTION_MODE_SHUTDOWN = "D";

}
