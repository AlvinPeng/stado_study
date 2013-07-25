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
 * 
 *  
 */
public class RequestMessage extends org.postgresql.stado.engine.io.XMessage {
    // id specific to a connection, thus not a unique id for the system. (?)
    // this should be used with the connection id to determine uniqueness
    private static int nextRequestId = 0;

    public synchronized static int getNextRequestId() {
        if (nextRequestId >= Integer.MAX_VALUE) {
            System.out
                    .println("request id has reached max int value, recycling");
            nextRequestId = 0;
        }
        return ++nextRequestId;
    }

    /**
     * Reconstruct received request
     */
    public RequestMessage(byte[] header) {
        super();
        setHeaderBytes(header);
    }

    public RequestMessage(byte type, String cmd) {
        super();
        setType(type);
        setRequestId(getNextRequestId());
        storeString(cmd);
    }

    public RequestMessage(int type, String cmd) {
        this((byte) type, cmd);
    }
}
