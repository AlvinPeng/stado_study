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

package org.postgresql.stado.common.util;

/**
 * Puropose:
 *
 * The main purpose of this class is to let us create a PipedInputStream instance with *custome size*;
 * It will be useful when EDBWriter instance wants to load data (into some database table) using EDB JDBC
 * copy command.
 *
 * 
 */
import java.io.*;

public class XDBPipedInputStream extends PipedInputStream {

    public XDBPipedInputStream(int bufferSize) {
        super();
        buffer = new byte[bufferSize];
    }

    public XDBPipedInputStream(PipedOutputStream out, int bufferSize) throws IOException {
        super(out);
        buffer = new byte[bufferSize];
    }

    public int getLength() {
        return in + 1;
    }
}    
    
