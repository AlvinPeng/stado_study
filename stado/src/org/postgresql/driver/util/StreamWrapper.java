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
package org.postgresql.driver.util;

import java.io.InputStream;

/**
 * Wrapper around a length-limited InputStream.
 *
 * @author Oliver Jowett (oliver@opencloud.com)
 */
public class StreamWrapper {
    public StreamWrapper(byte[] data, int offset, int length) {
        this.stream = null;
        this.rawData = data;
        this.offset = offset;
        this.length = length;
    }

    public StreamWrapper(InputStream stream, int length) {
        this.stream = stream;
        this.rawData = null;
        this.offset = 0;
        this.length = length;
    }

    public InputStream getStream() {
        if (stream != null)
            return stream;

        return new java.io.ByteArrayInputStream(rawData, offset, length);
    }

    public int getLength() {
        return length;
    }

    public int getOffset() {
        return offset;
    }

    public byte[] getBytes() {
        return rawData;
    }

    public String toString() {
        return "<stream of " + length + " bytes>";
    }

    private final InputStream stream;
    private final byte[] rawData;
    private final int offset;
    private final int length;
}
