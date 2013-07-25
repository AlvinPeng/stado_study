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
package org.postgresql.stado.engine.io;

import java.io.InputStream;
import java.io.OutputStream;

import org.postgresql.stado.common.ColumnMetaData;
import org.postgresql.stado.parser.SqlCopyData;


/**
 * Response Message returned on console COPY command (COPY FROM STDIN/COPY TO
 * STDOUT)
 * Contains a reference to SqlCopyData instance and allow to supply console
 * input or output stream to it.
 */
public class CopyResponse extends ResponseMessage {

    private SqlCopyData copyData;

    /**
     * A constructor
     * @param copyData
     */
    public CopyResponse(SqlCopyData copyData) {
        super((byte) (copyData.isCopyIn() ? MessageTypes.RESP_COPY_IN : MessageTypes.RESP_COPY_OUT), 0);
        this.copyData = copyData;
    }

    /**
     * Provide console input stream (STDIN) to SqlCopyData instance
     * @param is
     */
    public void setInputStream(InputStream is) {
        copyData.setStdIn(is);
    }

    /**
     * Provide console output stream (STDOUT) to SqlCopyData instance
     * @param os
     */
    public void setOutputStream(OutputStream os) {
        copyData.setStdOut(os);
    }

    /**
     * Provides description of columns affected by the COPY
     * @return
     */
    public ColumnMetaData[] getColumnMetaData() {
        return copyData.getColumnMeta();
    }

}
