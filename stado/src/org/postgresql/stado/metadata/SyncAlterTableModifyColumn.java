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
package org.postgresql.stado.metadata;

import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.parser.SqlAlterModifyColumn;

/**
 * SyncAlterTableAddColumn class synchornizes the MetaData DB after a ALTER
 * TABLE ADD COLUMN has been successful on the user DB
 * 
 * It implements IMetaDataUpdate. These methods are called from
 * Engine.executeDDLOnMultipleNodes();
 */

public class SyncAlterTableModifyColumn implements IMetaDataUpdate {

    private SqlAlterModifyColumn aSqlAlterModifyColumn;

    /**
     * Constructor Creates an Object for Updating and refresing the MetaData DB
     * as per the SqlAlterAddColumn (CREATE TABLE ...)
     */
    public SyncAlterTableModifyColumn(SqlAlterModifyColumn aSqlAlterModifyColumn) {
        this.aSqlAlterModifyColumn = aSqlAlterModifyColumn;
    }

    /**
     * Method execute() Updates the MetaData DB as per the ALTER TABLE ADD COL
     * statement stored in aSqlAlterAddColumn
     */
    public void execute(XDBSessionContext client) throws Exception {
        // -----------------------------
        // update the xsyscolumn table etc
        // -----------------------------
        MetaUtils.modifyTableColumn(aSqlAlterModifyColumn.getColDef(),
                aSqlAlterModifyColumn.getParent().getTable());
    }

    /**
     * refresh() Refreshes the MetaData cahce by reading in the table
     * information just created
     */
    public void refresh() throws Exception {
        // refresh MetaData cache for new table added
        aSqlAlterModifyColumn.getParent().getTable().readTableInfo();
    }
}
