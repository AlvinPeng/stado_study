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
 * SyncDropIndex.java
 *
 *  
 */

package org.postgresql.stado.metadata;

// import other packages
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.parser.SqlDropIndex;

/**
 * SyncDropIndex class synchornizes the MetaData DB after a DROP INDEX has been
 * successful on the user DB
 * 
 * It implements IMetaDataUpdate. These methods are called from
 * Engine.executeDDLOnMultipleNodes();
 * 
 * 
 */
public class SyncDropIndex implements IMetaDataUpdate {

    private SysDatabase database;

    private SqlDropIndex aSqlDropIndex;

    /** Creates a new instance of SyncDropIndex */
    public SyncDropIndex(SqlDropIndex aSqlDropIndex) {
        this.aSqlDropIndex = aSqlDropIndex;
    }

    /**
     * Method execute() Updates the MetaData DB as per the DROP INDEX statement
     * stored in aSqlCreateTable
     */
    public void execute(XDBSessionContext client) throws Exception {
        database = client.getSysDatabase();

        SysTable theTable = database.getSysTable(aSqlDropIndex.getTableName());
        SysIndex theIndex = theTable.getSysIndex(aSqlDropIndex.getIndexName());

        // must also all records of this index from xsysconstraint
        // we have already verified that this is not used as a reference
        // so we do not need to drop xsysreferences.
        // This verification was carried out in checkSemantics() of
        // SqlDropIndex.
        // ----------------------
        // xSysConstraints
        // ----------------------
        String xSysConstraints = "delete from xsysconstraints where idxid = "
                + theIndex.idxid;
        // if (DEBUG) System.out.println(xSysConstraints);
        MetaData.getMetaData().executeUpdate(xSysConstraints);

        // now delete the index info
        MetaUtils.dropIndex(theIndex.idxid);
    }

    /**
     * refresh() Refreshes the MetaData cache by reading in the table
     * information
     */

    public void refresh() throws Exception {
        // refresh MetaData structure, adding new index
        database.getSysTable(aSqlDropIndex.getTableName()).readTableInfo();
    }
}
