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

// import standard java packeages used
import java.util.List;

import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.parser.SqlDropTable;


// import other packages

/**
 * SyncDropTable class synchornizes the MetaData DB after a DROP TABLE has been
 * successful on the user DB
 * 
 * It implements IMetaDataUpdate. These methods are called from
 * Engine.executeDDLOnMultipleNodes();
 */

public class SyncDropTable implements IMetaDataUpdate {

    private XDBSessionContext client;

    private SysDatabase database;

    private SqlDropTable aSqlDropTable;

    /**
     * Constructor Creates an Object for Updating and refresing the MetaData DB
     * as per the SqlDropTable (DROP TABLE ...)
     */
    public SyncDropTable(SqlDropTable aSqlDropTable) {
        this.aSqlDropTable = aSqlDropTable;
    }

    /**
     * Method execute() Updates the MetaData DB as per the DROP TABLE statement
     */
    public void execute(XDBSessionContext client) throws Exception {
        /*
         * We will have to lock the Meta Data Object - Or we can delete all
         * information from the MetaData and then work with the physical
         * tables.This way the complete object will not be locked and other
         * threads will be able to work their way.
         */
        this.client = client;
        deleteTableInfo(client.getSysDatabase(), aSqlDropTable.getTablename());
    }

    /**
     * This method is callable from DropDatabase as well. Does not begin, commit
     * or rollback This is so as to maintain the semantics for
     * IMetaDataUpdate.execute() from which it is also called.
     */
    public void deleteTableInfo(SysDatabase database, List<String> tableNames)
            throws Exception {
        this.database = database;
        for (String tableName : tableNames) {
            SysTable tableToDrop = database.getSysTable(tableName);
            int tableid = tableToDrop.getSysTableid();
            /*
             * This will remove the information from the meta data tables. At
             * present I am not going to implement this Things to consider are -
             * How will things work in a multi threaded scenario. Please look
             * for comments on the top of the function
             */

            // DeleteInMemoryTableInfo (tableName); TODO
            // handled by refresh()
            /*
             * The order in which we execute these delete statements
             * xSysTabParts - xSysForeginKeys xSysIndexKeys xSysReferences
             * xSysChecks xSysConstraints xSysIndexes xSysColumn xSysTable
             */
            // ---------------------------
            // xSysForiegnKeys
            // ---------------------------
            String xSysForeignKeys = "delete from xsysforeignkeys where refid in "
                    + "( select refid from xsysreferences where constid in "
                    + "( select constid FROM xsysconstraints  where tableid = "
                    + tableid + "))";
            MetaData.getMetaData().executeUpdate(xSysForeignKeys);

            // ---------------------------
            // xSysIndexKeys
            // ---------------------------
            String xSysIndexKeys = "delete from xsysindexkeys where idxid in "
                    + "(select idxid from xsysindexes where tableid = "
                    + tableid + ")";
            MetaData.getMetaData().executeUpdate(xSysIndexKeys);

            // ---------------------------
            // xSysReferences
            // ---------------------------
            String xSysReferences = "delete from xsysreferences where constid in "
                    + "(SELECT Constid FROM xsysconstraints  where tableid = "
                    + tableid + ")";
            MetaData.getMetaData().executeUpdate(xSysReferences);

            // ---------------------------
            // xSysChecks
            // ---------------------------
            String xSysChecks = "delete from xsyschecks where constid in "
                    + "(SELECT constid FROM xsysconstraints"
                    + " where consttype = 'C' and tableid = " + tableid + ")";
            MetaData.getMetaData().executeUpdate(xSysChecks);

            // -----------------------------
            // xSysConstraints
            // -----------------------------
            String xSysConstraints = "delete from xsysconstraints where tableid = "
                    + tableid;
            MetaData.getMetaData().executeUpdate(xSysConstraints);
            /*
             * TODO While deleting indexes we have to take into consideration
             * the deletion of the indexex too. Or will this be take care by the
             * underlying database.
             */
            // -------------------------
            // xSysIndexes
            // -------------------------
            String xSysIndexes = "delete from xsysindexes where tableid = "
                    + tableid;
            MetaData.getMetaData().executeUpdate(xSysIndexes);

            // -------------------------
            // xSysColumns
            // ------------------------
            String xSysColumns = "delete from xsyscolumns where tableid = "
                    + tableid;
            MetaData.getMetaData().executeUpdate(xSysColumns);

            // ----------------------------
            // remove all prtition information which belongs to this table
            // ----------------------------
            tableToDrop.getPartitionMap().removeMapFromMetadataDB(
                    MetaData.getMetaData(), tableToDrop);

            // -------------------------
            // xSysTabPrivs
            // ------------------------
            String xSysTabPrivs = "delete from xsystabprivs where tableid = "
                    + tableid;
            MetaData.getMetaData().executeUpdate(xSysTabPrivs);

            // ---------------------------
            // xSysTables
            // ---------------------------
            String xSysTables = "delete from xsystables where tableid ="
                    + tableid;
            MetaData.getMetaData().executeUpdate(xSysTables);
        }
    }

    /**
     * refresh() Refreshes the MetaData cache by deleting it from cache
     */
    public void refresh() throws Exception {
        synchronized (database) {
            // Update related tables - foreign keys were removed
            for (SysTable aTable : aSqlDropTable.getRelatedTables()) {
                aTable.readTableInfo();
            }
            /*
             * Before deleting the information from the MetaData Database remove
             * the information from the, LockManager and the Notifier, this will
             * also interrupt thoes threads which are waiting on this table
             */
            List<String> tableNames = aSqlDropTable.getTablename();
            for (String tableName : tableNames) {
                synchronized (database.getScheduler()) {
                    database.getLm().remove(database.getSysTable(tableName),
                            client);
                }
                database.dropSysTable(tableName);
            }
        }
    }
}
