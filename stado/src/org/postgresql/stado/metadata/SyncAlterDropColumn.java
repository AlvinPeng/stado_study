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
import java.sql.*;

import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.parser.SqlAlterDropColumn;


// import other packages

/**
 * SyncAlterTableDropColumn class synchornizes the MetaData DB after a ALTER
 * TABLE DROP COLUMN has been successful on the user DB
 * 
 * It implements IMetaDataUpdate. These methods are called from
 * Engine.executeDDLOnMultipleNodes();
 */

public class SyncAlterDropColumn implements IMetaDataUpdate {

    private SqlAlterDropColumn aSqlAlterDropColumn;

    private SysDatabase database;

    /**
     * Constructor Creates an Object for Updating and refresing the MetaData DB
     * as per the SqlAlterDropColumn (ALTER TABLE ...)
     */
    public SyncAlterDropColumn(SqlAlterDropColumn aSqlAlterDropColumn) {
        this.aSqlAlterDropColumn = aSqlAlterDropColumn;
    }

    /**
     * Method execute() Updates the MetaData DB as per the ALTER TABLE DROP
     * COLUMN statement stored in aSqlAlterDropColumn
     * 
     * This asumes that the COLUMN being dropped is a plain column. By plain; I
     * mean this column is not used anywhere as primary key, foreign key,
     * secondary key - no constraints on this.
     * 
     * SqlAlterDropColumn.checkSemantics() has made some basic checks to allow
     * or deny the DROP COLUMN to take place. For cases not handled there - we
     * rely on the underlying user DB to fail.
     * 
     * In such cases execute() is not called ( as a result of userDB update
     * Failures).
     */
    public void execute(XDBSessionContext client) throws Exception {
        database = client.getSysDatabase();

        SysColumn sysCol = aSqlAlterDropColumn.getParent().getTable()
                .getSysColumn(aSqlAlterDropColumn.getColumnName());

        int colid = sysCol.getColID();

        // We need to delete any Index created on this column.
        // Info for that is in xsysindexes, xsysindexkeys, xsysconstraints
        //
        // PROCEDURE
        // 1. Identify any index created on this coulumn by
        // Select idxid from xsysindexkeys where colid = xxx
        //
        // 2. for all such indexes found
        //
        // 2.1 delete from xyscontraints where idxid = idxid
        //
        // 2.2 delete from xsysindexkeys where .idxid = idxid
        //
        // 2.3 delete from xsysindexes where .idxid = idxid
        //
        // 3. delete col info from xsyscol

        // setp 1
        String sql_1 = "SELECT idxid from xsysindexkeys where colid=" + colid;
        // ResultSet rs = aSqlStatement.executeQuery(sql_1);
        ResultSet rs = MetaData.getMetaData().executeQuery(sql_1);

        // step2
        // delete all indexes and index-constraints defined on this column
        String sql_2_1, sql_2_2, sql_2_3;

        while (rs.next()) {
            int idxid = rs.getInt("idxid");

            // step 2.1
            sql_2_1 = "DELETE FROM xsysconstraints WHERE idxid=" + idxid;
            // aSqlStatement.executeUpdate(sql_2_1);
            MetaData.getMetaData().executeUpdate(sql_2_1);

            // step 2.2
            sql_2_2 = "DELETE FROM xsysindexkeys WHERE idxid=" + idxid;
            // aSqlStatement.executeUpdate(sql_2_2);
            MetaData.getMetaData().executeUpdate(sql_2_2);

            // step 2.3
            sql_2_3 = "DELETE FROM xsysindexes WHERE idxid=" + idxid;
            // aSqlStatement.executeUpdate(sql_2_3);
            MetaData.getMetaData().executeUpdate(sql_2_3);
        }

        // step 3
        String sql_3 = "DELETE FROM xsyscolumns where colid = " + colid;
        // aSqlStatement.executeUpdate(sql_3);
        MetaData.getMetaData().executeUpdate(sql_3);
    }

    /**
     * refresh() Refreshes the MetaData cahce by reading in the table
     * information just created
     */
    public void refresh() throws Exception {
        // refresh table - this will refresh Column info as well
        aSqlAlterDropColumn.getParent().getTable().readTableInfo();
    }
}
