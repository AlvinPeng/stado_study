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
package org.postgresql.stado.metadata.partitions;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.Collection;

import org.postgresql.stado.metadata.MetaData;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysTable;


/**
 * 
 * 
 */
public interface PartitionMap extends Serializable, Cloneable {
    /**
     * Generate default mapping for provided list of node IDs
     * 
     * @param nodeIdList
     */
    public void generateDistribution(Collection<Integer> partitionIdList);

    public void readMapFromMetadataDB(MetaData metadata, SysTable parent)
            throws SQLException;

    public void storeMapToMetadataDB(MetaData metadata, SysDatabase database,
            int tableID) throws SQLException;

    public void removeMapFromMetadataDB(MetaData metadata, SysTable parent)
            throws SQLException;

    /**
     * Call this method if you want to add new row to the table. Returns a list
     * of partitions where row with specified key should be written The row
     * should be written in all the listed partitions Passed in parameter could
     * be ignored for different partitioning schemes, if partitioning does not
     * depends on data. WARNING: value returned by the method may differ from
     * call to call even if parameter is kept unchanged. Call it once for each
     * row.
     * 
     * @param key
     * @return collection of partition ids where row should be inserted
     */
    // TODO change parameter type to Object to allow more complex partitioning
    // schemes
    public Collection<Integer> getPartitions(String key);

    /**
     * Call this method if you want to know partitions where row can be found.
     * You may want to update the row or delete it or you just query single row
     * by key. Useful to reduce list of target nodes, where query would be
     * executed. The row could be in one or more of the listed partitions, if
     * partition does not present in the list it does not have the row. If row
     * does not exist in listed partitions it does not exists in the table.
     * 
     * @param key
     * @return collection of partition ids
     */
    // TODO change parameter type to Object to allow more complex partitioning
    // schemes
    public Collection<Integer> findPartitions(String key);

    /**
     * Returns a list of partitions having full set of table rows. Call this
     * method if you need to get all rows of the table. You may query just
     * specified partitions and cobine results ignoring other partitions. Result
     * still in general may have duplicated rows which should be filtered out.
     * It is guaranteed there are no duplicates if getRedundancyLevel() returns
     * 0 or 1 (it does for all currently implemented partitioning schemes).
     * WARNING: value returned by the method may differ from call to call
     * 
     * @return collection of partition ids
     */
    public Collection<Integer> joinPartitions();

    /**
     * Returns full list of partitions where table has data. For bulk updates
     * and deletes, transaction handling, etc.
     * 
     * @return collection of partition ids
     */
    public Collection<Integer> allPartitions();

    /**
     * How many partitions can hold a copy of the row. There is one special
     * value: 0 if row always stored in all the table's partitions. This is max
     * size of a Collection returned by getPartitions(), if result is 0
     * joinPartitions() always returns single row, if result is 0 or 1 subset of
     * partitions returned by joinPartitions() does not have duplicates.
     * 
     * @return redundancy level, 0 - if full
     */
    public int getRedundancyLevel();

    public PartitionMap clone();
}
