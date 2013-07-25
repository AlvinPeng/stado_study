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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;
import java.util.TreeSet;

import org.postgresql.stado.metadata.MetaData;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysTable;


/**
 * 
 * 
 */
public class RobinPartitionMap implements PartitionMap {
    /**
     * 
     */
    private static final long serialVersionUID = -269652343536584158L;

    private Integer[] partitions;

    private transient int currentIdx = -1;

    /** Parameterless constructor is required for serialization */
    public RobinPartitionMap() {
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.MetaData.partitions.PartitionMap#findPartitions(java.lang.String)
     */
    public Collection<Integer> findPartitions(String key) {
        return allPartitions();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.MetaData.partitions.PartitionMap#generateDistribution(java.util.Collection)
     */
    public void generateDistribution(Collection<Integer> partitionIdList) {
        partitions = partitionIdList
                .toArray(new Integer[partitionIdList.size()]);
        Arrays.sort(partitions);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.MetaData.partitions.PartitionMap#getPartitions(java.lang.String)
     */
    public Collection<Integer> getPartitions(String key) {
        if (currentIdx < 0) {
            Random rnd = new Random(System.currentTimeMillis());
            currentIdx = rnd.nextInt(partitions.length);
        }
        if (++currentIdx == partitions.length) {
            currentIdx = 0;
        }
        return Collections.singleton(partitions[currentIdx]);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.MetaData.partitions.PartitionMap#joinPartitions()
     */
    public Collection<Integer> joinPartitions() {
        return allPartitions();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.MetaData.partitions.PartitionMap#allPartitions()
     */
    public Collection<Integer> allPartitions() {
        return Arrays.asList(partitions);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.MetaData.partitions.PartitionMap#getRedundancyLevel()
     */
    public int getRedundancyLevel() {
        return 1;
    }

    @Override
    public PartitionMap clone() {
        try {
            return (PartitionMap) super.clone();
        } catch (CloneNotSupportedException e) {
            // never thrown
            return null;
        }
    }

    @Override
    public boolean equals(Object other) {
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.MetaData.partitions.PartitionMap#readMapFromMetadataDB(org.postgresql.stado.MetaData.MetaData,
     *      int)
     */
    public void readMapFromMetadataDB(MetaData metadata, SysTable parent)
            throws SQLException {
        TreeSet<Integer> partSet = new TreeSet<Integer>();
        String query = "SELECT nodeid from xsystabparts WHERE tableid = "
                + parent.getTableId() + " ORDER BY nodeid";
        ResultSet rs = metadata.executeQuery(query);
        try {
            while (rs.next()) {
                partSet.add(rs.getInt(1));
            }
        } finally {
            rs.close();
        }
        partitions = partSet.toArray(new Integer[partSet.size()]);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.MetaData.partitions.PartitionMap#storeMapToMetadataDB(org.postgresql.stado.MetaData.MetaData,
     *      int)
     */
    public void storeMapToMetadataDB(MetaData metadata, SysDatabase database,
            int tableID) throws SQLException {
        int partid = 0;
        String query = "SELECT max(partid) FROM xsystabparts";
        ResultSet rs = metadata.executeQuery(query);
        try {
            rs.next();
            partid = rs.getInt(1) + 1;
        } finally {
            rs.close();
        }
        for (Integer nodeID : partitions) {
            String insert = "INSERT INTO xsystabparts "
                    + "(partid, tableid, dbid, nodeid) " + "VALUES ("
                    + partid++ + ", " + tableID + ", " + database.getDbid()
                    + ", " + nodeID + ")";
            metadata.executeUpdate(insert);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.MetaData.partitions.PartitionMap#removeMapFromMetadataDB(org.postgresql.stado.MetaData.MetaData,
     *      int)
     */
    public void removeMapFromMetadataDB(MetaData metadata, SysTable parent)
            throws SQLException {
        String delete = "DELETE FROM xsystabparts WHERE tableid = "
                + parent.getTableId();
        metadata.executeUpdate(delete);
    }
}
