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
import java.util.HashSet;
import java.util.Iterator;

import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.MetaData;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysTable;


/**
 * 
 *
 */
public class HashPartitionMap implements PartitionMap {
    /**
     *
     */
    private static final long serialVersionUID = -5952477578322320409L;

    private static final int HASH_SIZE = 256;

    private Integer[] mappingTable;

    private transient HashSet<Integer> partitions;

    /** Parameterless constructor is required for serialization */
    public HashPartitionMap() {
    }


    /**
     * 
     * @see org.postgresql.stado.MetaData.partitions.PartitionMap#findPartitions(java.lang.String)
     *
     * @param key 
     * @return a collection of nodes (one) to use
     */
    public Collection<Integer> findPartitions(String key) {
        return Collections.singleton(getNodeId(key));
    }

    /**     
     * Generate a distribution for the specified node list.
     * This is useful for distributing intermediate tables.
     * 
     * @see org.postgresql.stado.MetaData.partitions.PartitionMap#generateDistribution(java.util.Collection)
     *
     * @param partitionIdList 
     */
    public void generateDistribution(Collection<Integer> partitionIdList) {
        partitions = new HashSet<Integer>(partitionIdList);
        Iterator<Integer> it = partitionIdList.iterator();
        // create mapping table, for all HASH_SIZE possible values,
        // assigning a node to each.
        mappingTable = new Integer[HASH_SIZE];
        for (int i = 0; i < mappingTable.length; i++) {
            if (!it.hasNext()) {
                it = partitionIdList.iterator();
            }
            mappingTable[i] = it.next();
        }
    }
    
    /**
     * 
     * @param key 
     * @return 
     */
    public Collection<Integer> getPartitions(String key) {
        return Collections.singleton(getNodeId(key));
    }

    /**
     * 
     * @return 
     */
    public Collection<Integer> joinPartitions() {
        return allPartitions();
    }

    /**
     * 
     * @return 
     */
    public Collection<Integer> allPartitions() {
        if (partitions == null) {
            partitions = new HashSet<Integer>();
            for (Integer element : mappingTable) {
                partitions.add(element);
            }
        }
        return partitions;
    }

    /**
     * 
     * @return 
     */
    public int getRedundancyLevel() {
        return 1;
    }

    /**
     * 
     * @return 
     */
    @Override
    public PartitionMap clone() {
        try {
            return (PartitionMap) super.clone();
        } catch (CloneNotSupportedException e) {
            // never thrown
            return null;
        }
    }

    /**
     * 
     * @param other 
     * @return 
     */
    @Override
    public boolean equals(Object other) {
        if (other instanceof HashPartitionMap) {
            return Arrays.equals(mappingTable,
                    ((HashPartitionMap) other).mappingTable);
        }
        return false;
    }

    /**
     * 
     * @param sValue 
     * @return 
     */
    private Integer getNodeId(String sValue) {
        return getNodeFromHash(hash(sValue));
    }

    /**
     * 
     * @param iHash 
     * @return 
     */
    private Integer getNodeFromHash(int iHash) {
        if (mappingTable[iHash] == null) {
            // should not get here
            // "Could not determine the Node from the given hash value: "
            String errorMessage = ErrorMessageRepository.NODE_INFORMATION_LOST_FROM_NODE_TABLE
                    + " ( " + iHash + " )";
            throw new XDBServerException(
                    errorMessage,
                    XDBServerException.SEVERITY_LOW,
                    ErrorMessageRepository.NODE_INFORMATION_LOST_FROM_NODE_TABLE_CODE);
        }
        return mappingTable[iHash];
    }

    /**
     * 
     * @param str 
     * @return 
     */
    private final static int hash(String str) {

        return hash(str == null ? null : str.getBytes());
    }

    /**
     * get hash value, 0 - HASH_SIZE
     */
    private final static int hash(byte key[]) {

        if (key == null) {
            return 0;
        }
        int value = 0x238F13AF * key.length;

        for (int i = 0; i < key.length; i++) {
            value = value + (key[i] << i * 5 % 24) & 0x7fffffff;
        }

        return (1103515243 * value + 12345) % 65537 & 0x000000FF;
    }

    /**
     * 
     * @param metadata 
     * @param parent 
     * @throws java.sql.SQLException 
     */
    public void readMapFromMetadataDB(MetaData metadata, SysTable parent)
            throws SQLException {
        mappingTable = new Integer[HASH_SIZE];
        String query = "SELECT hashValue, nodeid FROM xsystabparthash WHERE tableid = "
                + parent.getTableId();
        ResultSet rs = metadata.executeQuery(query);
        try {
            while (rs.next()) {
                mappingTable[rs.getInt(1)] = rs.getInt(2);
            }
        } finally {
            rs.close();
        }
    }

    
    /**
     * 
     * @param metadata 
     * @param database 
     * @param tableID 
     *
     * @throws java.sql.SQLException 
     */
    public void storeMapToMetadataDB(MetaData metadata, SysDatabase database,
            int tableID) throws SQLException {
        int parthashid = 0;
        String query = "SELECT max(parthashid) FROM xsystabparthash";
        ResultSet rs = metadata.executeQuery(query);
        try {
            rs.next();
            parthashid = rs.getInt(1) + 1;
        } finally {
            rs.close();
        }
        for (int i = 0; i < mappingTable.length; i++) {
            String insert = "INSERT INTO xsystabparthash "
                    + "(parthashid, tableid, dbid, hashValue, nodeid) "
                    + "VALUES (" + parthashid++ + ", " + tableID + ", "
                    + database.getDbid() + ", " + i + ", " + mappingTable[i]
                    + ")";
            metadata.executeUpdate(insert);
        }
    }

    /**
     * 
     * @param metadata 
     * @param parent 
     *
     * @throws java.sql.SQLException 
     */
    public void removeMapFromMetadataDB(MetaData metadata, SysTable parent)
            throws SQLException {
        String delete = "DELETE FROM xsystabparthash WHERE tableid = "
                + parent.getTableId();
        metadata.executeUpdate(delete);
        delete = "DELETE FROM xsystabparts WHERE tableid = "
                + parent.getTableId();
        metadata.executeUpdate(delete);
    }
}
