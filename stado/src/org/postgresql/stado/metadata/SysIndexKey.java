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
 * SysIndexKey.java
 *
 *  
 */

package org.postgresql.stado.metadata;

/**
 * class SysIndexKey caches data from xsysindexkeys (one record)
 * 
 * 
 */
public class SysIndexKey {
    public int idxkeyid; // unique id for this index key

    public int idxid; // index id - referes to SysIndex.idxid

    public int idxkeyseq; // sequence number of this key in the index

    public int idxascdesc; // ==0 for ascending

    public int colid; // id corresponding to the SysColumn

    public String coloperator;

    public SysColumn sysColumn = null; // corresponding SysColumn object
}
