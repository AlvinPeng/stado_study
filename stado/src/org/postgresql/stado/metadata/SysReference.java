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
 * SysReference.java
 *
 *  
 */

package org.postgresql.stado.metadata;

import java.util.Vector;
import java.sql.*;

import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBServerException;


/**
 * class SysReference caches data from xsysreferences (one record)
 * 
 * 
 */

public class SysReference {
    private int refid; // a unique id for this reference

    private SysConstraint constraint; // constraint defining this reference

    private int reftableid; // tableid having refered fields

    private int refidxid; // the indexid on the table specified by 'tableid'

    private Vector foreignKeys; // vector of SysForeignKey's for this reference

    public SysReference(SysConstraint constraint, int refid, int reftableid,
            int refidxid) {
        this.constraint = constraint;
        this.refid = refid;
        this.reftableid = reftableid;
        this.refidxid = refidxid;
    }

    public Vector getForeignKeys() {
        return foreignKeys;
    }

    /**
     * Reads the foreign keys info for this reference
     * 
     * Pre-Condition The RefId must be set correctly for this to work. Updates
     * the info in this SysReference.
     * 
     * ABORTS if Refid does not exist
     */
    public void readForeignKeysInfo() throws XDBServerException {

        if (refid == -1) {
            // programmer error
            throw new XDBServerException(
                    ErrorMessageRepository.SYSREFKEYS_CORRUPT + "(" + refid
                            + ")", 0,
                    ErrorMessageRepository.SYSREFKEYS_CORRUPT_CODE);
        }

        try {
            // ----------------------------------------------------------
            // get indexkeys info
            ResultSet keysRS = MetaData.getMetaData().executeQuery(
                    "SELECT * FROM xsysforeignkeys WHERE refid = " + refid
                            + " ORDER BY fkeyseq");

            foreignKeys = new Vector();
            int keys = 0;

            while (keysRS.next()) {
                SysForeignKey aKey = new SysForeignKey();
                aKey.fkeyid = keysRS.getInt("fkeyid");
                aKey.refid = keysRS.getInt("refid");
                aKey.fkeyseq = keysRS.getInt("fkeyseq");
                aKey.colid = keysRS.getInt("colid");
                aKey.refcolid = keysRS.getInt("refcolid");
                keys++;
                foreignKeys.addElement(aKey);
            }

            // Hmm - if no reference keys are defined
            // BOMB out !
            if (keys == 0) {
                throw new SQLException("No Foreign keys defined for " + refid);
            }

        } catch (SQLException se) {
            String errorMessage = ErrorMessageRepository.REFERENCE_FAILURE
                    + " ( " + refid + " )";
            throw new XDBServerException(errorMessage, se,
                    XDBServerException.SEVERITY_HIGH);
        }
    }

    public SysConstraint getConstraint() {
        return constraint;
    }

    public int getRefID() {
        return refid;
    }

    public int getRefTableID() {
        return reftableid;
    }

    public int getRefIdxID() {
        return refidxid;
    }

    public boolean getDistributedCheck() {
        return constraint.getIsSoft() == 1;
    }
}
