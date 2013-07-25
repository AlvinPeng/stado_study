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
package org.postgresql.stado.metadata.scheduler;

import java.sql.Connection;

import org.postgresql.stado.common.util.Property;
import org.postgresql.stado.exception.XDBServerException;


/**
 * 
 * 
 * 
 */
public class LockType {
    /**
     * Object is locked for read
     */
    public static final int LOCK_SHARE_READ_INT = 1;

    /**
     * Object is locked for write
     */
    public static final int LOCK_SHARE_WRITE_INT = 2;

    /**
     * Object is locked for exclusive use
     */
    public static final int LOCK_EXCLUCIVE_INT = 10;

    private static final String LOCK_MODE_STRICT = "S";

    private static final String LOCK_MODE_LOOSE = "L";

    private static final String LOCK_MODE = LOCK_MODE_LOOSE
            .equalsIgnoreCase(Property.get("xdb.locks.readcommitted.mode")) ? LOCK_MODE_LOOSE
            : LOCK_MODE_STRICT;

    private static final LockType LOCK_SHARE_READ = new LockType(
            LOCK_SHARE_READ_INT, false);

    private static final LockType LOCK_SHARE_WRITE = new LockType(
            LOCK_SHARE_WRITE_INT, false);

    private static final LockType LOCK_EXCLUSIVE = new LockType(
            LOCK_EXCLUCIVE_INT, false);

    public static LockType get(int lockType, boolean upgradeable) {
        if (upgradeable) {
            return new LockType(lockType, true);
        } else {
            switch (lockType) {
            case LOCK_SHARE_READ_INT:
                return LOCK_SHARE_READ;
            case LOCK_SHARE_WRITE_INT:
                return LOCK_SHARE_WRITE;
            case LOCK_EXCLUCIVE_INT:
                return LOCK_EXCLUSIVE;
            default:
                throw new XDBServerException("Unknown lock type: " + lockType);
            }
        }
    }

    private int lockType;

    private boolean upgradeable;

    private LockType(int lockType, boolean upgradeable) {
        this.lockType = lockType;
        this.upgradeable = upgradeable;
    }

    void upgrade(int newType) {
        if (upgradeable) {
            // TODO
        } else {
            throw new XDBServerException("This lock is not upgradeable");
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other instanceof LockType) {
            return lockType == ((LockType) other).lockType;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return lockType;
    }

    @Override
    public String toString() {
        String value = "";
        switch (lockType) {
        case LOCK_SHARE_READ_INT:
            value = "ShareRead";
            break;
        case LOCK_SHARE_WRITE_INT:
            value = "ShareWrite";
            break;
        case LOCK_EXCLUCIVE_INT:
            value = "Exclusive";
            break;
        }
        if (upgradeable) {
            value += "*";
        }
        return value;
    }

    /**
     * Compatibility table
     * 
     * <pre>
     * is held\getting  SH_RD(RC) SH_RD(S) SH_WR(RC) SH_WR(S) EXCL
     * SH_RD(RC)          yes        yes      yes      yes     no
     * SH_RD(S)           yes        yes      no       no      no
     * SH_WR(RC)          yes        no       no       no      no      -
     * SH_WR(S)           yes        no       no       no      no
     * EXCL               no         no       no       no      no
     * </pre>
     * 
     * Legend: Lock types: SH_RD - LOCK_SHARE_READ SH_WR - LOCK_SHARE_WRITE EXCL -
     * LOCK_EXCLUSIVE Isolation levels: (RC) - TRANSACTION_READ_COMMITTED (S) -
     * TRANSACTION_SERIALIZABLE
     * 
     * @param other
     * @param isolationLevel
     * @param otherIsolationLevel
     * @return
     */
    public boolean isCompatible(LockType other, int isolationLevel,
            int otherIsolationLevel) {
        // SHARE_READ
        if (lockType == LOCK_SHARE_READ_INT) {
            return other.lockType == LOCK_SHARE_READ_INT
                    || (other.lockType == LOCK_SHARE_WRITE_INT && isolationLevel == Connection.TRANSACTION_READ_COMMITTED);
        }
        // SHARE_WRITE
        else if (lockType == LOCK_SHARE_WRITE_INT) {
            return (other.lockType == LOCK_SHARE_READ_INT || (other.lockType == LOCK_SHARE_WRITE_INT && LOCK_MODE == LOCK_MODE_LOOSE))
                    && otherIsolationLevel == Connection.TRANSACTION_READ_COMMITTED;
        }
        // EXCLUSIVE locks are always not compatible
        else {
            return false;
        }
    }

    /**
     * 
     * @param other -
     *            other LockType
     * @return true if this lock is stricter (may replace other) false otherwise
     */
    boolean stricterThen(LockType other) {
        return lockType >= other.lockType
                && !(other.lockType == LOCK_SHARE_READ_INT && lockType == LOCK_SHARE_WRITE_INT);
    }

    /**
     * 
     * @return true if this LockType allows reading
     */
    boolean forRead() {
        return lockType == LOCK_SHARE_READ_INT
                || lockType == LOCK_EXCLUCIVE_INT;
    }

    /**
     * 
     * @return true if this LockType allows writing
     */
    boolean forWrite() {
        return lockType == LOCK_SHARE_WRITE_INT
                || lockType == LOCK_EXCLUCIVE_INT;
    }
}
