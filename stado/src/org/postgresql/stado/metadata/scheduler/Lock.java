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
package org.postgresql.stado.metadata.scheduler;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import org.postgresql.stado.common.util.XLogger;


/**
 * Class to describe and track lock on specified object Lock may be held on
 * object or it may be wanted to be acquired Lock is collection of lock types.
 * Locks may be compared, merged and they may conflict with each other.
 * 
 * 
 * @param <MO>
 *            Managed Object
 */
public class Lock<MO> implements Cloneable {
    static final XLogger CATEGORY_LOCK = XLogger.getLogger("Locks");

    /**
     * Managed object (can not be changed)
     */
    private MO managedObject;

    /**
     * Collection of LockTypes
     */
    private HashSet<LockType> lockTable = new HashSet<LockType>();

    /**
     * Creates and initializes the new Lock object
     * 
     * @param managedObject -
     *            the managed object
     */
    public Lock(MO managedObject) {
        this.managedObject = managedObject;
    }

    /**
     * Creates and initializes the new Lock object
     * 
     * @param managedObject -
     *            the managed object
     * @param lt -
     *            initial lock
     */
    public Lock(MO managedObject, LockType lt) {
        this.managedObject = managedObject;
        lockTable.add(lt);
    }

    /**
     * Creates and initializes the new Lock object
     * 
     * @param managedObject -
     *            the managed object
     * @param lts -
     *            initial locks
     */
    public Lock(MO managedObject, Collection<LockType> lts) {
        this.managedObject = managedObject;
        lockTable.addAll(lts);
    }

    /**
     * 
     * @param lock -
     *            other Lock
     * @return true if this lock is stricter (may replace other) false otherwise
     */
    boolean stricterThan(Lock<MO> lock) {
        if (lock != null) {
            for (LockType otherLT : lock.lockTable) {
                for (LockType thisLT : lockTable) {
                    if (!thisLT.stricterThen(otherLT)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    /**
     * Merge other lock into this. Resulting lock is minimal lock that stricter
     * then each of previous locks.
     * 
     * @param lock -
     *            other lock
     */
    void merge(Lock<MO> lock) {
        for (LockType otherLT : lock.lockTable) {
            boolean add = true;
            for (Iterator<LockType> it = lockTable.iterator(); it.hasNext();) {
                LockType thisLT = it.next();
                if (otherLT.stricterThen(thisLT)) {
                    it.remove();
                }
                if (thisLT.stricterThen(otherLT)) {
                    add = false;
                }
            }
            if (add) {
                lockTable.add(otherLT);
            }
        }
    }

    /**
     * Check if this lock and other one are conflicting
     * 
     * @param otherLock -
     *            other lock
     * @param isolationLevel -
     *            isolation level of transaction holding this lock
     * @param otherIsolationLevel -
     *            isolation level of transaction holding other lock
     * @return true if locks are not conflicting false otherwise
     * @see java.sql.Connection.getTransactionIsolation()
     */
    boolean isCompatible(Lock<MO> otherLock, int isolationLevel,
            int otherIsolationLevel) {
        if (otherLock == null || otherLock.managedObject != managedObject) {
            return true;
        }
        for (LockType otherLT : otherLock.lockTable) {
            for (LockType thisLT : lockTable) {
                if (!otherLT.isCompatible(thisLT, isolationLevel,
                        otherIsolationLevel)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * 
     * @return true if lock allows read from the managed object
     */
    boolean forRead() {
        for (LockType lt : lockTable) {
            if (lt.forRead()) {
                return true;
            }
        }
        return false;
    }

    /**
     * 
     * @return true if lock allows write to the managed object
     */
    boolean forWrite() {
        for (LockType lt : lockTable) {
            if (lt.forWrite()) {
                return true;
            }
        }
        return false;
    }

    /**
     * 
     * @return the managed object
     */
    public MO getManagedObject() {
        return managedObject;
    }

    @Override
    public String toString() {
        return "<" + managedObject + lockTable + ">";
    }
}
