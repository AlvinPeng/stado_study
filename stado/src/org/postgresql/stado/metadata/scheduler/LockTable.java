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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Class to store locks held by transactions. Can be represented as matrix where
 * one dimension is Transactions, second is ManagedObjects and Lock objects are
 * in cells of the matrix. Designed to provide fast access to cells and easily
 * provide info like "transactions holding locks on an Object" or "Objects
 * locked by Transaction".
 * 
 * 
 */
public class LockTable<T extends ITransaction, MO> implements Cloneable {
    /**
     * Transactions that are holding locks
     */
    private HashMap<T, HashMap<MO, Lock<MO>>> storage = new HashMap<T, HashMap<MO, Lock<MO>>>();

    /**
     * Objects locked by transactions
     */
    private HashMap<MO, HashMap<T, Lock<MO>>> index = new HashMap<MO, HashMap<T, Lock<MO>>>();

    /**
     * Returns locks held by specified transaction
     * 
     * @param transaction -
     *            the transaction
     * @return map ManagedObject -> Lock
     */
    public HashMap<MO, Lock<MO>> getLocksByTransaction(T transaction) {
        return storage.get(transaction);
    }

    /**
     * Returns transactions holding locks on specified object
     * 
     * @param managedObject -
     *            the object
     * @return map Transaction -> Lock
     */
    public HashMap<T, Lock<MO>> getLocksByManagedObject(MO managedObject) {
        return index.get(managedObject);
    }

    /**
     * Remove all locks held by specified transaction
     * 
     * @param transaction -
     *            the transaction
     */
    public synchronized void removeLocks(T transaction) {
        HashMap<MO, Lock<MO>> locks = storage.remove(transaction);
        if (locks != null) {
            for (MO managedObject : locks.keySet()) {
                HashMap<T, Lock<MO>> locksByMO = index.get(managedObject);
                locksByMO.remove(transaction);
                if (locksByMO.isEmpty()) {
                    index.remove(managedObject);
                }
            }
        }
    }

    /**
     * Add specified locks to the table.
     * 
     * @param transaction -
     *            the transaction that acquires locks
     * @param locks -
     *            the collection of lock to be acquired
     */
    public synchronized void addLocks(T transaction, Collection<Lock<MO>> locks) {
        for (Lock<MO> lock : locks) {
            HashMap<MO, Lock<MO>> locksByMO = storage.get(transaction);
            if (locksByMO == null) {
                locksByMO = new HashMap<MO, Lock<MO>>();
                storage.put(transaction, locksByMO);
            }
            Lock<MO> oldLock = locksByMO.get(lock.getManagedObject());
            if (oldLock == null) {
                locksByMO.put(lock.getManagedObject(), lock);
                HashMap<T, Lock<MO>> locksByTran = index.get(lock
                        .getManagedObject());
                if (locksByTran == null) {
                    locksByTran = new HashMap<T, Lock<MO>>();
                    index.put(lock.getManagedObject(), locksByTran);
                }
                locksByTran.put(transaction, lock);
            } else {
                oldLock.merge(lock);
            }
        }
    }

    /**
     * Check if specified locks can be acquired (other transactions are not
     * holding conflicting locks)
     * 
     * @param transaction -
     *            the transaction that acquires locks
     * @param locks -
     *            the collection of lock to be acquired
     * @return true if possible to place all the locks false otherwise
     */
    public synchronized boolean checkLocks(T transaction,
            Collection<Lock<MO>> locks) {
        for (Lock<MO> lock : locks) {
            HashMap<MO, Lock<MO>> locksByMO = storage.get(transaction);
            // Check is we already have that lock
            if (locksByMO != null) {
                Lock<MO> oldLock = locksByMO.get(lock.getManagedObject());
                if (!lock.stricterThan(oldLock)) {
                    continue;
                }
            }
            // Check if other locks are compatible
            HashMap<T, Lock<MO>> locksByTran = index.get(lock
                    .getManagedObject());
            if (locksByTran != null) {
                for (Map.Entry<T, Lock<MO>> entry : locksByTran.entrySet()) {
                    if (entry.getKey() != transaction
                            && !lock.isCompatible(entry.getValue(), transaction
                                    .getTransactionIsolation(), entry.getKey()
                                    .getTransactionIsolation())) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    /**
     * 
     * @return transactions that are currently holding locks on objects
     */
    Collection<T> getTransactions() {
        return new ArrayList<T>(storage.keySet());
    }

    @Override
    public synchronized Object clone() {
        LockTable<T, MO> copy = null;
        try {
            copy = (LockTable<T, MO>) super.clone();
            copy.storage = (HashMap<T, HashMap<MO, Lock<MO>>>) storage.clone();
            for (Map.Entry<T, HashMap<MO, Lock<MO>>> entry : storage.entrySet()) {
                copy.storage.put(entry.getKey(), (HashMap<MO, Lock<MO>>) entry
                        .getValue().clone());
            }
            copy.index = (HashMap<MO, HashMap<T, Lock<MO>>>) index.clone();
            for (Map.Entry<MO, HashMap<T, Lock<MO>>> entry : index.entrySet()) {
                copy.index.put(entry.getKey(), (HashMap<T, Lock<MO>>) entry
                        .getValue().clone());
            }
        } catch (CloneNotSupportedException e) {
            // assert false;
        }
        return copy;
    }

    @Override
    public String toString() {
        return "<LockTable: " + storage + ">";
    }
}
