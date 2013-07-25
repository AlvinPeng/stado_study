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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Level;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysTable;


/**
 * LockManager all the lock acquired in the database. It works on table level
 * but can work on different levels too.
 */
public class LockManager {
    /**
     * A table of locks held by particular transaction
     */
    private LockTable<ITransaction, SysTable> transactionLocks = new LockTable<ITransaction, SysTable>();

    /**
     * A table of locks that particular transaction wants to acquire. Some of
     * this lock can not be acquired at the moment, and transactions in the list
     * are waiting until other transaction is completed and releases resources
     */
    private LockTable<ITransaction, SysTable> lockCandidates = new LockTable<ITransaction, SysTable>();

    /**
     * Needed for Commit Synchronization.
     */
    private HashSet<ITransaction> workingTrans = new HashSet<ITransaction>();

    /**
     * Committing transactions that altered data are stored here while other
     * transactions that can be reading just altered data are working.
     */
    private HashMap<ITransaction, CommitSynchronizer> committingInserts = new HashMap<ITransaction, CommitSynchronizer>();

    /**
     * Creates and initializes LockManager
     * 
     * @param sysTableList
     *            initial list of object to control access to.
     */
    public LockManager() {
    }

    /**
     * Add new managed object to LockManager
     * 
     * @param createdTable
     *            new object
     * @param tranContext
     *            transacton that has created the object
     * @return true
     */
    public boolean add(Object createdTable, Object tranContext) {
        return true;
    }

    /**
     * Remove managed object to LockManager because it going to be dropped.
     * 
     * @param dropTable
     *            object being removed
     * @param tranContext
     *            transacton that is dropping the object
     * @return true
     */
    public boolean remove(Object dropTable, Object tranContext) {
        return true;
    }

    /**
     * Acquire specified locks for specified transaction and update
     * transactionlocks table accordingly. If other transaction is already held
     * conflicting lock false is returned and lockCandidates table is updated.
     * This method may block if trying to acquire read lock on an object that
     * was recently modified by a transaction that is currently committing. It
     * is unblocked after commit is completed.
     * 
     * @param lockSpecs -
     *            the locks to acquire
     * @param tranContext -
     *            the transaction
     * @return true if succeeded false if conflicting lock is already held.
     * @throws XDBServerException
     *             if deadlock is detected
     */
    public boolean getLock(LockSpecification<SysTable> lockSpecs,
            ITransaction tranContext, boolean isReadyOnlyStatment) throws XDBServerException {
        Lock.CATEGORY_LOCK.log(Level.DEBUG,
                "Transaction %0% is trying to acquire locks %1%", new Object[] {
                        tranContext, lockSpecs });
        boolean success;
        synchronized (transactionLocks) {
            lockCandidates.removeLocks(tranContext);
            
            if (SysDatabase.isReadOnly()) {
            	if (!isReadyOnlyStatment) {
                    notifyTransactionEnd(tranContext);
                    throw new XDBServerException(
                            "Statement invalid when in READ_ONLY mode");
            	}
            }
            
            success = transactionLocks.checkLocks(tranContext, lockSpecs
                    .getCombinedVector());
            if (success) {
                for (CommitSynchronizer synchronizer : committingInserts
                        .values()) {
                    for (Lock<SysTable> lock : lockSpecs
                            .getCombinedVector()) {
                        if (lock.forRead()
                                && synchronizer.hasTable(lock
                                        .getManagedObject())) {
                            return false;
                        }
                    }
                }
                workingTrans.add(tranContext);
                transactionLocks.addLocks(tranContext, lockSpecs
                        .getCombinedVector());
            } else {
                lockCandidates.addLocks(tranContext, lockSpecs
                        .getCombinedVector());
                Collection deadlocks = findDeadLocks();
                if (!deadlocks.isEmpty()) {
                    // Release locks are held by current transaction ...
                    notifyTransactionEnd(tranContext);
                    // ... and interrupt current operation
                    throw new XDBServerException(
                            "Can not complete request due to concurrent problem");
                    // Another way to go is to determine wait of deadlocked
                    // transaction
                    // and force rollback one with minimal cost
                }
            }
        }
        return success;
    }

    /**
     * Notification that transaction is completed request and idle
     * 
     * @param tranContext -
     *            the transaction
     * @return true
     */
    public boolean releaseLock(ITransaction tranContext) {
        Lock.CATEGORY_LOCK.log(Level.DEBUG,
                "Releasing locks held by transaction %0%",
                new Object[] { tranContext });
        synchronized (transactionLocks) {
            workingTrans.remove(tranContext);
            for (CommitSynchronizer synchronizer : committingInserts.values()) {
                if (synchronizer.hasTransaction(tranContext)) {
                    Lock.CATEGORY_LOCK.log(Level.DEBUG,
                            "Count down latch %0% for transaction %1%",
                            new Object[] { synchronizer, tranContext });
                    synchronizer.countDown();
                }
            }
            if (!tranContext.isInTransaction() && !tranContext.isInSubTransaction()) {
                transactionLocks.removeLocks(tranContext);
            }
        }
        return true;
    }

    /**
     * Notification that transaction won't execute request that it wanted to
     * execute
     * 
     * @param tranContext -
     *            the transaction
     */
    public void notifyRefusedRequest(ITransaction tranContext) {
        lockCandidates.removeLocks(tranContext);
    }

    /**
     * Notification that transaction wants to commit. If the transaction
     * modified data and other transaction may be reading those data this method
     * blocks until those transactions complete their requests.
     * 
     * @param tranContext -
     *            the transaction being committed
     */
    public void beforeCommit(ITransaction tranContext) {
        Lock.CATEGORY_LOCK.log(Level.DEBUG,
                "Client wants to commit transaction %0%",
                new Object[] { tranContext });
        releaseLock(tranContext);
        CommitSynchronizer synchronizer = null;
        HashSet<SysTable> tables = new HashSet<SysTable>();
        HashSet<ITransaction> transactions = new HashSet<ITransaction>();
        synchronized (transactionLocks) {
            HashMap<SysTable, Lock<SysTable>> locksByMO = transactionLocks
                    .getLocksByTransaction(tranContext);
            if (locksByMO == null) {
                return;
            }
            for (Map.Entry<SysTable, Lock<SysTable>> entry : locksByMO
                    .entrySet()) {
                if (entry.getValue().forWrite()) {
                    HashMap<ITransaction, Lock<SysTable>> lockByTran = transactionLocks
                            .getLocksByManagedObject(entry.getKey());
                    for (Map.Entry<ITransaction, Lock<SysTable>> entry1 : lockByTran
                            .entrySet()) {
                        if (entry1.getKey() != tranContext
                                && workingTrans.contains(entry1.getKey())
                                && entry1.getValue().forRead()) {
                            tables.add(entry.getKey());
                            transactions.add(entry1.getKey());
                        }
                    }
                }
            }
            if (!transactions.isEmpty()) {
                synchronizer = new CommitSynchronizer(transactions, tables);
                Lock.CATEGORY_LOCK.log(Level.DEBUG,
                        "Create latch %0% for transactions %1%",
                        new Object[] { synchronizer, transactions });
                committingInserts.put(tranContext, synchronizer);
            }
        }
        if (synchronizer != null) {
            synchronizer.await();
        }
    }

    /**
     * Notification that transaction is completed and all locks should be
     * removed.
     * 
     * @param tranContext -
     *            the transaction
     */
    public void notifyTransactionEnd(ITransaction tranContext) {
        Lock.CATEGORY_LOCK.log(Level.DEBUG,
                "Releasing locks by the end of transaction %0%",
                new Object[] { tranContext });
        synchronized (transactionLocks) {
            committingInserts.remove(tranContext);
            transactionLocks.removeLocks(tranContext);
            lockCandidates.removeLocks(tranContext);
            transactionLocks.notifyAll();
        }
    }

    /**
     * Analyze the acquired locks and locks wanted to be acquired and return
     * collection of deadlocked transactions
     * 
     * @return deadlocked transactions
     */
    private Collection<ITransaction> findDeadLocks() {
        //
        boolean reduced;
        LockTable<ITransaction, SysTable> locks;
        LockTable<ITransaction, SysTable> candidates;
        // Take a snapshot
        synchronized (transactionLocks) {
            synchronized (lockCandidates) {
                locks = (LockTable<ITransaction, SysTable>) transactionLocks
                        .clone();
                candidates = (LockTable<ITransaction, SysTable>) lockCandidates
                        .clone();
            }
        }
        do {
            reduced = false;
            // First reduction step: remove locks that are held by transactions
            // that do not want acquire anything
            for (Iterator<ITransaction> it = locks.getTransactions().iterator(); it
                    .hasNext();) {
                ITransaction transaction = it.next();
                if (candidates.getLocksByTransaction(transaction) == null) {
                    locks.removeLocks(transaction);
                    reduced = true;
                }
            }
            // Second reduction step: remove candidates that could be satisfied
            if (reduced) {
                for (Iterator<ITransaction> it = candidates.getTransactions()
                        .iterator(); it.hasNext();) {
                    ITransaction transaction = it.next();
                    if (locks.checkLocks(transaction, candidates
                            .getLocksByTransaction(transaction).values())) {
                        candidates.removeLocks(transaction);
                        reduced = true;
                    }
                }
            }
        } while (reduced);
        return locks.getTransactions();
    }

    /**
     * For debugging purposes
     * 
     * @return
     */
    public String dumpLockManager() {
        return "Current locks: " + transactionLocks + "\n"
                + "Lock candidates: " + lockCandidates + "\n"
                + "Wait for commit: " + committingInserts.keySet() + "\n"
                + "Busy: " + workingTrans + "\n";
    }

    /**
     * Utility class to block one transaction until others complete their work.
     * 
     * 
     */
    private class CommitSynchronizer {
        CountDownLatch aLatch;

        HashSet<SysTable> tables;

        HashSet<ITransaction> transactions;

        CommitSynchronizer(HashSet<ITransaction> transactions,
                HashSet<SysTable> tables) {
            this.transactions = transactions;
            this.tables = tables;
            aLatch = new CountDownLatch(transactions.size());
        }

        boolean hasTable(SysTable table) {
            return tables.contains(table);
        }

        boolean hasTransaction(ITransaction transaction) {
            Lock.CATEGORY_LOCK.log(Level.DEBUG,
                    "Check if latch %0% has transaction %1%",
                    new Object[] { this, transaction });
            return transactions.contains(transaction);
        }

        void countDown() {
            aLatch.countDown();
        }

        void await() {
            try {
                aLatch.await();
            } catch (InterruptedException ignore) {
            }
        }
    }
}