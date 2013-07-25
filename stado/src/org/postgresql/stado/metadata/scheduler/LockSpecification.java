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
import java.util.Iterator;
import java.util.Map;

/**
 * 
 */
public class LockSpecification<MO> {
    private Map<MO, Lock<MO>> combinedVector = new HashMap<MO, Lock<MO>>();

    public LockSpecification() {

    }

    public LockSpecification(Collection<MO> readObjects,
            Collection<MO> writeObjects) {
        addReadObjects(readObjects);
        addWriteObjects(writeObjects);
    }

    /**
     * @return Returns the combinedVector.
     */
    public Collection<Lock<MO>> getCombinedVector() {
        return combinedVector.values();
    }

    public void addReadObject(MO toLock) {
        add(toLock, LockType.get(LockType.LOCK_SHARE_READ_INT, false));
    }

    public void addWriteObject(MO toLock) {
        add(toLock, LockType.get(LockType.LOCK_EXCLUCIVE_INT, false));
    }

    public void addReadObjects(Collection<MO> readObjects) {
        for (Iterator<MO> it = readObjects.iterator(); it.hasNext();) {
            addReadObject(it.next());
        }
    }

    public void addWriteObjects(Collection<MO> writeObjects) {
        for (Iterator<MO> it = writeObjects.iterator(); it.hasNext();) {
            addWriteObject(it.next());
        }
    }

    public void addAll(LockSpecification<MO> other) {
        for (Lock<MO> lock : other.getCombinedVector()) {
            Lock<MO> current = combinedVector.get(lock.getManagedObject());
            if (current == null) {
                combinedVector.put(lock.getManagedObject(), lock);
            } else {
                current.merge(lock);
            }
        }
    }

    public void add(MO toLock, LockType lockType) {
        Lock<MO> newLock = new Lock<MO>(toLock, lockType);
        Lock<MO> lock = combinedVector.get(toLock);
        if (lock == null) {
            combinedVector.put(toLock, newLock);
        } else {
            lock.merge(newLock);
        }
    }

    @Override
    public String toString() {
        return this.getClass().getName() + ":" + getCombinedVector();
    }
}
