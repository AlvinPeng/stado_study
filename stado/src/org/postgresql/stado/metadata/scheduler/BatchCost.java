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
 * BatchCost.java
 * 
 *  
 */
package org.postgresql.stado.metadata.scheduler;

import org.postgresql.stado.metadata.SysTable;

/**
 *  
 */
public class BatchCost implements ILockCost {
    private long totalCost = 0;

    private LockSpecification<SysTable> lockSpec = new LockSpecification<SysTable>();

    /**
     * 
     */
    public BatchCost() {
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#getCost()
     */
    public long getCost() {
        return totalCost;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#getLockSpecs(java.lang.Object)
     */
    public LockSpecification<SysTable> getLockSpecs() {
        return lockSpec;
    }

    public void addElement(ILockCost sqlObject) {
        if (sqlObject != null) {
            totalCost += sqlObject.getCost();
            lockSpec.addAll(sqlObject.getLockSpecs());
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#needCoordinatorConnection()
     */
    public boolean needCoordinatorConnection() {
        return true;
    }

	@Override
	public boolean isReadOnly() {
		// TODO Auto-generated method stub
		return false;
	}
}
