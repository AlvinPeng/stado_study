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
package org.postgresql.stado.metadata;

import org.postgresql.stado.exception.XDBGeneratorException;

/**
 * 
 * 
 */
public abstract class SysSerialGenerator {
    private boolean valid = false;

    private long nextValue;

    private long maxValue = Long.MAX_VALUE;

    /**
     * 
     */
    public SysSerialGenerator() {

    }

    /**
     * 
     */
    public SysSerialGenerator(long maxValue) {
        setMaxValue(maxValue);
    }

    protected void setMaxValue(long maxValue) {
        if (maxValue <= 0) {
            throw new IllegalArgumentException("Max value must be positive");
        }
        this.maxValue = maxValue;
    }

    protected long getMaxValue() {
        return maxValue;
    }

    public long allocateValue() throws XDBGeneratorException {
        return allocateRange(1);
    }

    public synchronized long allocateRange(long length)
            throws XDBGeneratorException {
        // We allow zero length since this is a way to force update
        if (length < 0) {
            throw new IllegalArgumentException(
                    "Range length must not be negative");
        }
        if (!valid) {
            update();
        }
        long value = nextValue;
        if (maxValue - length < value) {
            throw new XDBGeneratorException(getOverflowMessage());
        }
        nextValue += length;
        return value;
    }

    public synchronized void update(long newValue) {
        if (valid) {
            if (newValue > nextValue) {
                nextValue = newValue;
            }
        } else {
            valid = true;
            nextValue = newValue;
        }
    }

    public synchronized void invalidate() {
        valid = false;
    }

    protected abstract void update() throws XDBGeneratorException;

    protected String getOverflowMessage() {
        return "Generator overflow";
    }
}
