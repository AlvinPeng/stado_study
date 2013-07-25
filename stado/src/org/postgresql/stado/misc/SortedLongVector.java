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
 * SortedLongVector.java
 */

package org.postgresql.stado.misc;

import java.util.*;

/**
 * Quick and dirty class for keeping things sorted in a Vector Note that it
 * sorts in descending order (biggest first)
 */
public class SortedLongVector extends Vector {

    /**
     * 
     */
    private static final long serialVersionUID = -4839602795589055469L;
    private Vector keyVector;

    /** Creates a new instance of SortedVector */
    public SortedLongVector() {
        super();

        keyVector = new Vector();
    }

    // addElement, but with a key.
    // This Vector will contain a small number of elements,
    // so just iterate to find position.

    public void blah() {

    }

    public synchronized void addElement(long key, Object anObject) {
        int i;

        for (i = 0; i < keyVector.size(); i++) {
            if (key < ((Long) keyVector.elementAt(i)).intValue()) {
                // i--;
                break;
            }
        }

        keyVector.insertElementAt(new Long(key), i);

        super.insertElementAt(anObject, i);
    }

    // Get the key for the specified postion
    public long getKeyAt(int position) {
        return ((Long) keyVector.elementAt(position)).longValue();
    }

}
