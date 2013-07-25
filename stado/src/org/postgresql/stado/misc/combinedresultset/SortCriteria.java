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
package org.postgresql.stado.misc.combinedresultset;

/**
 * 
 */
public class SortCriteria {
    public static final int ASCENDING = 1;

    public static final int DESCENDING = 2;

    private int columnPosition;

    private boolean includeInResult;

    int columnType;

    public int getDirection() {
        return direction;
    }

    private int direction;

    public SortCriteria(int columnPosition, boolean includeInResult,
            int direction, int columnType) {
        this.columnPosition = columnPosition;
        this.includeInResult = includeInResult;
        this.direction = direction;
        this.columnType = columnType;
    }

    public int getColumnPosition() {
        return columnPosition;
    }

    public boolean isIncludeInResult() {
        return includeInResult;
    }

    public int getColumnType() {
        return columnType;
    }
}
