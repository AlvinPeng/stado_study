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
package org.postgresql.stado.misc;

public class Timer {
    private java.util.Date start;

    private java.util.Date end;

    private long totalms;

    public Timer() {
        start = new java.util.Date();
        totalms = 0;
    }

    public void startTimer() {
        start = new java.util.Date();
    }

    public void stopTimer() {
        end = new java.util.Date();
        totalms += end.getTime() - start.getTime();
    }

    public String getDuration() {
        if (end == null || end.before(start)) {
            totalms = 0;
        }

        long ms = totalms;
        long min = ms / 60000;
        long sec = (ms - min * 60000) / 1000;
        ms = ms - min * 60000 - sec * 1000;
        return (new String(min + "m " + sec + "s " + ms + "ms"));
    }

    public long getDurationSeconds() {
        if (end == null || end.before(start)) {
            totalms = 0;
        }

        return Math.round(totalms / 1000);
    }

    @Override
    public String toString() {
        return (getDuration());
    }
}
