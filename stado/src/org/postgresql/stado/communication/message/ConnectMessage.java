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
package org.postgresql.stado.communication.message;

/**
 * 
 * 
 */
public class ConnectMessage extends CommandMessage {
    private static final long serialVersionUID = 8585413334511623568L;

    private String database;

    private String jdbcDriver;

    private String jdbcString;

    private String jdbcUser;

    private String jdbcPassword;

    private int maxConns;

    private int minConns;

    private long timeOut;

    /** Parameterless constructor required for serialization */
    public ConnectMessage() {
    }

    /**
     * @param messageType
     */
    protected ConnectMessage(int messageType) {
        super(messageType);
    }

    /**
     * @return the database name
     */
    @Override
    public String getDatabase() {
        return database;
    }

    /**
     * @param database the database name
     */
    @Override
    public void setDatabase(String database) {
        this.database = database;
    }

    /**
     * @return the fully qualified name of JDBC class of the driver
     */
    @Override
    public String getJdbcDriver() {
        return jdbcDriver;
    }

    /**
     * @return the JDBC URI
     */
    @Override
    public String getJdbcString() {
        return jdbcString;
    }

    /**
     * @return the user name
     */
    @Override
    public String getJdbcUser() {
        return jdbcUser;
    }

    /**
     * @return the password
     */
    @Override
    public String getJdbcPassword() {
        return jdbcPassword;
    }

    /**
     * @return the max number of connections in the pool 
     */
    @Override
    public int getMaxConns() {
        return maxConns;
    }

    /**
     * @return the min number of connections in the pool
     */
    @Override
    public int getMinConns() {
        return minConns;
    }

    /**
     * @return the timeout
     */
    @Override
    public long getTimeOut() {
        return timeOut;
    }

    /**
     * @param string the fully qualified name of JDBC class of the driver
     */
    @Override
    public void setJdbcDriver(String string) {
        jdbcDriver = string;
    }

    /**
     * @param string the JDBC URI
     */
    @Override
    public void setJdbcString(String string) {
        jdbcString = string;
    }

    /**
     * @param string the user name 
     */
    @Override
    public void setJdbcUser(String string) {
        jdbcUser = string;
    }

    /**
     * @param string the password
     */
    @Override
    public void setJdbcPassword(String string) {
        jdbcPassword = string;
    }

    /**
     * @param maxConns the max number of connections in the pool
     */
    @Override
    public void setMaxConns(int maxConns) {
        this.maxConns = maxConns;
    }

    /**
     * @param minConns the min number of connections in the pool
     */
    @Override
    public void setMinConns(int minConns) {
        this.minConns = minConns;
    }

    /**
     * @param timeOut the timeout
     */
    @Override
    public void setTimeOut(long timeOut) {
        this.timeOut = timeOut;
    }
}
