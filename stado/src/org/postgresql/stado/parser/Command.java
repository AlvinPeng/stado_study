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
package org.postgresql.stado.parser; 

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.optimizer.SqlExpression;
import org.postgresql.stado.parser.handler.QueryTreeTracker;


/**
 * 
 */
public class Command {

    public static final int SELECT = 1;

    public static final int UPDATE = 2;

    public static final int INSERT = 3;

    public static final int CREATE = 4;

    public static final int DELETE = 5;

    public static final int DROP = 6;

    public static final int ALTER = 7;

    public static final int MODIFY = 8;
    
    public static final int FETCH = 9;

    private XDBSessionContext client;

    // Command object
    private Object commandObj;

    // command ID
    private int commandToExecute;

    // This tracks the query trees in this particular command
    private QueryTreeTracker aQueryTreeTracker;

    private ArrayList<SqlExpression> parameters;

    // Other information - which could pertain to
    public Command(int commandToExecute, Object obj,
            QueryTreeTracker aQueryTreeTracker, XDBSessionContext client) {
        this.commandToExecute = commandToExecute;
        this.commandObj = obj;
        this.aQueryTreeTracker = aQueryTreeTracker;
        this.client = client;
    }

    /**
     * This will return the command object
     * 
     * @return
     */
    public Object getCommandObj() {
        return commandObj;
    }
    
    /**
     * This will return the ID to execute
     * 
     * @return
     */
    public int getCommandToExecute() {
        return commandToExecute;
    }

    /**
     * This will return the query tree tracker
     * 
     * @return
     */
    public QueryTreeTracker getaQueryTreeTracker() {
        return aQueryTreeTracker;
    }

    /**
     * Get the client session
     */
    public XDBSessionContext getClientContext() {
        return client;
    }

    /**
     * Get the database name
     */
    public String getDBName() {
        return client.getDBName();
    }

    /**
     * @param index
     * @param sqlExpression
     */
    public void registerParameter(int index, SqlExpression sqlExpression) {
        if (parameters == null) {
            parameters = new ArrayList<SqlExpression>();
        }
        parameters.ensureCapacity(index);
        while (index > parameters.size()) {
            parameters.add(null);
        }
        parameters.set(index - 1, sqlExpression);
    }

    public SqlExpression getParameter(int index) {
        return parameters.get(index - 1);
    }

    public List<SqlExpression> getParameters() {
        return parameters == null ? null
                : Collections.unmodifiableList(parameters);
    }

    public int getParamCount() {
        return parameters == null ? 0 : parameters.size();
    }
}
