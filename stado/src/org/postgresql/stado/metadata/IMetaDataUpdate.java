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
 * IMetaDataUpdate.java
 *
 *  
 */

package org.postgresql.stado.metadata;

import org.postgresql.stado.engine.XDBSessionContext;

/**
 * Interface IMetaDataUpdate provides the methods that that are called to update
 * the MetaData DB and refresh the cache from
 * Engine.executeDDLOnMultipleNodes().
 * 
 * RESTRICTIONS - Never call beginTransaction(), commitTransaction() or
 * rollbackTransaction() on MetaData, from callses implementing this interface.
 * 
 * This interface has been created solely for executing DDL commands on MetaData
 * DB.
 * 
 */
public interface IMetaDataUpdate {
    public void execute(XDBSessionContext client) throws Exception;

    public void refresh() throws Exception;
}
