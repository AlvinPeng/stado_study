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
 * INodeWriterFactory.java
 *
 *
 */
package org.postgresql.stado.engine.loader;

import java.io.IOException;
import java.util.Map;

import org.postgresql.stado.engine.loader.Loader.DATA_SOURCE;
import org.postgresql.stado.metadata.NodeDBConnectionInfo;


/**
 *
 */
public interface INodeWriterFactory {

    public void setNodeConnectionInfos(
            NodeDBConnectionInfo[] nodeDBConnectionInfos);

    public NodeDBConnectionInfo[] getNodeConnectionInfos();

    public INodeWriter createWriter(int node) throws IOException;

    public void setParams(DATA_SOURCE ds, Map<String,String> params);

    public void close();
}
