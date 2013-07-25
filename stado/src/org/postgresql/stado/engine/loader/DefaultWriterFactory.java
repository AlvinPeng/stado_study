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
 * DefaultWriterFactory.java
 *
 *
 */
package org.postgresql.stado.engine.loader;

import java.io.IOException;
import java.util.Map;

import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.engine.loader.Loader.DATA_SOURCE;
import org.postgresql.stado.metadata.NodeDBConnectionInfo;


/**
 *
 */
public class DefaultWriterFactory implements INodeWriterFactory {
    private NodeDBConnectionInfo[] nodeDBConnectionInfos;

    private String template;

    private Map<String,String> params;

    /**
     *
     */
    public DefaultWriterFactory() {
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Util.loader.ILoadWriterFactory#createWriter(int)
     */
    public INodeWriter createWriter(int nodeID) throws IOException {
        if (nodeDBConnectionInfos == null) {
            throw new IOException(
                    "Can not create Writer: no database connection info");
        }
        for (NodeDBConnectionInfo element : nodeDBConnectionInfos) {
            if (element.getNodeID() == nodeID) {
                INodeWriter writer = new DefaultWriter(element, template, params);
                return writer;
            }
        }
        throw new IOException(
                "Can not create Writer: no database connection info for Node "
                        + nodeID);
    }

    public void close() {
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Util.loader.INodeWriterFactory#getNodeConnectionInfos()
     */
    public NodeDBConnectionInfo[] getNodeConnectionInfos() {
        return nodeDBConnectionInfos;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Util.loader.INodeWriterFactory#setNodeConnectionInfos(org.postgresql.stado..MetaData.NodeDBConnectionInfo[])
     */
    public void setNodeConnectionInfos(
            NodeDBConnectionInfo[] nodeDBConnectionInfos) {
        this.nodeDBConnectionInfos = nodeDBConnectionInfos;
    }

    /* (non-Javadoc)
     * @see org.postgresql.stado.engine.loader.INodeWriterFactory#setParams(java.util.Map)
     */
    public void setParams(DATA_SOURCE ds, Map<String, String> params) {
        if (ds == DATA_SOURCE.CSV) {
            template = Props.XDB_LOADER_NODEWRITER_CSV_TEMPLATE;
        } else {
            template = Props.XDB_LOADER_NODEWRITER_TEMPLATE;
        }
        this.params = params;
    }
}
