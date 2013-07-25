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
 * SqlCreateDatabase.java
 *
 *
 */

package org.postgresql.stado.parser;

import java.sql.ResultSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.engine.io.MessageTypes;
import org.postgresql.stado.exception.XDBSecurityException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.MetaData;
import org.postgresql.stado.metadata.NodeDBConnectionInfo;
import org.postgresql.stado.metadata.SysLogin;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.parser.core.syntaxtree.CreateDatabase;
import org.postgresql.stado.parser.core.syntaxtree.INode;
import org.postgresql.stado.parser.core.syntaxtree.NodeSequence;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.IdentifierHandler;
import org.postgresql.stado.util.DbGateway;

/**
 *
 *
 */
public class SqlCreateDatabase extends DepthFirstVoidArguVisitor implements IXDBSql,
        IPreparable {
    private static final XLogger logger = XLogger
            .getLogger(SqlCreateDatabase.class);

    private XDBSessionContext client;

    private boolean prepared = false;

    private String[] nodeList;

    private String dbName;

    private boolean manual = false;

    private boolean spatial = false;

    /** Creates a new instance of SqlCreateDatabase */
    public SqlCreateDatabase(XDBSessionContext client) {
        this.client = client;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IPreparable#isPrepared()
     */
    public boolean isPrepared() {
        return prepared;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Parser.IXDBSql#getNodeList()
     */
    public Collection<DBNode> getNodeList() {
        return Collections.emptyList();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#getCost()
     */
    public long getCost() {
        return LOW_COST;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#getLockSpecs()
     */
    public LockSpecification<SysTable> getLockSpecs() {
        Collection<SysTable> empty = Collections.emptyList();
        return new LockSpecification<SysTable>(empty, empty);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#needCoordinatorConnection()
     */
    public boolean needCoordinatorConnection() {
        return false;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IPreparable#prepare()
     */
    public void prepare() throws Exception {
        final String method = "prepare";
        logger.entering(method);
        try {

            if (!prepared) {
                if (client.getCurrentUser().getLogin().getUserClass() != SysLogin.USER_CLASS_DBA) {
                    throw new XDBSecurityException(
                            "You are not allowed to create database");
                }                
            }
            prepared = true;

        } finally {
            logger.exiting(method);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IExecutable#execute(org.postgresql.stado.Engine.Engine)
     */
    public ExecutionResult execute(Engine engine) throws Exception {
        final String method = "execute";
        logger.entering(method, new Object[] { engine });
        try {

            if (!isPrepared()) {
                prepare();
            }

            XDBSessionContext newClient = XDBSessionContext.createSession();
            try {
                newClient.useDB(dbName, MessageTypes.CONNECTION_MODE_CREATE);
                newClient.login(client.getCurrentUser().getLogin(), spatial);
                ExecutionResult result = newClient.createDatabase(nodeList);
                if (!manual) {
                    NodeDBConnectionInfo[] connectionInfos = newClient
                            .getConnectionInfos(nodeList);
                    HashMap<String, String> valueMap = new HashMap<String, String>();
                    DbGateway aGwy = new DbGateway();
                    aGwy.createDbOnNodes(valueMap, connectionInfos);
                    aGwy.addStadoSchemaOnNodes(valueMap, connectionInfos);
                    
                    if (spatial) {
                    	aGwy.addSpatialOnNodes(valueMap, connectionInfos);
                    }
                }
                ExecutionResult er = newClient.persistDatabase();
                
                if (spatial) {
	                long dbid = er.getRangeStart();
	                int ownerid = client.getCurrentUser().getUserID();
	
	                String sql = "INSERT INTO xsystables (dbid, tablename, numrows, partscheme, owner)" +
	                " VALUES (" + dbid + ", 'geometry_columns', 3750, 2, " + ownerid + ") " +
	                " RETURNING tableid ";
	
	                ResultSet keys = MetaData.getMetaData().executeUpdateReturning(sql);
                    int tableID;
                    if (keys.next()) {
                        tableID = keys.getInt(1);
                    } else {
                        throw new Exception("Error creating table");
                    }
				
                    String sqlHeader = "INSERT INTO xsyscolumns (tableid,  colseq, colname, coltype, "
                            + "collength, isnullable, isserial, "
                            + "checkexpr, nativecoldef) VALUES ";
				   
				    sql = sqlHeader
                            + "("
                            + tableID
                            + ", 1, 'f_table_catalog', 12, 256, 0, 0, null, '\"f_table_catalog\" VARCHAR (256) NOT NULL')";
                    MetaData.getMetaData().executeUpdate(sql);
				
                    sql = sqlHeader
                            + "("
                            + tableID
                            + ", 2, 'f_table_schema', 12, 256, 0, 0, null, '\"f_table_schema\" VARCHAR (256) NOT NULL')";
                    MetaData.getMetaData().executeUpdate(sql);
				
                    sql = sqlHeader
                            + "("
                            + tableID
                            + ", 3, 'f_table_name', 12, 256, 0, 0, null, '\"f_table_name\" VARCHAR (256) NOT NULL')";
                    MetaData.getMetaData().executeUpdate(sql);
				
                    sql = sqlHeader
                            + "("
                            + tableID
                            + ", 4, 'f_geometry_column', 12, 256, 0, 0, null, '\"f_geometry_column\" VARCHAR (256) NOT NULL')";
                    MetaData.getMetaData().executeUpdate(sql);
				
                    sql = sqlHeader
                            + "("
                            + tableID
                            + ", 5, 'coord_dimension', 4, null, 0, 0, null, '\"coord_dimension\" INT NOT NULL')";
                    MetaData.getMetaData().executeUpdate(sql);
	
                    sql = sqlHeader
                            + "("
                            + tableID
                            + ", 6, 'srid', 4, null, 0, 0, null, '\"srid\" INT NOT NULL')";
                    MetaData.getMetaData().executeUpdate(sql);
	
                    sql = sqlHeader
                            + "("
                            + tableID
                            + ", 7, 'type', 12, 30, 0, 0, null, '\"type\" VARCHAR (30) NOT NULL')";
                    MetaData.getMetaData().executeUpdate(sql);
	
	                
                    sql = "INSERT INTO xsystables (dbid, tablename, numrows, partscheme, owner)"
                            + " VALUES ("
                            + dbid
                            + ", 'spatial_ref_sys', 3750, 2, "
                            + ownerid
                            + ") "
                            + " RETURNING tableid ";
                    keys = MetaData.getMetaData().executeUpdateReturning(sql);
                    tableID = 0;
	                if (keys.next()) {
	                	tableID = keys.getInt(1);
	                } else {
	                	throw new Exception("Error creating table");
	                }
	
	                sql = sqlHeader + "(" + tableID + ", 1, 'srid', 4, null, 0, 0, null, '\"srid\" INT NOT NULL')";
	                MetaData.getMetaData().executeUpdate(sql);
	
	                sql = sqlHeader + "(" + tableID + ", 2, 'auth_name', 12, 256, 1, 0, null, '\"auth_name\" VARCHAR (256)')";
	                MetaData.getMetaData().executeUpdate(sql);
	
	                sql = sqlHeader + "(" + tableID + ", 3, 'auth_srid', 4, null, 1, 0, null, '\"auth_srid\" INT')";
	                MetaData.getMetaData().executeUpdate(sql);
	
	                sql = sqlHeader + "(" + tableID + ", 4, 'srtext', 12, 2048, 1, 0, null, '\"srtext\" VARCHAR (2048)')";
	                MetaData.getMetaData().executeUpdate(sql);
	
	                sql = sqlHeader + "(" + tableID + ", 5, 'proj4text', 12, 2048, 1, 0, null, '\"proj4text\" VARCHAR (2048)')";
	                MetaData.getMetaData().executeUpdate(sql);
	                
	                // NEED to resync metadata
                }
                
                return result;
            } finally {
                newClient.logout();
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Grammar production:
     * f0 -> <CREATE_DB_>
     * f1 -> Identifier(prn)
     * f2 -> [ [ <WITH_> ] ( <OWNER_> [ "=" ] Identifier(prn) | <SPATIAL_> )+ ]
     * f3 -> [ <MANUAL_> ]
     * f4 -> <ON_>
     * f5 -> ( <NODE_> | <NODES_> )
     * f6 -> <INT_LITERAL>
     * f7 -> ( "," <INT_LITERAL> )*
     */
    @Override
    public void visit(CreateDatabase n, Object argu) {
        dbName = (String) n.f1.accept(new IdentifierHandler(), argu);

        if (n.f2.present()) {
            // TODO Database ownership is not supported at the moment
        	// and need to check if this is for spatial
        	spatial = true;
        }
        manual = n.f3.present();
        nodeList = new String[n.f7.size() + 1];
        int i = 0;
        nodeList[i++] = n.f6.tokenImage;
        Iterator<INode> nodeEnum = n.f7.elements();
        while (nodeEnum.hasNext()) {
            Object nextNode = nodeEnum.next();
            Object actualNode = ((NodeSequence) nextNode).nodes.get(1);
            nodeList[i++] = actualNode.toString();
        }
    }

	@Override
	public boolean isReadOnly() {
		// TODO Auto-generated method stub
		return false;
	}
}
