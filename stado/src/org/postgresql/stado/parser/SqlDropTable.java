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
 *
 *
 */

package org.postgresql.stado.parser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.XDBSecurityException;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.SyncDropTable;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysForeignKey;
import org.postgresql.stado.metadata.SysLogin;
import org.postgresql.stado.metadata.SysReference;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.SysView;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.parser.core.syntaxtree.INode;
import org.postgresql.stado.parser.core.syntaxtree.dropTable;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.IdentifierHandler;
import org.postgresql.stado.parser.handler.TableNameHandler;


public class SqlDropTable extends DepthFirstVoidArguVisitor implements IXDBSql,
        IPreparable {
    private XDBSessionContext client;

    private SysDatabase database;

    private List<SysTable> sysTableList;

    private String dropTableSQL;

    private LinkedList<SysTable> refs = null;

    private List<Boolean> tempList;

    private List<String> tableList;

    private List<String> referenceList;

    public SqlDropTable(XDBSessionContext client) {
        this.client = client;
        database = client.getSysDatabase();
        tableList = new ArrayList<String>();
        referenceList = new ArrayList<String>();
        tempList = new ArrayList<Boolean>();
    }

    /**
     * Grammar production: f0 -> <DROP_> f1 -> <TABLE_> f2 -> TableName(prn) f3
     * -> ( "," TableName(prn) )*
     */
    @Override
    public void visit(dropTable n, Object argu) {
        n.f0.accept(this, argu);
        n.f1.accept(this, argu);
        TableNameHandler aTableNameHandler = new TableNameHandler(client);
        n.f2.accept(aTableNameHandler, argu);
        tableList.add(aTableNameHandler.getTableName());
        referenceList.add(aTableNameHandler.getReferenceName());
        tempList.add(aTableNameHandler.isTemporary());

        for (Object node : n.f3.nodes) {
            ((INode) node).accept(aTableNameHandler, argu);
            tableList.add(aTableNameHandler.getTableName());
            referenceList.add(aTableNameHandler.getReferenceName());
            tempList.add(aTableNameHandler.isTemporary());
        }
    }

    public long getCost() {
        return LOW_COST;
    }

    public Collection<SysTable> getRelatedTables() {
        if (refs == null) {
            refs = new LinkedList<SysTable>();
            if (!isPrepared()) {
                try {
                    prepare();
                } catch (Exception ex) {
                    // Ignore
                }
            }
            for (SysTable table : sysTableList) {
                Vector<SysReference> vRefFkList = table.getSysFkReferenceList();
                Vector<SysReference> vRefRefList = table.getSysReferences();

                for (SysReference aReferringTab : vRefFkList) {
                    refs.add(database.getSysTable(aReferringTab.getRefTableID()));
                }

                for (SysReference aReferringTab : vRefRefList) {
                    SysForeignKey aFkey = (SysForeignKey) aReferringTab
                            .getForeignKeys().elementAt(0);
                    SysTable refTable = aFkey.getReferringSysColumn(database)
                            .getSysTable();
                    refs.add(refTable);
                }
            }
        }
        return refs;
    }

    /**
     * Send back the lock specification that we should send back
     * 
     * @param aMetadata
     * @return
     */
    public LockSpecification<SysTable> getLockSpecs() {
        return new LockSpecification<SysTable>(getRelatedTables(), sysTableList);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.Parser.IXDBSql#getNodeList()
     */
    public Collection<DBNode> getNodeList() {
        HashSet<DBNode> nodes = new HashSet<DBNode>();
        for (SysTable table : sysTableList) {
            if (table != null) {
                nodes.addAll(table.getNodeList());
            }
        }
        for (Object element : getRelatedTables()) {
            nodes.addAll(((SysTable) element).getNodeList());
        }
        return nodes;
    }

    public List<String> getTablename() {
        return tableList;
    }

    /**
     * @return
     */
    public List<Boolean> isTempTable() {
        return tempList;
    }

    /**
     * @return Returns the referenceName.
     */
    public List<String> getReferenceName() {
        return referenceList;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.Engine.IPreparable#isPrepared()
     */
    public boolean isPrepared() {
        return sysTableList != null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.Engine.IPreparable#prepare()
     */
    public void prepare() throws Exception {
        try {
            int sz = tableList.size();
            sysTableList = new ArrayList<SysTable>(sz);
            for (int i = 0; i < sz; i++) {
                String tablename = tableList.get(i);
                SysTable table = database.getSysTable(tablename);
                sysTableList.add(table);

                Enumeration eViews = database.getAllViews();
                while (eViews.hasMoreElements()) {
                    SysView view = (SysView) eViews.nextElement();
                    if (view.hasDependedTable(table.getTableId())) {
                        XDBSecurityException ex = new XDBSecurityException(
                                "cannot drop table " + tablename
                                        + " because other objects depend on it");
                        throw ex;
                    }
                }
                if (client.getCurrentUser().getUserClass() != SysLogin.USER_CLASS_DBA
                        && table.getOwner() != client.getCurrentUser()) {
                    XDBSecurityException ex = new XDBSecurityException(
                            "You are not allowed to drop table " + tablename);
                    throw ex;
                }
                if (table.getChildrenTables().size() > 0) {
                    throw new XDBServerException("Table "
                            + referenceList.get(i)
                            + " has children tables. Please, remove them first");
                }
                Collection<SysReference> references = table.getSysReferences();
                if (references != null && !references.isEmpty()) {
                    throw new XDBServerException(
                            "Table "
                                    + referenceList.get(i)
                                    + " has foreign references. Please, remove them first");
                }

                if (0 == i) {
                    dropTableSQL = "DROP TABLE "
                            + IdentifierHandler.quote(tablename);
                } else {
                    dropTableSQL += "," + IdentifierHandler.quote(tablename);
                }
            }
        } finally {
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.postgresql.stado.Engine.IExecutable#execute(org.postgresql.stado.Engine.Engine)
     */
    public ExecutionResult execute(Engine engine) throws Exception {
        try {

            engine.executeDDLOnMultipleNodes(dropTableSQL, getNodeList(),
                    new SyncDropTable(this), client);
            int sz = tempList.size();
            for (int i = 0; i < sz; i++) {
                if (tempList.get(i)) {
                    client.deregisterTempTableWithSession(referenceList.get(i));
                }
            }
            // Table dropped successfully, so do not try and release table lock
            sysTableList.clear();
            return ExecutionResult
                    .createSuccessResult(ExecutionResult.COMMAND_DROP_TABLE);

        } finally {
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.postgresql.stado.MetaData.Scheduler.ILockCost#needCoordinatorConnection()
     */
    public boolean needCoordinatorConnection() {
        return false;
    }

	@Override
	public boolean isReadOnly() {
		// TODO Auto-generated method stub
		return false;
	}
}
