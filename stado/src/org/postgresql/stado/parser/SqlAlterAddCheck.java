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
 * SqlAlterAddPrimary.java
 *
 *
 */

package org.postgresql.stado.parser;

import java.util.HashMap;

import org.postgresql.stado.common.util.ParseCmdLine;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.metadata.SyncAlterTableCheck;
import org.postgresql.stado.metadata.SysPermission;
import org.postgresql.stado.parser.core.syntaxtree.CheckDef;
import org.postgresql.stado.parser.core.syntaxtree.Constraint;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.IdentifierHandler;
import org.postgresql.stado.parser.handler.QueryConditionHandler;
import org.postgresql.stado.parser.handler.QueryTreeTracker;

/**
 * Class for adding a CHECK constraint to a table
 * 
 * 
 */

public class SqlAlterAddCheck extends DepthFirstVoidArguVisitor implements IPreparable {
    private static final XLogger logger = XLogger
            .getLogger(SqlAlterAddCheck.class);

    private XDBSessionContext client;

    private SqlAlterTable parent;

    private String constraintName = null;

    private String checkDef;

    private String[] commands;

    /**
     * @param table
     * @param client
     */
    public SqlAlterAddCheck(SqlAlterTable parent, XDBSessionContext client) {
        this.client = client;
        this.parent = parent;
    }

    /**
     * Grammar production:
     * f0 -> <CONSTRAINT_>
     * f1 -> Identifier(prn)
     */
    @Override
    public void visit(Constraint n, Object argu) {
        constraintName = (String) n.f1.accept(new IdentifierHandler(), argu);
    }

    @Override
    public void visit(CheckDef n, Object argu) {
        QueryConditionHandler qch = new QueryConditionHandler(new Command(
                Command.CREATE, this, new QueryTreeTracker(), client));
        n.f2.accept(qch, argu);
        checkDef = qch.aRootCondition.rebuildString();
    }

    public String getConstraintName() {
        return constraintName;
    }

    public String getCheckDef() {
        return checkDef;
    }

    public SqlAlterTable getParent() {
        return parent;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IPreparable#isPrepared()
     */
    public boolean isPrepared() {
        return commands != null;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IPreparable#prepare()
     */
    public void prepare() throws Exception {
        final String method = "prepare";
        logger.entering(method, new Object[] {});
        try {

            parent.getTable().ensurePermission(client.getCurrentUser(),
                    SysPermission.PRIVILEGE_ALTER);
            if (constraintName == null) {
                constraintName = "CHK_" + parent.getTableName().toUpperCase();
            }
            HashMap<String, String> arguments = new HashMap<String, String>();
            arguments.put("table",
                    IdentifierHandler.quote(parent.getTableName()));
            arguments.put("constr_name",
                    IdentifierHandler.quote(constraintName));
            arguments.put("check_def", checkDef);

            String sql = ParseCmdLine.substitute(
                    Props.XDB_SQLCOMMAND_ALTERTABLE_ADDCHECK, arguments);
            if (Props.XDB_SQLCOMMAND_ALTERTABLE_ADDCHECK_TO_PARENT) {
                parent.addCommonCommand(sql);
                commands = new String[0];
            } else {
                commands = new String[1];
                commands[0] = sql;
            }
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
        logger.entering(method, new Object[] {});
        try {
            engine.executeDDLOnMultipleNodes(commands, parent.getNodeList(),
                    new SyncAlterTableCheck(this), client);
            return null;

        } finally {
            logger.exiting(method);
        }
    }

	@Override
	public boolean isReadOnly() {
		// TODO Auto-generated method stub
		return false;
	}
}
