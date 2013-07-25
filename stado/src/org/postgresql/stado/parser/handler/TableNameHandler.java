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
package org.postgresql.stado.parser.handler;

import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.parser.core.syntaxtree.NodeSequence;
import org.postgresql.stado.parser.core.syntaxtree.TableName;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;

/**
 * This class provides the parser-level handling for a table name that appears
 * in a query.
 */
public class TableNameHandler extends DepthFirstVoidArguVisitor {

    private String tableName;

    private String referenceName;

    private boolean isTemporary;
    
    private boolean isOnly;

    private XDBSessionContext client;

    public TableNameHandler(XDBSessionContext client) {
        this.client = client;
    }

    /**
     * It returns the table name represented by tableName attribute.
     * @return A String value represented by tableName
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * It returns the table reference name represented by referenceName attribute.
     * @return A String value represented by referenceName.
     */
    public String getReferenceName() {
        return referenceName;
    }

    /**
     * It returns isTemporary flag that indicates if the table is temporary or not.
     * @return A boolean value represented by isTemporary flag.
     */
    public boolean isTemporary() {
        return isTemporary;
    }
    
    /**
     * It returns isOnly flag that indicates if the table includes
     * only itself and not derived tables
     * @return A boolean value represented by isTemporary flag.
     */
    public boolean isOnly() {
        return isOnly;
    }

    /**
     * Setter
     */
    public void setOnly (boolean isOnly) {
        this.isOnly = isOnly;
    }
    
    /**
     * Grammar production:
     * f0 -> ( Identifier(prn) | <TEMPDOT_> Identifier(prn) 
     * | <PUBLICDOT_> Identifier(prn) | <QPUBLICDOT_> Identifer(prn)
     * )
     */
    @Override
    public void visit(TableName n, Object argu) {
        IdentifierHandler identifierHandler = new IdentifierHandler();
        switch (n.f0.which) {
            case 0:
            {
                referenceName = (String) n.f0.choice.accept(identifierHandler, argu);
                tableName = client.getTempTableName(referenceName);
                if (tableName == null) {
                    tableName = referenceName;
                    isTemporary = false;
                } else {
                    isTemporary = true;
                }
                break;
            }
            case 1:
            {
                tableName = "TEMP.";
                NodeSequence ns = (NodeSequence) n.f0.choice;
                tableName += (String) ns.elementAt(1).accept(identifierHandler, argu);
                referenceName = tableName;
                isTemporary = true;
                break;
            }
            // handle public.* case
            case 2:
            case 3:
            {
                n.f0.choice.accept(identifierHandler, argu);
                referenceName = identifierHandler.getIdentifier();
                tableName = identifierHandler.getIdentifier();
                isTemporary = false;
                break;
            }
        }
    }
}
