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

import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.parser.core.syntaxtree.AliasName;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;

/**
 * This Alias Spec handler is used for holding information about alias name.
  */
public class AliasSpecHandler extends DepthFirstVoidArguVisitor {

    private String aliasName;

    /**
     * It returns the alias name.
     *
     * @return A string which is the alias name
     * @throws XDBServerException
     *             If the alias name is null we throw an alias not found
     *             exception though this thing should not occur as the parser
     *             will throw an exception if the signature does not match a
     *             Alias Specification
     */

    public String getAliasName() {
        return aliasName;
    }

    /**
     * Grammar production:
     * f0 -> Identifier(prn)
     * @param n
     * @param argu
     * @return
     */
    @Override
    public void visit(AliasName n, Object argu) {
        aliasName = (String) n.f0.accept(new IdentifierHandler(), argu);
    }
}
