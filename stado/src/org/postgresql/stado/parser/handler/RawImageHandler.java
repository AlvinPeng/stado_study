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
package org.postgresql.stado.parser.handler;

import org.postgresql.stado.parser.core.syntaxtree.NodeToken;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;

/**
 * Traverse syntax tree and returns image of tokens it is built of as a String.
 * Image is not exact: there is exactly one witespace between tokens, it does
 * not matter what was in the original string.
 * 
 * 
 */
public class RawImageHandler extends DepthFirstVoidArguVisitor {
    private String image;

    /**
     * 
     */
    public RawImageHandler() {
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.Parser.ParserCore.visitor.ObjectDepthFirst#visit(org.postgresql.stado.Parser.ParserCore.syntaxtree.NodeToken,
     *      java.lang.Object)
     */
    @Override
    public void visit(NodeToken n, Object argu) {
        if (image == null) {
            image = n.tokenImage;
        } else {
            image += " " + n.tokenImage;
        }
    }

    /**
     * 
     * @return 
     */
    public String getImage() {
        return image;
    }
}
