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

import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.parser.core.syntaxtree.Identifier;
import org.postgresql.stado.parser.core.syntaxtree.NodeToken;
import org.postgresql.stado.parser.core.syntaxtree.UnreservedWords;
import org.postgresql.stado.parser.core.visitor.DepthFirstRetArguVisitor;

/**
 * @author amart
 *
 */
public class IdentifierHandler extends DepthFirstRetArguVisitor {

    public static String quote(String identifier) {
        StringBuffer sbQuoted = new StringBuffer(identifier.length() * 2 + 2);
        sbQuoted.append(Props.XDB_IDENTIFIER_QUOTE_OPEN);
        int pos = 0;
        for (int quotePos = identifier.indexOf(Props.XDB_IDENTIFIER_QUOTE_CLOSE);
                quotePos != -1; quotePos = identifier.indexOf(Props.XDB_IDENTIFIER_QUOTE_CLOSE, pos)) {
            if (quotePos > pos) {
                sbQuoted.append(identifier.substring(pos, quotePos - 1));
            }
            sbQuoted.append(Props.XDB_IDENTIFIER_QUOTE_ESCAPE);
            sbQuoted.append(Props.XDB_IDENTIFIER_QUOTE_CLOSE);
            pos = quotePos + 1;
        }
        sbQuoted.append(identifier.substring(pos));
        sbQuoted.append(Props.XDB_IDENTIFIER_QUOTE_CLOSE);
        return sbQuoted.toString();
    }

    public static String stripQuotes(String identifier) {
        if (identifier.length() >= 2 && identifier.startsWith(Props.XDB_IDENTIFIER_QUOTE_OPEN)
                && identifier.endsWith(Props.XDB_IDENTIFIER_QUOTE_CLOSE)) {
            identifier = identifier.substring(1, identifier.length() - 1);
            StringBuffer newImage = new StringBuffer();
            int pos = 0;
            for (int quotePos = identifier.indexOf(Props.XDB_IDENTIFIER_QUOTE_OPEN); quotePos != -1;
                    quotePos = identifier.indexOf(Props.XDB_IDENTIFIER_QUOTE_OPEN, pos)) {
                int endPos = identifier.indexOf(Props.XDB_IDENTIFIER_QUOTE_CLOSE);
                if (endPos >= 0) {                    
                    newImage.append(identifier.substring(pos, endPos + 1));
                }
                pos = quotePos + 1 + Props.XDB_IDENTIFIER_QUOTE_CLOSE.length();
            }
            newImage.append(identifier.substring(pos));
            return newImage.toString();
        }
        return identifier;
    }

    public static String normalizeCase(String identifier) {
        if (Props.XDB_IDENTIFIER_CASE == Props.XDB_IDENTIFIER_CASE_LOWER) {
            return identifier.toLowerCase();
        } else if (Props.XDB_IDENTIFIER_CASE == Props.XDB_IDENTIFIER_CASE_UPPER) {
            return identifier.toUpperCase();
        } else {
            return identifier;
        }
    }

    private String identifier = null;

    /**
     * Grammar production:
     * f0 -> <IDENTIFIER>
     *       | <QUOTED_IDENTIFIER>
     *       | UnreservedWords(prn)
     */
    @Override
    public Object visit(Identifier n, Object argu) {
        switch (n.f0.which) {
        case 0:
            identifier = normalizeCase(((NodeToken) n.f0.choice).tokenImage);
            break;
        case 1:
            identifier = stripQuotes(((NodeToken) n.f0.choice).tokenImage);
            break;
        case 2:
            identifier = normalizeCase((String) n.f0.choice.accept(this, argu));
            break;
        }
        return identifier;
    }

    /**
     * Grammar production:
     * f0 -> <POSITION_>
     *       | <DATE_>
     *       | <DAY_>
     *  -snip-
     *       | <REGEXP_REPLACE_>
     */
    @Override
    public Object visit(UnreservedWords n, Object argu) {
        return ((NodeToken) n.f0.choice).tokenImage;
    }

    public String getIdentifier() {
        return identifier;
    }

}
