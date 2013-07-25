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
 * Lexer.java
 *
 *
 *
 * The purpose of this class is to be used for manually parsing
 * simple SQL statements.
 *
 * We don't bother breaking things out into a fixed set of tokens;
 * we just return Strings in a convenient manner.
 */

package org.postgresql.stado.parser;

import java.util.LinkedList;

import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.parser.handler.IdentifierHandler;


/**
 *
 *
 */
public class Lexer {

    public static final int UNKNOWN = 0;

    public static final int IDENTIFIER = 1;

    public static final int LITERAL = 2;

    public static final int SIGN = 3;

    public static final int OPERATOR = 4;

    private String lexString;

    private int offset = 0;

    private int length = 0;

    private int tokenType = UNKNOWN;

    private LinkedList<String> peekedTokens = new LinkedList<String>();

    private LinkedList<Integer> peekedTokenTypes = new LinkedList<Integer>();

    /** Creates a new instance of Lexer */
    public Lexer(String s) {
        lexString = s.trim();
        length = lexString != null ? lexString.length() : 0;
    }

    public boolean hasMoreTokens() {
        return !peekedTokens.isEmpty() || offset < length;
    }

    /**
     * Gets the next token. It handles string literals and some simple
     * operators. This could be expanded on and perhaps broken out to get the
     * next token based on state.
     *
     * @return String
     */
    public String nextToken() throws XDBServerException {
        if (peekedTokens.isEmpty()) {
            return parseToken();
        } else {
            tokenType = peekedTokenTypes.remove();
            return peekedTokens.remove();
        }
    }

    /**
     * Look ahead
     * @param skip
     * @return
     * @throws XDBServerException
     */
    public String peekToken(int skip) throws XDBServerException {
        String token = null;
        int saveTokenType = tokenType;
        while (skip >= peekedTokens.size()) {
            try {
                token = parseToken();
            } catch (XDBServerException xe) {
                return null;
            }
            peekedTokenTypes.add(tokenType);
            peekedTokens.add(token);
        }
        tokenType = saveTokenType;
        // if we token is null the requested token already was in the buffer
        return token == null ? peekedTokens.get(skip) : token;
    }

    private String parseToken() throws XDBServerException {
        String nextToken = null;
        tokenType = UNKNOWN;

        if (lexString == null || offset >= length) {
            throw new XDBServerException("No more tokens found.");
        }

        int i = 0;

        // skip spaces
        while (offset < length
                && Character.isWhitespace(lexString.charAt(offset))) {
            offset++;
        }

        // See if we have a quoted table/column
        if (lexString.charAt(offset) == '"') {
            i++;
            while (offset + i < length) {
                if (lexString.charAt(offset + i) == '"') {
                    if (offset + i + 1 < length
                            && lexString.charAt(offset + i + 1) == '"') {
                        // Doubled up quote
                        i += 2;
                        continue;
                    } else {
                        // End of quoted identifier
                        break;
                    }
                }
                i++;
            }

            if (offset == length || offset + i >= length) {
                // malformed " " expression
                throw new XDBServerException("Syntax Error near character "
                        + offset);
            }

            // return with quotes stripped
            nextToken = IdentifierHandler.stripQuotes(lexString.substring(offset, offset + i + 1));
            offset += i + 1;
            tokenType = IDENTIFIER;
            return nextToken;
        }
        
        // See if we have a string constant
        // Allow for Escaped strings
        if (lexString.charAt(offset) == '\''
                || (lexString.charAt(offset) == 'E' && offset + 1 < length && lexString.charAt(offset + 1) == '\'')) {

            if (lexString.charAt(offset) == 'E' && offset + 1 < length && lexString.charAt(offset + 1) == '\'')
            {
                i++;
            }
            i++;
            while (true) {
                while (offset + i < length
                        && lexString.charAt(offset + i) != '\'') {
                    i++;
                }
                // now we peek ahead to see if we have a double ''
                if (offset + i + 1 < length
                        && lexString.charAt(offset + i + 1) == '\'') {
                    i += 2;
                } else {
                    break;
                }
            }

            if (offset == length || offset + i >= length) {
                // malformed " " expression
                throw new XDBServerException("Syntax Error near character "
                        + offset);
            }

            nextToken = lexString.substring(offset, offset + i + 1);
            offset = offset + i + 1;
            tokenType = LITERAL;
            return nextToken;
        }

        for (; offset + i < length; i++) {
            char nextChar = lexString.charAt(offset + i);

            if (Character.isWhitespace(nextChar)) {
                nextToken = lexString.substring(offset, offset + i);
                offset = offset + i;
                break;
            }

            // check for decimals
            if (nextChar == '.') {
                if (i == 0) {
                    tokenType = SIGN;
                    nextToken = lexString.substring(offset, offset + 1);
                } else {
                    nextToken = lexString.substring(offset, offset + i);
                }

                try {
                    Integer.parseInt(nextToken);
                    tokenType = LITERAL;
                } catch (NumberFormatException n) {
                    // not a number
                    if (i == 0) {
                        offset++;
                    } else {
                        offset = offset + i;
                    }
                    return nextToken;
                }

                // Check for number beyond decimal point
                int j;

                for (j = i + 1; offset + j < length; j++) {
                    char decChar = lexString.charAt(offset + j);

                    if (decChar < '0' || decChar > '9') {
                        // if we encountered a non number right away,
                        // it is not a decimal.
                        if (j == i + 1) {
                            nextToken = lexString.substring(offset, offset + i);
                            offset += i;
                            return nextToken;
                        } else {
                            nextToken = lexString.substring(offset, offset + j);
                            offset += j;
                            return nextToken;
                        }
                    }
                }
                // if we encountered a non-number right away,
                if (j == i + 1) {
                    nextToken = lexString.substring(offset, offset + i);
                    offset += i;
                } else {
                    nextToken = lexString.substring(offset, offset + j);
                    offset += j;
                }

                return nextToken;
            }

            if (nextChar == ',' || nextChar == '(' || nextChar == ')'
                    || nextChar == ';' || nextChar == '+' || nextChar == '-'
                        || nextChar == '*' || nextChar == '/' || nextChar == '%') {
                if (i == 0) {
                    tokenType = SIGN;
                    nextToken = lexString.substring(offset, offset + 1);
                    offset++;
                } else {
                    nextToken = lexString.substring(offset, offset + i);
                    offset = offset + i;
                }
                break;
            }

            if (nextChar == '=' || nextChar == '<' || nextChar == '>'
                    || nextChar == '!' || nextChar == '&' || nextChar == '@'
                    || nextChar == '~' || nextChar == '|') {
                if (i > 0) {
                    nextToken = lexString.substring(offset, offset + i);
                    offset = offset + i;
                    break;
                }

                for (i = 1; offset + i < length; i++) {
                    nextChar = lexString.charAt(offset + i);
                    if (nextChar != '=' && nextChar != '<' && nextChar != '>'
                            && nextChar != '!' && nextChar != '&'
                            && nextChar != '@' && nextChar != '~'
                            && nextChar != '|') {
                        break;
                    }
                }
                nextToken = lexString.substring(offset, offset + i);
                tokenType = OPERATOR;
                offset = offset + i;
                return nextToken;
            }
            if (nextChar == ':') {
                if (i > 0) {
                    nextToken = lexString.substring(offset, offset + i);
                    offset = offset + i;
                    break;
                }

                tokenType = SIGN;
                char nextNextChar = lexString.charAt(offset);

                if (nextNextChar == ':') {
                    offset += 2;
                    return "::";
                }

                throw new XDBServerException("Syntax Error near character "
                        + offset);
            }
        }

        if (nextToken == null) {
            nextToken = lexString.substring(offset);
            offset = length;
        }

        return IdentifierHandler.normalizeCase(nextToken);
    }

    /**
     * Check if it is a valid operator (only the simple ones we recognize). Note
     * that if these are changed, the nextToken method should be as well.
     *
     * @param String
     *            operator
     *
     * @return boolean
     */
    public boolean isOperator(String operator) throws XDBServerException {
        if (operator.equals("=")) {
            return true;
        }
        if (operator.equals("<")) {
            return true;
        }
        if (operator.equals(">")) {
            return true;
        }
        if (operator.equals("<=")) {
            return true;
        }
        if (operator.equals(">=")) {
            return true;
        }
        if (operator.equals("<>")) {
            return true;
        }
        if (operator.equals("!=")) {
            return true;
        }
        if (operator.equals("&&")) {
            return true;
        }       
        if (operator.equals("&<")) {
            return true;
        }
        if (operator.equals("&<|")) {
            return true;
        }
        if (operator.equals("&>")) {
            return true;
        }
        if (operator.equals("<<")) {
            return true;
        }
        if (operator.equals("<<|")) {
            return true;
        }
        if (operator.equals(">>")) {
            return true;
        }
        if (operator.equals("@")) {
            return true;
        }
        if (operator.equals("|&>")) {
            return true;
        }
        if (operator.equals("|>>")) {
            return true;
        }
        if (operator.equals("~")) {
            return true;
        }
        if (operator.equals("!~")) {
            return true;
        }
        if (operator.equals("~=")) {
            return true;
        }

        return false;
    }

    /**
     * This is like hasMoreTokens, but just throws an Exception if there are no
     * more.
     */
    public void checkTokens() throws XDBServerException {
        if (!hasMoreTokens()) {
            throw new XDBServerException("Token expected at end of string.");
        }
    }

    public int lastTokenType() {
        return tokenType;
    }
}
