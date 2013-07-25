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
 */
package org.postgresql.stado.common.util;

import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.postgresql.stado.engine.io.XMessage;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBServerException;


/**
 *
 */
public class ParseCmdLine {
    /**
     * Used for substituting variables in command templates
     */
    public static String substitute(String sourceString,
            Map<String, String> valueMap) {
        Iterator<Map.Entry<String, String>> itMap = valueMap.entrySet()
                .iterator();

        while (itMap.hasNext()) {
            Map.Entry<String, String> entry = itMap.next();
            sourceString = sourceString.replace("{" + entry.getKey() + "}", ""
                    + entry.getValue());
        }

        return sourceString;
    }

    public static String[] splitCmdLine(String cmd) {
        if (cmd == null) {
            throw new XDBServerException("Missed cmd string");
        }
        StringTokenizer st = new StringTokenizer(cmd, XMessage.ARGS_DELIMITER);
        String[] result = new String[st.countTokens()];
        for (int i = 0; st.hasMoreTokens(); i++) {
            result[i] = st.nextToken();
        }
        return result;
    }

    public static String stripEscapes(String source) {
        StringBuffer result = new StringBuffer();
        boolean backslash = false;
        for (char ch : source.toCharArray()) {
            if (backslash) {
                switch (ch) {
                case '\\':
                    result.append("\\");
                    break;
                case 'n':
                    result.append("\n");
                    break;
                case 'r':
                    result.append("\r");
                    break;
                case 't':
                    result.append("\t");
                    break;
                case 'b':
                    result.append("\b");
                    break;
                case 'f':
                    result.append("\f");
                    break;
                case '"':
                    result.append("\"");
                    break;
                case '\'':
                    result.append("'");
                    break;
                default:
                    result.append(ch);
                }
                backslash = false;
            } else {
                if (ch == '\\') {
                    backslash = true;
                } else {
                    result.append(ch);
                }
            }
        }
        return result.toString();
    }

    public static String escape(String source) {
        // pass delimiter as null to avoid escaping of delimiter char that
        // otherwise is required in context of loader
        return escape(source, -1, true);
    }

    /**
     * Escape any escape characters in the input string. Also escape the field
     * delimiter char in the input string if a custom delimiter is specified.
     * @param source The source string
     * @param delimiter Field delimiter string
     * @return String with escape chars and field delimiter char in the source
     *         string escaped.
     */
    public static String escape(String source, int delimiter,
            boolean escapeQuote) {

        /*
         * If the source has any chars that need to be escaped, allocate a
         * new buffer and copy into it.
         * Allocate a new buffer iff source has any chars that need to
         * be esacaped.
         * Allocate enough so that the java buffer manager need not re-allocate
         * the buffer. Worst case is that all chars in the string need to be
         * escaped, resulting in twice the source length
         */
        int     currpos = 0;
        StringBuffer result = null;
        // the default delimiter in COPY format is tab '\t'
        boolean escapeDelimiter = false;
        // check if the user specified a custom delimiter
        if (delimiter != -1) {
            escapeDelimiter = true;
        }

        for (char ch : source.toCharArray()) {
            switch (ch) {
            case '\\':
                if (result == null) {
                    result = new StringBuffer(2 * source.length() + 1);
                    result.append(source.subSequence(0, currpos));
                }
                result.append("\\\\");
                break;
            case '\n':
                if (result == null) {
                    result = new StringBuffer(2 * source.length() + 1);
                    result.append(source.subSequence(0, currpos));
                }
                result.append("\\n");
                break;
            case '\r':
                if (result == null) {
                    result = new StringBuffer(2 * source.length() + 1);
                    result.append(source.subSequence(0, currpos));
                }
                result.append("\\r");
                break;
            case '\t':
                if (result == null) {
                    result = new StringBuffer(2 * source.length() + 1);
                    result.append(source.subSequence(0, currpos));
                }
                result.append("\\t");
                break;
            case '\b':
                if (result == null) {
                    result = new StringBuffer(2 * source.length() + 1);
                    result.append(source.subSequence(0, currpos));
                }
                result.append("\\b");
                break;
            case '\f':
                if (result == null) {
                    result = new StringBuffer(2 * source.length() + 1);
                    result.append(source.subSequence(0, currpos));
                }
                result.append("\\f");
                break;
            case '\'':
                if (escapeQuote) {
                    if (result == null) {
                        result = new StringBuffer(2 * source.length() + 1);
                        result.append(source.subSequence(0, currpos));
                    }
                    result.append("''");
                    break;
                }
                // Fall through to default otherwise
            default:
                if (result != null) {
                    if (escapeDelimiter && ch == delimiter) {
                        result.append("\\");
                        result.append(delimiter);
                    } else {
                        result.append(ch);
                    }
                }
                else if (escapeDelimiter && ch == delimiter) {
                    result = new StringBuffer(2 * source.length() + 1);
                    result.append(source.subSequence(0, currpos));
                    result.append("\\");
                    result.append(delimiter);
                }
            }
            currpos++;
        }
        if (result != null) {
            return result.toString();
        } else {
            return source;
        }
    }

    /**
     * String replacement helper method will replace ignoring case
     *
     * @param orig the original string
     * @param o the string to search and replace
     * @param n the new string to replace with
     * @return
     */
    public static String replace(String orig, String o, String n) {

        if (orig == null) {
            return null;
        }
        StringBuffer origSB = new StringBuffer(orig);
        replace(origSB, o, n, true);
        String newString = origSB.toString();

        return newString;
    }

    /**
     * String replacement method, using a StringBuffer, and also ignoring case
     *
     * @param orig the original string
     * @param o the string to search and replace
     * @param n the new string to replace with*
     */
    // ----------------------------------------------------------------------
    public static void replace(StringBuffer orig, String o, String n,
            boolean all) {
        if (orig == null || o == null || o.length() == 0 || n == null) {
            throw new XDBServerException(
                    ErrorMessageRepository.ILLEGAL_PARAMETER, 0,
                    ErrorMessageRepository.ILLEGAL_PARAMETER_CODE);
        }

        int i = 0;
        while (i + o.length() <= orig.length()) {
            if (orig.substring(i, i + o.length()).equalsIgnoreCase(o)) {
                orig.replace(i, i + o.length(), n);
                if (!all) {
                    break;
                } else {
                    i += n.length();
                }
            } else {
                i++;
            }
        }
    }
    
    /**
     * Strip domain info from the user
     * @param user
     * @return stripped user info
     */
    public static String stripDomain(String user) {
        if (user.lastIndexOf("\\") > -1) {
            user = user.substring(user.lastIndexOf("\\")+1); 
        }
        if (user.indexOf('@') > -1) {
            user = user.substring(0,user.indexOf('@'));                        
        }
        return user;
    }
}
