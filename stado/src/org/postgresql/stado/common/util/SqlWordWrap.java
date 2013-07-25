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
 * SqlWordWrap.java
 *
 * 
 */
package org.postgresql.stado.common.util;

import java.util.Arrays;

import org.postgresql.stado.parser.Lexer;


/**
 *
 * Class for doing a decent job of wrapping SQL statements in a readable
 *  format, to make it easier when debugging. 
 * 
 * This could be further improved by identing based on nested subqueries and 
 * parens.
 *
 * 
 */
public class SqlWordWrap {
    
    private static final int indentFactor = 6;
    
    /** Creates a new instance of SqlWordWrap */
    public SqlWordWrap() {
    }
    
    
    public static String wrap (String inString, int maxLength) {

        if (inString == null) return null;
                        
        int spaceCount = 0;
        int charCount = 0;
        int parenDepth = 0;
        
        boolean lastPunc = false;
        StringBuilder sbOutput = new StringBuilder();       
        

        try {
            Lexer aLexer = new Lexer (inString);
        
            while (aLexer.hasMoreTokens()) {
                String token = aLexer.nextToken();

                if (token.equalsIgnoreCase("SELECT")) {
                    if (sbOutput.length() > 0) { sbOutput.append('\n'); }
                    sbOutput.append(padSpace(parenDepth*indentFactor));                
                    sbOutput.append("SELECT ");
                    spaceCount = 7;
                    charCount = 7 + parenDepth*indentFactor;
                } else {
                    String[] keywords = {"FROM","INNER","OUTER","WHERE","GROUP","ORDER","UNION"};
                    int newSpaceCount = checkKeywords (token, keywords, sbOutput, parenDepth);
                    if (newSpaceCount > 0) {
                        spaceCount = newSpaceCount;                                    
                        lastPunc = false;
                    } else {
                        if (token.length() + charCount >= maxLength) {
                            sbOutput.append('\n');
                            charCount = parenDepth*indentFactor + spaceCount;  
                            sbOutput.append (padSpace(parenDepth*indentFactor + spaceCount));
                        }

                        if (token.equals(",") && parenDepth == 0) {
                            sbOutput.append(token);
                            sbOutput.append('\n');
                            charCount = parenDepth*indentFactor + spaceCount;  
                            sbOutput.append (padSpace(parenDepth*indentFactor + spaceCount));                        
                            lastPunc = false;
                        } else if (token.equals(".")) {
                            sbOutput.append(token);
                            charCount++;
                            lastPunc = true;
                        }                  
                        else if (token.equals("(")) {
                            sbOutput.append(' ');
                            sbOutput.append(token);
                            charCount += 2;
                            parenDepth++;
                            lastPunc = true;
                        } else if (token.equals(")")) {
                            sbOutput.append(token);
                            sbOutput.append(' ');
                            charCount += token.length() + 1;
                            parenDepth--;
                            lastPunc = true;                        
                        } else {
                            if (!lastPunc) {
                                sbOutput.append(' ');
                                charCount++;
                            }
                            sbOutput.append(token);                   
                            charCount += token.length();
                            lastPunc = false;
                        }
                    }
                }

            }        
            return sbOutput.toString();
        } catch (Exception e) {
            // Something went wrong trying to format this nicely.
            // Just return original string.
            return inString;
        }
        
    }
    
    
    /**
     *
     */
    private static int checkKeywords (String token, String[] keywords, StringBuilder sbOutput, int depth) {
        
        for (String keyword : keywords) {
            if (token.equalsIgnoreCase(keyword)) {
                sbOutput.append('\n');
                if (depth > 0) {
                    sbOutput.append(padSpace(depth*indentFactor));
                }
                sbOutput.append (keyword);
                return depth*indentFactor + keyword.length();                        
            }      
        }        
        return 0;
    }
    
    
    /**
     * Pad a number of spaces
     */                
    private static String padSpace (int length) {
        char chars[] = new char[length];
        Arrays.fill(chars, ' ');
        return new String(chars);
    }
}
