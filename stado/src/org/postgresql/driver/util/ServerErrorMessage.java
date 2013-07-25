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
package org.postgresql.driver.util;

import java.util.Hashtable;
import java.io.Serializable;

public class ServerErrorMessage implements Serializable
{

    private static final Character SEVERITY = new Character('S');
    private static final Character MESSAGE = new Character('M');
    private static final Character DETAIL = new Character('D');
    private static final Character HINT = new Character('H');
    private static final Character POSITION = new Character('P');
    private static final Character WHERE = new Character('W');
    private static final Character FILE = new Character('F');
    private static final Character LINE = new Character('L');
    private static final Character ROUTINE = new Character('R');
    private static final Character SQLSTATE = new Character('C');
    private static final Character INTERNAL_POSITION = new Character('p');
    private static final Character INTERNAL_QUERY = new Character('q');

    private final Hashtable m_mesgParts = new Hashtable();
    private final int verbosity;

    public ServerErrorMessage(String p_serverError, int verbosity)
    {
        this.verbosity = verbosity;

        char[] l_chars = p_serverError.toCharArray();
        int l_pos = 0;
        int l_length = l_chars.length;
        while (l_pos < l_length)
        {
            char l_mesgType = l_chars[l_pos];
            if (l_mesgType != '\0')
            {
                l_pos++;
                int l_startString = l_pos;
                while (l_chars[l_pos] != '\0' && l_pos < l_length)
                {
                    l_pos++;
                }
                String l_mesgPart = new String(l_chars, l_startString, l_pos - l_startString);
                m_mesgParts.put(new Character(l_mesgType), l_mesgPart);
            }
            l_pos++;
        }
    }

    public String getSQLState()
    {
        return (String)m_mesgParts.get(SQLSTATE);
    }

    public String getMessage()
    {
        return (String)m_mesgParts.get(MESSAGE);
    }

    public String getSeverity()
    {
        return (String)m_mesgParts.get(SEVERITY);
    }

    public String getDetail()
    {
        return (String)m_mesgParts.get(DETAIL);
    }

    public String getHint()
    {
        return (String)m_mesgParts.get(HINT);
    }

    public int getPosition()
    {
        return getIntegerPart(POSITION);
    }

    public String getWhere()
    {
        return (String)m_mesgParts.get(WHERE);
    }

    public String getFile()
    {
        return (String)m_mesgParts.get(FILE);
    }

    public int getLine()
    {
        return getIntegerPart(LINE);
    }

    public String getRoutine()
    {
        return (String)m_mesgParts.get(ROUTINE);
    }

    public String getInternalQuery()
    {
        return (String)m_mesgParts.get(INTERNAL_QUERY);
    }

    public int getInternalPosition()
    {
        return getIntegerPart(INTERNAL_POSITION);
    }

    private int getIntegerPart(Character c)
    {
        String s = (String)m_mesgParts.get(c);
        if (s == null)
            return 0;
        return Integer.parseInt(s);
    }

    public String toString()
    {
        //Now construct the message from what the server sent
        //The general format is:
        //SEVERITY: Message \n
        //  Detail: \n
        //  Hint: \n
        //  Position: \n
        //  Where: \n
        //  Internal Query: \n
        //  Internal Position: \n
        //  Location: File:Line:Routine \n
        //  SQLState: \n
        //
        //Normally only the message and detail is included.
        //If INFO level logging is enabled then detail, hint, position and where are
        //included.  If DEBUG level logging is enabled then all information
        //is included.

        StringBuffer l_totalMessage = new StringBuffer();
        String l_message = (String)m_mesgParts.get(SEVERITY);
        if (l_message != null)
            l_totalMessage.append(l_message).append(": ");
        l_message = (String)m_mesgParts.get(MESSAGE);
        if (l_message != null)
            l_totalMessage.append(l_message);
        l_message = (String)m_mesgParts.get(DETAIL);
        if (l_message != null)
            l_totalMessage.append("\n  ").append(GT.tr("Detail: {0}", l_message));

        l_message = (String)m_mesgParts.get(HINT);
        if (l_message != null)
            l_totalMessage.append("\n  ").append(GT.tr("Hint: {0}", l_message));
        l_message = (String)m_mesgParts.get(POSITION);
        if (l_message != null)
            l_totalMessage.append("\n  ").append(GT.tr("Position: {0}", l_message));
        l_message = (String)m_mesgParts.get(WHERE);
        if (l_message != null)
            l_totalMessage.append("\n  ").append(GT.tr("Where: {0}", l_message));

        if (verbosity > 2)
        {
            String l_internalQuery = (String)m_mesgParts.get(INTERNAL_QUERY);
            if (l_internalQuery != null)
                l_totalMessage.append("\n  ").append(GT.tr("Internal Query: {0}", l_internalQuery));
            String l_internalPosition = (String)m_mesgParts.get(INTERNAL_POSITION);
            if (l_internalPosition != null)
                l_totalMessage.append("\n  ").append(GT.tr("Internal Position: {0}", l_internalPosition));

            String l_file = (String)m_mesgParts.get(FILE);
            String l_line = (String)m_mesgParts.get(LINE);
            String l_routine = (String)m_mesgParts.get(ROUTINE);
            if (l_file != null || l_line != null || l_routine != null)
                l_totalMessage.append("\n  ").append(GT.tr("Location: File: {0}, Routine: {1}, Line: {2}", new Object[] {l_file, l_routine, l_line}));
            l_message = (String)m_mesgParts.get(SQLSTATE);
            if (l_message != null)
                l_totalMessage.append("\n  ").append(GT.tr("Server SQLState: {0}", l_message));
        }

        return l_totalMessage.toString();
    }
}
