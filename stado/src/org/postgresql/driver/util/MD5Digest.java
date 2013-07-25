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

/**
 * MD5-based utility function to obfuscate passwords before network 
 * transmission.
 *
 * @author Jeremy Wohl
 */

import java.security.*;

public class MD5Digest
{
    private MD5Digest()
    {
    }


    /*
     * Encodes user/password/salt information in the following way:
     *  MD5(MD5(password + user) + salt)
     *
     * @param user  The connecting user.
     * @param password The connecting user's password.
     * @param salt  A four-salt sent by the server.
     *
     * @return A 35-byte array, comprising the string "md5" and an MD5 digest.
     */
    public static byte[] encode(byte user[], byte password[], byte salt[])
    {
        MessageDigest md;
        byte[] temp_digest, pass_digest;
        byte[] hex_digest = new byte[35];

        try
        {
            md = MessageDigest.getInstance("MD5");

            md.update(password);
            md.update(user);
            temp_digest = md.digest();

            bytesToHex(temp_digest, hex_digest, 0);
            md.update(hex_digest, 0, 32);
            md.update(salt);
            pass_digest = md.digest();

            bytesToHex(pass_digest, hex_digest, 3);
            hex_digest[0] = (byte) 'm';
            hex_digest[1] = (byte) 'd';
            hex_digest[2] = (byte) '5';
        }
        catch (Exception e)
        {
            ; // "MessageDigest failure; " + e
        }

        return hex_digest;
    }


    /*
     * Turn 16-byte stream into a human-readable 32-byte hex string
     */
    private static void bytesToHex(byte[] bytes, byte[] hex, int offset)
    {
        final char lookup[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
                                'a', 'b', 'c', 'd', 'e', 'f' };

        int i, c, j, pos = offset;

        for (i = 0; i < 16; i++)
        {
            c = bytes[i] & 0xFF;
            j = c >> 4;
            hex[pos++] = (byte) lookup[j];
            j = (c & 0xF);
            hex[pos++] = (byte) lookup[j];
        }
    }
}
