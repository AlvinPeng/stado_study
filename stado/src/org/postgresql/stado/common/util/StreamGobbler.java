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
package org.postgresql.stado.common.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

import org.apache.log4j.Level;

/**
 *
 *         should read from external process' std out or std err in separate
 *         thread to workaround hanging issue on some platforms. Output can be
 *         optionally redirected to other stream
 */
public class StreamGobbler extends Thread {
    private BufferedReader in;
    private XLogger logger;
    private Level level;
    
    private String streamValue = "";

    /**
     *
     */
    public StreamGobbler(InputStream input, XLogger logger, Level level)
            throws IOException {
        // Note: we do not use BufferedInputReader bacause it may have problem
        // if input stream does not have end of line character
        in = new BufferedReader(new InputStreamReader(input));
        this.level = level == null ? Level.INFO : level;
        if (logger != null && logger.isEnabledFor(level)) {
            this.logger = logger;
        }
    }

    @Override
    public void run() {
        try {
            try {
                String read;
                while ((read = in.readLine()) != null) {
                    streamValue = streamValue + read;
                    if (logger != null) {
                        logger.log(level, read, null);
                    }
                }
                in.close();
            } finally {
                in.close();
            }
        } catch (IOException ignore) {
        }
    }
    
    public String getStreamValue() {
    	return streamValue;
    }
}
