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
package org.postgresql.driver.ssl;

import java.util.Properties;
import java.io.IOException;
import java.net.Socket;
import java.lang.reflect.Constructor;
import javax.net.ssl.SSLSocketFactory;

import org.postgresql.driver.core.Logger;
import org.postgresql.driver.core.PGStream;
import org.postgresql.driver.util.GT;
import org.postgresql.driver.util.PSQLException;
import org.postgresql.driver.util.PSQLState;

public class MakeSSL {
    public static void convert(PGStream stream, Properties info, Logger logger) throws IOException, PSQLException {
        logger.debug("converting regular socket connection to ssl");

        SSLSocketFactory factory;

        // Use the default factory if no specific factory is requested
        //
        String classname = info.getProperty("sslfactory");
        if (classname == null)
        {
            factory = (SSLSocketFactory)SSLSocketFactory.getDefault();
        }
        else
        {
            Object[] args = {info.getProperty("sslfactoryarg")};
            Constructor ctor;
            Class factoryClass;

            try
            {
                factoryClass = Class.forName(classname);
                try
                {
                    ctor = factoryClass.getConstructor(new Class[]{String.class});
                }
                catch (NoSuchMethodException nsme)
                {
                    ctor = factoryClass.getConstructor((Class[])null);
                    args = null;
                }
                factory = (SSLSocketFactory)ctor.newInstance(args);
            }
            catch (Exception e)
            {
                throw new PSQLException(GT.tr("The SSLSocketFactory class provided {0} could not be instantiated.", classname), PSQLState.CONNECTION_FAILURE, e);
            }
        }

        Socket newConnection = factory.createSocket(stream.getSocket(), stream.getHost(), stream.getPort(), true);
        stream.changeSocket(newConnection);
    }

}


