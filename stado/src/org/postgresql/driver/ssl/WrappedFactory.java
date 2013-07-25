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

import java.io.IOException;
import java.net.Socket;
import java.net.InetAddress;

import javax.net.ssl.SSLSocketFactory;

/**
 * Provide a wrapper to a real SSLSocketFactory delegating all calls
 * to the contained instance.  A subclass needs only provide a
 * constructor for the wrapped SSLSocketFactory.
 */
public abstract class WrappedFactory extends SSLSocketFactory {

    protected SSLSocketFactory _factory;

    public Socket createSocket(InetAddress host, int port) throws IOException {
        return _factory.createSocket(host, port);
    }

    public Socket createSocket(String host, int port) throws IOException {
        return _factory.createSocket(host, port);
    }

    public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException {
        return _factory.createSocket(host, port, localHost, localPort);
    }

    public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException {
        return _factory.createSocket(address, port, localAddress, localPort);
    }

    public Socket createSocket(Socket socket, String host, int port, boolean autoClose) throws IOException {
        return _factory.createSocket(socket, host, port, autoClose);
    }

    public String[] getDefaultCipherSuites() {
        return _factory.getDefaultCipherSuites();
    }

    public String[] getSupportedCipherSuites() {
        return _factory.getSupportedCipherSuites();
    }

}
