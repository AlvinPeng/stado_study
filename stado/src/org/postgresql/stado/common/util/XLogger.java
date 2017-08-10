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
 * XLogger.java
 *
 *
 */

package org.postgresql.stado.common.util;


import java.util.StringTokenizer;

import org.apache.log4j.Level;
import org.apache.log4j.Appender;

/**
 * log4j wrapper class which has a lower "trace" level for detailed debugging.
 * This class is based on some logic from jboss.org
 *
 *
 */
public class XLogger implements java.io.Serializable {
    /**
     *
     */
    private static final long serialVersionUID = -217487181061734376L;

    private String name = null;

    private transient org.apache.log4j.Logger log;

    /** Creates a new instance of XLogger */
    private XLogger() {
    }

    private XLogger(String name) {
        this.name = name;
        log = org.apache.log4j.Logger.getLogger(name);
    }

    // factory methods
    public static XLogger getLogger(String name) {
        return new XLogger(name);
    }

    public static XLogger getLogger(Class clazz) {
        return getLogger(clazz.getName());
    }

    public void supertrace(Object message) {
        log.log(XLevel.SUPERTRACE, message);
    }

    public void supertrace(Object message, Throwable t) {
        log.log(XLevel.SUPERTRACE, message, t);
    }

    public void trace(Object message) {
        log.log(XLevel.TRACE, message);
    }

    public void trace(Object message, Throwable t) {
        log.log(XLevel.TRACE, message, t);
    }

    public void debug(Object message) {
        log.log(Level.DEBUG, message);
    }

    public void debug(Object message, Throwable t) {
        log.log(Level.DEBUG, message, t);
    }

    public void info(Object message) {
        log.log(Level.INFO, message);
    }

    public void info(Object message, Throwable t) {
        log.log(Level.INFO, message, t);
    }

    public void warn(Object message) {
        log.log(Level.WARN, message);
    }

    public void warn(Object message, Throwable t) {
        log.log(Level.WARN, message, t);
    }

    public void error(Object message) {
        log.log(Level.ERROR, message);
    }

    public void error(Object message, Throwable t) {
        log.log(Level.ERROR, message, t);
    }

    public void fatal(Object message) {
        log.log(Level.FATAL, message);
    }

    public void fatal(Object message, Throwable t) {
        log.log(Level.FATAL, message, t);
    }

    // convenient methods to determine the level before making the method call

    public boolean isEnabledFor(Level level) {
        return log.isEnabledFor(level);
    }

    public boolean isSuperTraceEnabled() {
        return log.isEnabledFor(XLevel.SUPERTRACE);
    }

    public boolean isTraceEnabled() {
        return log.isEnabledFor(XLevel.TRACE);
    }

    public boolean isDebugEnabled() {
        return log.isEnabledFor(Level.DEBUG);
    }

    public boolean isInfoEnabled() {
        return log.isEnabledFor(Level.INFO);
    }

    public boolean isWarnEnabled() {
        return log.isEnabledFor(Level.WARN);
    }

    public boolean isErrorEnabled() {
        return log.isEnabledFor(Level.ERROR);
    }

    public boolean isFatalEnabled() {
        return log.isEnabledFor(Level.FATAL);
    }

    // adheres to the Serializable interface
    private void writeObject(java.io.ObjectOutputStream out)
            throws java.io.IOException {
    }

    private void readObject(java.io.ObjectInputStream stream)
            throws java.io.IOException, ClassNotFoundException {
        // Restore logging
        log = org.apache.log4j.Logger.getLogger(name);
    }

    // methods to trace execution flow
    private static final Object ENTERING = "Entering method: ";

    private static final Object EXITING = "Exiting method: ";

    private static final Object RETURNING = "Returning value from method: ";

    private static final Object CATCHING = "Catching throwable: ";

    private static final Object THROWING = "Throwing throwable: ";

    public void entering(String method, Object[] args) {
        if (isSuperTraceEnabled()) {
            StringBuffer buf = new StringBuffer();
            buf.append(ENTERING).append(method).append("(");
            if (args != null) {
                for (int i = 0; i < args.length; i++) {
                    if (i != 0) {
                        buf.append(", ");
                    }
                    buf.append(args[i]);
                }
            }
            buf.append(")");
            supertrace(buf);
        }
    }

    public void entering(String method) {
        entering(method, null);
    }

    public void exiting(String method, Object returnValue) {
        if (isSuperTraceEnabled()) {
            StringBuffer buf = new StringBuffer();
            buf.append(RETURNING).append(returnValue).append(";")
                    .append(method).append("()");
            supertrace(buf);
        }
    }

    public void exiting(String method) {
        if (isSuperTraceEnabled()) {
            StringBuffer buf = new StringBuffer();
            buf.append(EXITING).append(method).append("()");
            supertrace(buf);
        }
    }

    public void catching(Throwable t) {
        if (isErrorEnabled()) {
            error(CATCHING, t);
            if (isDebugEnabled()) {
                StringBuffer buf = new StringBuffer(); 
                buf.append(t.getMessage()).append(System.getProperty("line.separator"));
                buf.append(getStackTrace(t));
                error(buf);
            }
        }
    }

    public void throwing(Throwable t) {
        if (isErrorEnabled()) {
            error(THROWING, t);
            if (isDebugEnabled()) {
                StringBuffer buf = new StringBuffer(); 
                buf.append(t.getMessage()).append(System.getProperty("line.separator"));
                buf.append(getStackTrace(t));             
                error(buf);
            }
        }
    }

    private String format(String template, Object[] params) {
        StringBuffer sb = new StringBuffer();
        StringTokenizer st = new StringTokenizer(template, "%");
        while (st.hasMoreTokens()) {
            sb.append(st.nextToken());
            if (st.hasMoreTokens()) {
                String idxStr = st.nextToken();
                if ("".equals(idxStr)) {
                    sb.append("%");
                }
                try {
                    int idx = Integer.parseInt(idxStr);
                    sb.append(params[idx]);
                } catch (Exception e) {
                    sb.append("%").append(idxStr).append("%");
                }
            }
        }
        return sb.toString();
    }

    public void log(Level level, String template, Object[] params) {
        if (log.isEnabledFor(level)) {
            if (params == null) {
                log.log(level, template);
            } else {
                log.log(level, format(template, params));
            }
        }
    }
    
    public static String getStackTrace(Throwable t)
    {
        final StringBuilder result = new StringBuilder();
        
        for (StackTraceElement element : t.getStackTrace()) {
            result.append(' ')
                    .append(element)
                    .append(System.getProperty("line.separator"));
        }
        
        return result.toString();
    }
}
