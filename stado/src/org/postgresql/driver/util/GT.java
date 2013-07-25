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

import java.text.MessageFormat;
import java.util.ResourceBundle;
import java.util.MissingResourceException;

/**
 * This class provides a wrapper around a gettext message catalog that
 * can provide a localized version of error messages.  The caller provides
 * a message String in the standard java.text.MessageFormat syntax and any
 * arguments it may need.  The returned String is the localized version if
 * available or the original if not.
 */

public class GT {

    private final static GT _gt = new GT();
    private final static Object noargs[] = new Object[0];

    public final static String tr(String message) {
        return _gt.translate(message, null);
    }

    public final static String tr(String message, Object arg) {
        return _gt.translate(message, new Object[]{arg});
    }

    public final static String tr(String message, Object args[]) {
        return _gt.translate(message, args);
    }


    private ResourceBundle _bundle;

    private GT() {
        try
        {
            _bundle = ResourceBundle.getBundle("org.postgresql.driver.translation.messages");
        }
        catch (MissingResourceException mre)
        {
            // translation files have not been installed
            _bundle = null;
        }
    }

    private final String translate(String message, Object args[])
    {
        if (_bundle != null && message != null)
        {
            try
            {
                message = _bundle.getString(message);
            }
            catch (MissingResourceException mre)
            {
                // If we can't find a translation, just
                // use the untranslated message.
            }
        }

        // If we don't have any parameters we still need to run
        // this through the MessageFormat(ter) to allow the same
        // quoting and escaping rules to be used for all messages.
        //
        if (args == null) {
            args = noargs;
        }

        // Replace placeholders with arguments
        //
        if (message != null)
        {
            message = MessageFormat.format(message, args);
        }

        return message;
    }

}

