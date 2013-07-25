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
package org.postgresql.driver.jdbc3;

import java.sql.SQLException;
import java.sql.Savepoint;

import org.postgresql.driver.core.Utils;
import org.postgresql.driver.util.GT;
import org.postgresql.driver.util.PSQLException;
import org.postgresql.driver.util.PSQLState;

public class PSQLSavepoint implements Savepoint {

    private boolean _isValid;
    private boolean _isNamed;
    private int _id;
    private String _name;

    public PSQLSavepoint(int id) {
        _isValid = true;
        _isNamed = false;
        _id = id;
    }

    public PSQLSavepoint(String name) {
        _isValid = true;
        _isNamed = true;
        _name = name;
    }

    public int getSavepointId() throws SQLException {
        if (!_isValid)
            throw new PSQLException(GT.tr("Cannot reference a savepoint after it has been released."),
                                    PSQLState.INVALID_SAVEPOINT_SPECIFICATION);

        if (_isNamed)
            throw new PSQLException(GT.tr("Cannot retrieve the id of a named savepoint."),
                                    PSQLState.WRONG_OBJECT_TYPE);

        return _id;
    }

    public String getSavepointName() throws SQLException {
        if (!_isValid)
            throw new PSQLException(GT.tr("Cannot reference a savepoint after it has been released."),
                                    PSQLState.INVALID_SAVEPOINT_SPECIFICATION);

        if (!_isNamed)
            throw new PSQLException(GT.tr("Cannot retrieve the name of an unnamed savepoint."),
                                    PSQLState.WRONG_OBJECT_TYPE);

        return _name;
    }

    public void invalidate() {
        _isValid = false;
    }

    public String getPGName() throws SQLException {
        if (!_isValid)
            throw new PSQLException(GT.tr("Cannot reference a savepoint after it has been released."),
                                    PSQLState.INVALID_SAVEPOINT_SPECIFICATION);

        if (_isNamed)
        {
            // We need to quote and escape the name in case it
            // contains spaces/quotes/etc.
            //
            return Utils.appendEscapedIdentifier(null, _name).toString();
        }

        return "JDBC_SAVEPOINT_" + _id;
    }

}
