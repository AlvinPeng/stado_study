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
package org.postgresql.driver.core.v3;

import java.lang.ref.PhantomReference;

import org.postgresql.driver.core.*;

/**
 * V3 ResultCursor implementation in terms of backend Portals.
 * This holds the state of a single Portal. We use a PhantomReference
 * managed by our caller to handle resource cleanup.
 *
 * @author Oliver Jowett (oliver@opencloud.com)
 */
class Portal implements ResultCursor {
    Portal(SimpleQuery query, String portalName) {
        this.query = query;
        this.portalName = portalName;
        this.encodedName = Utils.encodeUTF8(portalName);
    }

    public void close() {
        if (cleanupRef != null)
        {
            cleanupRef.clear();
            cleanupRef.enqueue();
            cleanupRef = null;
        }
    }

    String getPortalName() {
        return portalName;
    }

    byte[] getEncodedPortalName() {
        return encodedName;
    }

    SimpleQuery getQuery() {
        return query;
    }

    void setCleanupRef(PhantomReference cleanupRef) {
        this.cleanupRef = cleanupRef;
    }

    public String toString() {
        return portalName;
    }

    // Holding on to a reference to the generating query has
    // the nice side-effect that while this Portal is referenced,
    // so is the SimpleQuery, so the underlying statement won't
    // be closed while the portal is open (the backend closes
    // all open portals when the statement is closed)

    private final SimpleQuery query;
    private final String portalName;
    private final byte[] encodedName;
    private PhantomReference cleanupRef;
}
