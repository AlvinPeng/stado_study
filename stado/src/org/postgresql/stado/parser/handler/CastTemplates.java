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
 * CastTemplates.java
 *
 *  
 */
package org.postgresql.stado.parser.handler;

import java.sql.Types;

import org.postgresql.stado.common.util.Property;


/**
 * This class defines different constants that are associated with the configuration
 * attributes. Each constant has a default value that is applicable in case an attribute
 * is not explicitly set in the configuration file.
 *
 *  
 */
public class CastTemplates implements TypeConstants {

    private static final String DefaultTemplate = Property.get("xdb.cast.default.map", "cast({arg} as {type})");

    private static final String[][] TemplatesForCast = {
            {
                    Property.get("xdb.cast.integer.integer.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.integer.numeric.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.integer.char.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.integer.timestamp.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.integer.date.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.integer.time.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.integer.boolean.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.integer.null.map",
                            "cast({arg} as {type})"),

            },
            {
                    Property.get("xdb.cast.numeric.integer.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.numeric.numeric.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.numeric.char.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.numeric.timestamp.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.numeric.date.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.numeric.time.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.numeric.boolean.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.numeric.null.map",
                            "cast({arg} as {type})"),

            },
            {
                    Property.get("xdb.cast.char.integer.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.char.numeric.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.char.char.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.char.timestamp.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.char.date.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.char.time.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.char.boolean.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.char.null.map",
                            "cast({arg} as {type})"),

            },
            {
                    Property.get("xdb.cast.timestamp.integer.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.timestamp.numeric.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.timestamp.char.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.timestamp.timestamp.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.timestamp.date.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.timestamp.time.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.timestamp.boolean.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.timestamp.null.map",
                            "cast({arg} as {type})"),

            },
            {
                    Property.get("xdb.cast.date.integer.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.date.numeric.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.date.char.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.date.timestamp.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.date.date.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.date.time.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.date.boolean.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.date.null.map",
                            "cast({arg} as {type})"),

            },
            {
                    Property.get("xdb.cast.time.integer.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.time.numeric.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.time.char.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.time.timestamp.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.time.date.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.time.time.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.time.boolean.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.time.null.map",
                            "cast({arg} as {type})"),

            },
            {
                    Property.get("xdb.cast.boolean.integer.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.boolean.numeric.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.boolean.char.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.boolean.timestamp.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.boolean.date.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.boolean.time.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.boolean.boolean.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.boolean.null.map",
                            "cast({arg} as {type})"),

            },
            {
                    Property.get("xdb.cast.null.integer.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.null.numeric.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.null.char.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.null.timestamp.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.null.date.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.null.time.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.null.boolean.map",
                            "cast({arg} as {type})"),
                    Property.get("xdb.cast.null.null.map",
                            "cast({arg} as {type})"),

            },
    };

    /**
     * It returns the relevant index for the specific type as defined in
     * the 2-dim TemplatesForCast array in this class. For example the
     * type cast template for the character types are defined at index 2.
     *
     * @return An int value representing the array index.
     */
    private static int getIndex(int javaSqlType) {
        switch (javaSqlType) {
        case Types.BIT:
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
        case Types.BIGINT:
            return 0;
        case Types.FLOAT:
        case Types.REAL:
        case Types.DOUBLE:
        case Types.NUMERIC:
        case Types.DECIMAL:
            return 1;
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            return 2;
        case Types.TIMESTAMP:
            return 3;
        case Types.DATE:
            return 4;
        case Types.TIME:
            return 5;
        case Types.BOOLEAN:
            return 6;
        case Types.NULL:
            return 7;

        default:
            return -1;
        }

    }

    /**
     *
     * @param aFromType
     * @param aToType
     * @return
     */
    public static String getTemplate(int aFromType, int aToType) {
        int fromIndex = getIndex(aFromType);
        int toIndex = getIndex(aToType);
        return fromIndex < 0 || toIndex < 0 ? DefaultTemplate : TemplatesForCast[fromIndex][toIndex];
    }

}
