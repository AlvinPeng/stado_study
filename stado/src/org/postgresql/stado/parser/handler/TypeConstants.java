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
 * TypeConstants.java
 * 
 *  
 */
package org.postgresql.stado.parser.handler;

import org.postgresql.stado.common.util.Property;

/**
 * This class defines different constants that are associated with the configuration
 * attributes as specified in the xdb.config file. Each constant has a default value
 * that is applicable in case an attribute is not explicitly set in the configuration file.
 *
 *  
 */
public interface TypeConstants {
    public static final String INTEGER_TEMPLATE = Property.get(
            "xdb.sqltype.integer.map", "INT");

    public static final String BIGINTEGER_TEMPLATE = Property.get(
            "xdb.sqltype.biginteger.map", "BIGINT");

    public static final String SMALLINT_TEMPLATE = Property.get(
            "xdb.sqltype.smallint.map", "SMALLINT");

    public static final String BOOLEAN_TEMPLATE = Property.get(
            "xdb.sqltype.boolean.map", "BOOLEAN");

    public static final String REAL_TEMPLATE = Property.get(
            "xdb.sqltype.real.map", "REAL");

    public static final String FLOAT_TEMPLATE = Property.get(
            "xdb.sqltype.float.map", "FLOAT ({length})");
    
    public static final String DOUBLE_TEMPLATE = Property.get(
            "xdb.sqltype.double.map", "DOUBLE PRECISION");

    public static final String NUMERIC_TEMPLATE = Property.get(
            "xdb.sqltype.numeric.map", "NUMERIC ({precision}, {scale})");

    public static final String NUMERIC_TEMPLATE_WITHOUT_SCALE = Property.get(
    		"xdb.sqltype.numeric.map", "NUMERIC ({precision})");

    public static final String NUMERIC_TEMPLATE_WITHOUT_PRECISION = Property.get(
            "xdb.sqltype.numeric.map", "NUMERIC");

    public static final String FIXED_TEMPLATE = Property.get(
            "xdb.sqltype.fixed.map", "NUMERIC ({precision}, {scale})");

    public static final String TIMESTAMP_TEMPLATE = Property.get(
            "xdb.sqltype.timestamp.map", "TIMESTAMP");
    
    public static final String TIME_TEMPLATE = Property.get(
            "xdb.sqltype.time.map", "TIME");
    
    public static final String DATE_TEMPLATE = Property.get(
            "xdb.sqltype.date.map", "DATE");

    public static final String CHAR_TEMPLATE = Property.get(
            "xdb.sqltype.char.map", "CHAR ({length})");

    public static final String VARCHAR_TEMPLATE = Property.get(
            "xdb.sqltype.varchar.map", "VARCHAR ({length})");

    public static final String VARBIT_TEMPLATE = Property.get(
            "xdb.sqltype.varbit.map", "BIT VARYING ({length})");

    public static final String VARBIT_TEMPLATE_WITHOUT_LENGTH = Property.get(
            "xdb.sqltype.varbit.map", "VARBIT");

    public static final String BIT_TEMPLATE = Property.get(
            "xdb.sqltype.bit.map", "BIT ({length})");

    public static final String INTERVAL_TEMPLATE = Property.get(
            "xdb.sqltype.interval.map", "INTERVAL");

    public static final String INTERVAL_TEMPLATE_QUALIFIED = Property.get(
            "xdb.sqltype.interval.qualified.map", "INTERVAL {from} TO {to}");

    public static final String TEXT_TEMPLATE = Property.get(
            "xdb.sqltype.text.map", "TEXT");

    public static final String BLOB_TEMPLATE = Property.get(
            "xdb.sqltype.blob.map", "BYTEA");

    public static final String MACADDR_TEMPLATE = Property.get(
            "xdb.sqltype.macaddr.map", "MACADDR");
    
    public static final String CIDR_TEMPLATE = Property.get(
            "xdb.sqltype.cidr.map", "CIDR");

    public static final String INET_TEMPLATE = Property.get(
            "xdb.sqltype.inet.map", "INET");  

    public static final String GEOMETRY_TEMPLATE = Property.get(
            "xdb.sqltype.geometry.map", "GEOMETRY");        

    public static final String BOX2D_TEMPLATE = Property.get(
            "xdb.sqltype.geometry.map", "BOX2D");
    
    public static final String BOX3D_TEMPLATE = Property.get(
            "xdb.sqltype.geometry.map", "BOX3D");
    
    public static final String BOX3DEXTENT_TEMPLATE = Property.get(
            "xdb.sqltype.geometry.map", "BOX3D_EXTENT");        

    public static final String REGCLASS_TEMPLATE = Property.get(
            "xdb.sqltype.geometry.map", "REGCLASS");        
}
