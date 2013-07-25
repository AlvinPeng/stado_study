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
package org.postgresql.stado.optimizer;

import java.sql.Types;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.postgresql.stado.common.util.Property;
import org.postgresql.stado.engine.io.DataTypes;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.parser.Command;
import org.postgresql.stado.parser.ExpressionType;
import org.postgresql.stado.parser.handler.IFunctionID;


/**
 *
 * FunctionAnalysis Class is reponsible for doing the symantic checks for the
 * all the supported functions
 *
 */
public class FunctionAnalysis {

    private enum DATATYPE_MATCH {
        NO_MATCH, CAST_WITH_LOSS, CAST_NO_LOSS, EXACT_MATCH
    }

    private static class FunctionDef {
        String funcName;
        int[] funcParamTypes;
        ExpressionType funcReturnType;

        FunctionDef(String funcName, int[] funcParamTypes,
                ExpressionType funcReturnType) {
            this.funcName = funcName;
            this.funcParamTypes = funcParamTypes;
            this.funcReturnType = funcReturnType;
        }
    }

    private static final FunctionDef[] BUILTIN_FUNCTIONS = {
        new FunctionDef("abbrev", new int[] { ExpressionType.CIDR_TYPE },
                new ExpressionType(Types.CLOB)),
        new FunctionDef("abbrev", new int[] { ExpressionType.INET_TYPE },
                new ExpressionType(Types.CLOB)),
        new FunctionDef("abs", new int[] { Types.BIGINT },
                new ExpressionType(Types.BIGINT)),
        new FunctionDef("abs", new int[] { Types.DOUBLE },
                new ExpressionType(Types.DOUBLE)),
        new FunctionDef("acos", new int[] { Types.DOUBLE }, new ExpressionType(
                Types.DOUBLE)),
        new FunctionDef("adddate", new int[] { Types.DATE, Types.NUMERIC },
                new ExpressionType(Types.DATE)),
        new FunctionDef("adddate", new int[] { Types.TIMESTAMP, Types.NUMERIC }, 
                new ExpressionType(Types.TIMESTAMP)),
        new FunctionDef("addmonths", new int[] { Types.DATE, Types.NUMERIC },
                new ExpressionType(Types.DATE)),
        new FunctionDef("addmonths", new int[] { Types.TIMESTAMP, Types.NUMERIC }, 
                new ExpressionType(Types.TIMESTAMP)),
        new FunctionDef("addtime", new int[] { Types.TIME, Types.TIME}, 
                new ExpressionType(Types.TIME)),
        new FunctionDef("addtime", new int[] { Types.TIMESTAMP, Types.TIME}, 
                new ExpressionType(Types.TIMESTAMP)),
        new FunctionDef("age", new int[] { Types.TIMESTAMP, Types.TIMESTAMP }, 
                new ExpressionType(ExpressionType.INTERVAL_TYPE)),
        new FunctionDef("age", new int[] { Types.TIMESTAMP }, 
                new ExpressionType(ExpressionType.INTERVAL_TYPE)),
        new FunctionDef("ascii", new int[] { Types.CHAR }, new ExpressionType(
                Types.INTEGER)),
        new FunctionDef("asin", new int[] { Types.DOUBLE }, new ExpressionType(
                Types.DOUBLE)),
        new FunctionDef("atan", new int[] { Types.DOUBLE }, new ExpressionType(
                Types.DOUBLE)),
        new FunctionDef("atan2", new int[] { Types.DOUBLE, Types.DOUBLE }, 
                new ExpressionType(Types.DOUBLE)),
        new FunctionDef("bit_length", new int[] { Types.CLOB }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("broadcast", new int[] { ExpressionType.INET_TYPE }, 
                new ExpressionType(ExpressionType.INET_TYPE)),
        new FunctionDef("btrim", new int[] { Types.CLOB }, new ExpressionType(
                Types.CLOB)),
        new FunctionDef("btrim", new int[] { Types.CLOB, Types.NULL }, 
                new ExpressionType(Types.CLOB)),
        new FunctionDef("cbrt", new int[] { Types.DOUBLE }, new ExpressionType(
                Types.DOUBLE)),
        new FunctionDef("ceil", new int[] { Types.BIGINT }, new ExpressionType(
                Types.BIGINT)),
        new FunctionDef("ceil", new int[] { Types.DOUBLE }, new ExpressionType(
                Types.DOUBLE)),
        new FunctionDef("ceiling", new int[] { Types.BIGINT }, 
                new ExpressionType(Types.BIGINT)),
        new FunctionDef("ceiling", new int[] { Types.DOUBLE }, 
                new ExpressionType(Types.DOUBLE)),
        new FunctionDef("char_length", new int[] { Types.CLOB }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("character_length", new int[] { Types.CLOB }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("chr", new int[] { Types.INTEGER }, new ExpressionType(
                Types.CHAR, 1, 0, 0)),
        new FunctionDef("clock_timestamp", null, new ExpressionType(
                Types.TIMESTAMP)),
        new FunctionDef("concat", new int[] { Types.NULL, Types.NULL },
                new ExpressionType(Types.CLOB)),
        new FunctionDef("cos", new int[] { Types.DOUBLE }, new ExpressionType(
                Types.DOUBLE)),
        new FunctionDef("cosh", new int[] { Types.DOUBLE }, new ExpressionType(
                Types.DOUBLE)),
        new FunctionDef("cot", new int[] { Types.DOUBLE }, new ExpressionType(
                Types.DOUBLE)),
        new FunctionDef("database", null, new ExpressionType(
                Types.VARCHAR, 64, 0, 0)),
        new FunctionDef("date", null, new ExpressionType(Types.DATE)),
        new FunctionDef("datediff", new int[] { Types.DATE, Types.DATE }, 
                new ExpressionType(Types.FLOAT, 32, 0, 0)),
        new FunctionDef("date_part", new int[] { Types.CLOB, Types.TIMESTAMP }, 
                new ExpressionType(Types.DOUBLE)),
        new FunctionDef("date_part", new int[] { Types.CLOB,
                ExpressionType.INTERVAL_TYPE }, new ExpressionType(
                Types.DOUBLE)),
        new FunctionDef("date_trunc", new int[] { Types.CLOB, Types.TIMESTAMP }, 
                new ExpressionType(Types.TIMESTAMP)),
        new FunctionDef("day", new int[] { Types.TIMESTAMP }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("dayname", new int[] { Types.TIMESTAMP },
                new ExpressionType(Types.CHAR,
                        ExpressionType.MONTHORDAYNAME, 0, 0)),
        new FunctionDef("dayofmonth", new int[] { Types.TIMESTAMP }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("dayofweek", new int[] { Types.TIMESTAMP }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("dayofyear", new int[] { Types.TIMESTAMP }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("decode", new int[] { Types.CLOB, Types.CLOB }, 
                new ExpressionType(Types.BLOB)),
        new FunctionDef("degrees", new int[] { Types.DOUBLE }, 
                new ExpressionType(Types.DOUBLE)),
        new FunctionDef("encode", new int[] { Types.BLOB, Types.CLOB }, 
                new ExpressionType(Types.CLOB)),
        new FunctionDef("exp", new int[] { Types.DOUBLE }, new ExpressionType(
                Types.DOUBLE)),
        new FunctionDef("family", new int[] { ExpressionType.INET_TYPE}, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("float", new int[] { Types.DOUBLE, Types.DOUBLE }, 
                new ExpressionType(Types.FLOAT)),
        new FunctionDef("floor", new int[] { Types.BIGINT }, new ExpressionType(
                Types.BIGINT)),
        new FunctionDef("floor", new int[] { Types.DOUBLE }, new ExpressionType(
                Types.DOUBLE)),
        new FunctionDef("get_bit", new int[] { Types.CLOB, Types.BIGINT }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("get_byte", new int[] { Types.CLOB, Types.BIGINT }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("greatest", new int[] { Types.BIGINT, Types.BIGINT }, 
                new ExpressionType(Types.BIGINT)),
        new FunctionDef("greatest", new int[] { Types.DOUBLE, Types.DOUBLE }, 
                new ExpressionType(Types.DOUBLE)),
        new FunctionDef("greatest", new int[] { Types.CLOB, Types.CLOB }, 
                new ExpressionType(Types.CLOB)),
        new FunctionDef("host", new int[] { ExpressionType.INET_TYPE }, 
                new ExpressionType(Types.CLOB)),
        new FunctionDef("hostmask", new int[] { ExpressionType.INET_TYPE }, 
                new ExpressionType(ExpressionType.INET_TYPE)),
        new FunctionDef("hour", new int[] { Types.TIMESTAMP }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("index", new int[] { Types.CLOB, Types.CLOB }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("index", new int[] { Types.CLOB, Types.CLOB, 
                Types.INTEGER }, new ExpressionType(Types.INTEGER)),
        new FunctionDef("index", new int[] { Types.CLOB, Types.CLOB, 
                Types.INTEGER, Types.INTEGER }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("initcap", new int[] { Types.CLOB }, new ExpressionType(
                Types.CLOB)),
        new FunctionDef("instr", new int[] { Types.CLOB, Types.CLOB }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("instr", new int[] { Types.CLOB, Types.CLOB, 
                Types.INTEGER }, new ExpressionType(Types.INTEGER)),
        new FunctionDef("instr", new int[] { Types.CLOB, Types.CLOB, 
                Types.INTEGER, Types.INTEGER }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("isfinite", new int[] { Types.DATE }, 
                new ExpressionType(Types.BOOLEAN)),
        new FunctionDef("isfinite", new int[] { Types.TIMESTAMP }, 
                new ExpressionType(Types.BOOLEAN)),
        new FunctionDef("isfinite", new int[] { ExpressionType.INTERVAL_TYPE }, 
                new ExpressionType(Types.BOOLEAN)),
        new FunctionDef("justify_days", new int[] { ExpressionType.INTERVAL_TYPE }, 
                new ExpressionType(ExpressionType.INTERVAL_TYPE)),
        new FunctionDef("justify_hours", new int[] { ExpressionType.INTERVAL_TYPE }, 
                new ExpressionType(ExpressionType.INTERVAL_TYPE)),
        new FunctionDef("justify_interval", new int[] {ExpressionType.INTERVAL_TYPE},
                new ExpressionType(ExpressionType.INTERVAL_TYPE)),
        new FunctionDef("last_day", new int[] { Types.DATE }, 
                new ExpressionType(Types.DATE)),
        new FunctionDef("least", new int[] { Types.BIGINT, Types.BIGINT }, 
                new ExpressionType(Types.BIGINT)),
        new FunctionDef("least", new int[] { Types.DOUBLE, Types.DOUBLE }, 
                new ExpressionType(Types.DOUBLE)),
        new FunctionDef("least", new int[] { Types.CLOB, Types.CLOB }, 
                new ExpressionType(Types.CLOB)),
        new FunctionDef("left", new int[] { Types.CLOB, Types.INTEGER }, 
                new ExpressionType(Types.CLOB)),
        new FunctionDef("length", new int[] { Types.CLOB }, new ExpressionType(
                Types.INTEGER)),
        new FunctionDef("length", new int[] { Types.BLOB, Types.CHAR }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("ln", new int[] { Types.DOUBLE }, new ExpressionType(
                Types.DOUBLE)),
        new FunctionDef("localtime", null, new ExpressionType(Types.TIME)),
        new FunctionDef("localtimestamp", null, new ExpressionType(
                Types.TIMESTAMP)),
        new FunctionDef("log", new int[] { Types.DOUBLE }, 
                new ExpressionType(Types.DOUBLE)),
        new FunctionDef("log", new int[] { Types.DOUBLE, Types.DOUBLE }, 
                new ExpressionType(Types.DOUBLE)),
        new FunctionDef("lower", new int[] { Types.CLOB }, new ExpressionType(
                Types.CLOB)),
        new FunctionDef("lpad", new int[] { Types.CLOB, Types.INTEGER },
                new ExpressionType(Types.CLOB)),
        new FunctionDef("lpad", new int[] { Types.CLOB, Types.INTEGER, Types.CLOB },
                new ExpressionType(Types.CLOB)),
        new FunctionDef("ltrim", new int[] { Types.CLOB }, new ExpressionType(
                Types.CLOB)),
        new FunctionDef("ltrim", new int[] { Types.CLOB, Types.CLOB }, 
                new ExpressionType(Types.CLOB)),
        new FunctionDef("masklen", new int[] { ExpressionType.INET_TYPE }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("md5", new int[] { Types.CLOB }, 
                new ExpressionType(Types.CHAR, 32, 0, 0)),
        new FunctionDef("minute", new int[] { Types.TIMESTAMP }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("mod", new int[] { Types.BIGINT, Types.BIGINT }, 
                new ExpressionType(Types.BIGINT)),
        new FunctionDef("month", new int[] { Types.TIMESTAMP}, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("monthname", new int[] { Types.TIMESTAMP },
                new ExpressionType(Types.CHAR,
                        ExpressionType.MONTHORDAYNAME, 0, 0)),
        new FunctionDef("months_between", new int[] { Types.TIMESTAMP, Types.TIMESTAMP }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("netmask", new int[] { ExpressionType.INET_TYPE }, 
                new ExpressionType(ExpressionType.INET_TYPE)),
        new FunctionDef("network", new int[] { ExpressionType.INET_TYPE }, 
                new ExpressionType(ExpressionType.CIDR_TYPE)),
        new FunctionDef("next_day", new int[] { Types.TIMESTAMP, Types.CLOB }, 
                new ExpressionType(Types.TIMESTAMP)),
        new FunctionDef("now", null, new ExpressionType(Types.TIMESTAMP)),
        new FunctionDef("octet_length", new int[] { Types.CLOB }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("overlay", new int[] { Types.CLOB, Types.CLOB, 
                Types.INTEGER }, new ExpressionType(Types.CLOB)),
        new FunctionDef("overlay", new int[] { Types.CLOB, Types.CLOB, 
                Types.INTEGER, Types.INTEGER }, new ExpressionType(Types.CLOB)),
        new FunctionDef("pi", null, new ExpressionType(Types.DOUBLE)),
        new FunctionDef("position", new int[] { Types.CLOB, Types.CLOB }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("power", new int[] { Types.BIGINT, Types.BIGINT }, 
                new ExpressionType(Types.BIGINT)),
        new FunctionDef("power", new int[] { Types.DOUBLE, Types.DOUBLE }, 
                new ExpressionType(Types.DOUBLE)),
        new FunctionDef("quote_ident", new int[] { Types.CLOB }, 
                new ExpressionType(Types.CLOB)),
        new FunctionDef("quote_literal", new int[] { Types.CLOB }, 
                new ExpressionType(Types.CLOB)),
        new FunctionDef("radians", new int[] { Types.DOUBLE }, 
                new ExpressionType(Types.DOUBLE)),
        new FunctionDef("random", null, new ExpressionType(Types.DOUBLE)),
        new FunctionDef("regexp_matches", new int[] { Types.CLOB, Types.CLOB }, 
                new ExpressionType(Types.CLOB)),
        new FunctionDef("regexp_matches", new int[] { Types.CLOB, Types.CLOB,
                Types.CLOB }, new ExpressionType(Types.CLOB)),
        new FunctionDef("regexp_replace", new int[] { Types.CLOB, Types.CLOB,
                Types.CLOB }, new ExpressionType(Types.CLOB)),
        new FunctionDef("regexp_replace", new int[] { Types.CLOB, Types.CLOB,
                Types.CLOB, Types.CLOB }, new ExpressionType(Types.CLOB)),
        new FunctionDef("repeat", new int[] { Types.CLOB, Types.INTEGER }, 
                new ExpressionType(Types.CLOB)),
        new FunctionDef("replace", new int[] { Types.CLOB, Types.CLOB, 
                Types.CLOB}, new ExpressionType(Types.CLOB)),
        new FunctionDef("right", new int[] { Types.CLOB, Types.INTEGER }, 
                new ExpressionType(Types.CLOB)),
        new FunctionDef("round", new int[] { Types.BIGINT }, 
                new ExpressionType(Types.BIGINT)),
        new FunctionDef("round", new int[] { Types.DOUBLE }, new ExpressionType(
                Types.DOUBLE)),
        new FunctionDef("round", new int[] { Types.BIGINT, Types.INTEGER }, 
                new ExpressionType(Types.BIGINT)),
        new FunctionDef("round", new int[] { Types.DOUBLE, Types.INTEGER }, 
                new ExpressionType(Types.DOUBLE)),
        new FunctionDef("rpad", new int[] { Types.CLOB, Types.INTEGER },
                new ExpressionType(Types.CLOB)),
        new FunctionDef("rpad", new int[] { Types.CLOB, Types.INTEGER, Types.CLOB },
                new ExpressionType(Types.CLOB)),
        new FunctionDef("rtrim", new int[] { Types.CLOB }, new ExpressionType(
                Types.CLOB)),
        new FunctionDef("ltrim", new int[] { Types.CLOB, Types.CLOB }, 
                new ExpressionType(Types.CLOB)),
        new FunctionDef("second", new int[] { Types.TIMESTAMP }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("set_bit", new int[] { Types.BLOB, Types.INTEGER, 
                Types.INTEGER }, new ExpressionType(Types.BLOB)),
        new FunctionDef("set_byte", new int[] { Types.BLOB, Types.INTEGER, 
                Types.INTEGER }, new ExpressionType(Types.BLOB)),
        new FunctionDef("set_masklen", new int[] { ExpressionType.CIDR_TYPE, 
                Types.INTEGER }, new ExpressionType(ExpressionType.CIDR_TYPE)),
        new FunctionDef("set_masklen", new int[] { ExpressionType.INET_TYPE, 
                Types.INTEGER }, new ExpressionType(ExpressionType.INET_TYPE)),
        new FunctionDef("sign", new int[] { Types.BIGINT }, new ExpressionType(
                Types.BIGINT)),
        new FunctionDef("sign", new int[] { Types.DOUBLE }, new ExpressionType(
                Types.DOUBLE)),
        new FunctionDef("sin", new int[] { Types.DOUBLE }, new ExpressionType(
                Types.DOUBLE)),
        new FunctionDef("soundex", new int[] { Types.CLOB }, new ExpressionType(
                Types.CHAR, 4, 0, 0)),
        new FunctionDef("split_part", new int[] { Types.CLOB, Types.CLOB, 
                Types.INTEGER }, new ExpressionType(Types.CLOB)),
        new FunctionDef("sqrt", new int[] { Types.DOUBLE }, new ExpressionType(
                Types.DOUBLE)),
        new FunctionDef("st_addmeasure", new int[] { 
                ExpressionType.GEOMETRY_TYPE, Types.DOUBLE, Types.DOUBLE }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_addpoint", new int[] { ExpressionType.GEOMETRY_TYPE,
                ExpressionType.GEOMETRY_TYPE}, new ExpressionType(
                ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_addpoint", new int[] { ExpressionType.GEOMETRY_TYPE,
                ExpressionType.GEOMETRY_TYPE, Types.INTEGER}, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_affine", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, 
                Types.DOUBLE, Types.DOUBLE }, new ExpressionType(
                ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_affine", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, 
                Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, 
                Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_area", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.DOUBLE)),
        new FunctionDef("st_area2d", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.DOUBLE)),
        new FunctionDef("st_asbinary", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.BLOB)),
        new FunctionDef("st_asbinary", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.CHAR}, new ExpressionType(Types.BLOB)),
        new FunctionDef("st_asewkb", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.BLOB)),
        new FunctionDef("st_asewkb", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.CHAR}, new ExpressionType(Types.BLOB)),
        new FunctionDef("st_asewkt", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.CLOB)),
        new FunctionDef("st_asgeojson", new int[] { ExpressionType.GEOMETRY_TYPE, 
                Types.INTEGER, Types.INTEGER}, new ExpressionType(Types.CLOB)),
        new FunctionDef("st_asgeojson", new int[] { Types.INTEGER, 
                ExpressionType.GEOMETRY_TYPE, Types.INTEGER, Types.INTEGER}, 
                new ExpressionType(Types.CLOB)),
        new FunctionDef("st_asgml", new int[] { ExpressionType.GEOMETRY_TYPE, 
                Types.INTEGER, Types.INTEGER}, new ExpressionType(Types.CLOB)),
        new FunctionDef("st_asgml", new int[] { Types.INTEGER, 
                ExpressionType.GEOMETRY_TYPE, Types.INTEGER, Types.INTEGER}, 
                new ExpressionType(Types.CLOB)),
        new FunctionDef("st_ashexewkb", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.CHAR}, new ExpressionType(Types.CLOB)),
        new FunctionDef("st_askml", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.INTEGER}, new ExpressionType(Types.CLOB)),
        new FunctionDef("st_askml", new int[] { Types.INTEGER, 
                ExpressionType.GEOMETRY_TYPE, Types.INTEGER, Types.CHAR}, 
                new ExpressionType(Types.CLOB)),
        new FunctionDef("st_assvg", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.INTEGER, Types.INTEGER}, new ExpressionType(Types.CLOB)),
        new FunctionDef("st_astext", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.CLOB)),
        new FunctionDef("statement_timestamp", null, new ExpressionType(
                Types.TIMESTAMP)),
        new FunctionDef("st_azimuth", new int[] { ExpressionType.GEOMETRY_TYPE,
                ExpressionType.GEOMETRY_TYPE}, new ExpressionType(
                Types.DOUBLE)),
        new FunctionDef("st_bdmpolyfromtext", new int[] { Types.CLOB, Types.INTEGER }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_bdpolyfromtext", new int[] { Types.CLOB, Types.INTEGER }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_boundary", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_box2d", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(ExpressionType.BOX2D_TYPE)),
        new FunctionDef("st_box3d", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(ExpressionType.BOX3D_TYPE)),
        new FunctionDef("st_buffer", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.DOUBLE }, new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_buffer", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.DOUBLE, Types.INTEGER }, new ExpressionType(
                ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_buffer", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.DOUBLE, Types.CHAR }, new ExpressionType(
                ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_buildarea", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_bytea", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.BLOB)),
        new FunctionDef("st_centroid", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_closestpoint", new int[] { ExpressionType.GEOMETRY_TYPE,
                ExpressionType.GEOMETRY_TYPE }, new ExpressionType(
                ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_collect", new int[] { ExpressionType.GEOMETRY_TYPE,
                ExpressionType.GEOMETRY_TYPE }, new ExpressionType(
                ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_collectionextract", new int[] { ExpressionType.GEOMETRY_TYPE, 
                Types.INTEGER }, new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_combine_bbox", new int[] { ExpressionType.BOX2D_TYPE,
                ExpressionType.GEOMETRY_TYPE }, new ExpressionType(
                ExpressionType.BOX2D_TYPE)),
        new FunctionDef("st_contains", new int[] { ExpressionType.GEOMETRY_TYPE,
                ExpressionType.GEOMETRY_TYPE}, new ExpressionType(
                Types.BOOLEAN)),
        new FunctionDef("st_containsproperly", new int[] { ExpressionType.GEOMETRY_TYPE,
                ExpressionType.GEOMETRY_TYPE}, new ExpressionType(
                Types.BOOLEAN)),
        new FunctionDef("st_convexhull", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_coorddim", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("st_coveredby", new int[] { ExpressionType.GEOMETRY_TYPE,
                ExpressionType.GEOMETRY_TYPE }, new ExpressionType(
                Types.BOOLEAN)),
        new FunctionDef("st_covers", new int[] { ExpressionType.GEOMETRY_TYPE,
                ExpressionType.GEOMETRY_TYPE }, new ExpressionType(
                Types.BOOLEAN)),
        new FunctionDef("st_crosses", new int[] { ExpressionType.GEOMETRY_TYPE,
                ExpressionType.GEOMETRY_TYPE }, new ExpressionType(
                Types.BOOLEAN)),
        new FunctionDef("st_curvetoline", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_curvetoline", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.INTEGER }, new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_dfullywithin", new int[] { ExpressionType.GEOMETRY_TYPE,
                ExpressionType.GEOMETRY_TYPE, Types.DOUBLE }, 
                new ExpressionType(Types.BOOLEAN)),
        new FunctionDef("st_difference", new int[] { ExpressionType.GEOMETRY_TYPE,
                ExpressionType.GEOMETRY_TYPE}, new ExpressionType(
                ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_dimension", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("st_disjoint", new int[] { ExpressionType.GEOMETRY_TYPE,
                ExpressionType.GEOMETRY_TYPE }, new ExpressionType(
                ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_distance", new int[] { ExpressionType.GEOMETRY_TYPE,
                ExpressionType.GEOMETRY_TYPE }, new ExpressionType(
                Types.DOUBLE)),
        new FunctionDef("st_distance_sphere", new int[] { ExpressionType.GEOMETRY_TYPE,
                ExpressionType.GEOMETRY_TYPE }, new ExpressionType(
                Types.DOUBLE)),
        new FunctionDef("st_distance_spheroid", new int[] { ExpressionType.GEOMETRY_TYPE,
                ExpressionType.GEOMETRY_TYPE }, new ExpressionType(
                Types.DOUBLE)),
        new FunctionDef("st_dwithin", new int[] { ExpressionType.GEOMETRY_TYPE,
                ExpressionType.GEOMETRY_TYPE, Types.DOUBLE }, 
                new ExpressionType(Types.BOOLEAN)),
        new FunctionDef("st_endpoint", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.BOOLEAN)),
        new FunctionDef("st_envelope", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_equals", new int[] { ExpressionType.GEOMETRY_TYPE,
                ExpressionType.GEOMETRY_TYPE }, new ExpressionType(
                Types.BOOLEAN)),
        new FunctionDef("st_extent", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(ExpressionType.BOX2D_TYPE)),
        new FunctionDef("st_extent3d", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(ExpressionType.BOX3D_TYPE)),
        new FunctionDef("st_exteriorring", new int[] { ExpressionType.GEOMETRY_TYPE },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_force_2d", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_force_3d", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_force_3dm", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_force_3dz", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_force_4d", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_force_collection", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_forcerhr", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.BOOLEAN)),
        new FunctionDef("st_geogfromtext", new int[] { Types.CLOB },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_geogfromwkb", new int[] { Types.BLOB }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_geographyfromtext", new int[] { Types.CLOB },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_geohash", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.INTEGER}, new ExpressionType(Types.CLOB)),
        new FunctionDef("st_geomcollfromtext", new int[] { Types.CLOB },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_geomcollfromtext", new int[] { Types.CLOB, 
                Types.INTEGER }, new ExpressionType(
                ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_geomcollfromwkb", new int[] { Types.BLOB },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_geometryn", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.INTEGER }, new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_geometrytype", new int[] { ExpressionType.GEOMETRY_TYPE },
                new ExpressionType(Types.CLOB)),
        new FunctionDef("st_geomfromewkb", new int[] { Types.BLOB },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_geomfromewkt", new int[] { Types.CLOB },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_geomfromgml", new int[] { Types.CLOB }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_geomfromgml", new int[] { Types.CLOB, Types.INTEGER }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_geomfromkml", new int[] { Types.CLOB }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_geomfromtext", new int[] { Types.CLOB }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_geomfromtext", new int[] { Types.CLOB, Types.INTEGER }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_geomfromwkb", new int[] { Types.BLOB }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_geomfromwkb", new int[] { Types.BLOB, Types.INTEGER }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_gmltosql", new int[] { Types.CLOB }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_gmltosql", new int[] { Types.CLOB, Types.INTEGER }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_hasarc", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.BOOLEAN)),
        new FunctionDef("st_hausdorffdistance", new int[] { 
                ExpressionType.GEOMETRY_TYPE, ExpressionType.GEOMETRY_TYPE },
                new ExpressionType(Types.DOUBLE)),
        new FunctionDef("st_hausdorffdistance", new int[] { 
                ExpressionType.GEOMETRY_TYPE, ExpressionType.GEOMETRY_TYPE, 
                Types.DOUBLE }, new ExpressionType(Types.DOUBLE)),
        new FunctionDef("st_height", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("st_interiorringn", new int[] { 
                ExpressionType.GEOMETRY_TYPE, Types.INTEGER },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_intersection", new int[] { 
                ExpressionType.GEOMETRY_TYPE, ExpressionType.GEOMETRY_TYPE },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_intersects", new int[] { 
                ExpressionType.GEOMETRY_TYPE, ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.BOOLEAN)),
        new FunctionDef("st_isclosed", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.BOOLEAN)),
        new FunctionDef("st_isempty", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.BOOLEAN)),
        new FunctionDef("st_isring", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.BOOLEAN)),
        new FunctionDef("st_issimple", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.BOOLEAN)),
        new FunctionDef("st_isvalid", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.BOOLEAN)),
        new FunctionDef("st_isvalid", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.INTEGER }, new ExpressionType(Types.BOOLEAN)),
        new FunctionDef("st_isvalidreason", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.BOOLEAN)),
        new FunctionDef("st_isvalidreason", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.INTEGER }, new ExpressionType(Types.BOOLEAN)),
        new FunctionDef("st_length", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.DOUBLE)),
        new FunctionDef("st_length2d", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.DOUBLE)),
        new FunctionDef("st_length2d_spheroid", new int[] 
                { ExpressionType.GEOMETRY_TYPE, ExpressionType.GEOMETRY_TYPE },
                new ExpressionType(Types.DOUBLE)),
        new FunctionDef("st_length3d", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.DOUBLE)),
        new FunctionDef("st_length3d_spheroid", new int[] 
                { ExpressionType.GEOMETRY_TYPE, ExpressionType.GEOMETRY_TYPE },
                new ExpressionType(Types.DOUBLE)),
        new FunctionDef("st_length_spheroid", new int[] 
                { ExpressionType.GEOMETRY_TYPE, ExpressionType.GEOMETRY_TYPE },
                new ExpressionType(Types.DOUBLE)),
        new FunctionDef("st_linecrossingdirection", new int[] 
                { ExpressionType.GEOMETRY_TYPE, ExpressionType.GEOMETRY_TYPE },
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("st_linefrommultipoint", new int[] 
                { ExpressionType.GEOMETRY_TYPE }, new ExpressionType(
                ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_linefromtext", new int[] { Types.CLOB },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_linefromtext", new int[] { Types.CLOB, Types.INTEGER },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_linefromwkb", new int[] { Types.BLOB, Types.INTEGER },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_linefromwkb", new int[] { Types.BLOB },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_line_interpolate_point", new int[] 
                { ExpressionType.GEOMETRY_TYPE, Types.DOUBLE },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_line_locate_point", new int[] 
                { ExpressionType.GEOMETRY_TYPE, ExpressionType.GEOMETRY_TYPE },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_linemerge", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_linestringfromwkb", new int[] { Types.BLOB },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_linestringfromwkb", new int[] { Types.BLOB, 
                Types.INTEGER }, new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_line_substring", new int[] { 
                ExpressionType.GEOMETRY_TYPE, Types.DOUBLE, Types.DOUBLE },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_linetocurve", new int[] {ExpressionType.GEOMETRY_TYPE}, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_locate_along_measure", new int[] 
                { ExpressionType.GEOMETRY_TYPE, Types.DOUBLE },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_locatebetweenelevations", new int[] 
                { ExpressionType.GEOMETRY_TYPE, Types.DOUBLE, Types.DOUBLE },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_locate_between_measures", new int[] 
                { ExpressionType.GEOMETRY_TYPE, Types.DOUBLE, Types.DOUBLE },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_longestline", new int[] { ExpressionType.GEOMETRY_TYPE,
                ExpressionType.GEOMETRY_TYPE }, new ExpressionType(
                ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_m", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.DOUBLE)),
        new FunctionDef("st_makeenvelope", new int[] { Types.DOUBLE, 
                Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.INTEGER },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_makepoint", new int[] { Types.DOUBLE, Types.DOUBLE }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_makepoint", new int[] { Types.DOUBLE, Types.DOUBLE, 
                Types.DOUBLE }, new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_makepoint", new int[] { Types.DOUBLE, Types.DOUBLE, 
                Types.DOUBLE, Types.DOUBLE }, new ExpressionType(
                ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_makepointm", new int[] { Types.DOUBLE, Types.DOUBLE, 
                Types.DOUBLE }, new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_makepolygon", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_makepolygon", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.ARRAY}, new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_maxdistance", new int[] { ExpressionType.GEOMETRY_TYPE,
                ExpressionType.GEOMETRY_TYPE }, new ExpressionType(Types.DOUBLE)),
        new FunctionDef("st_mem_size", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("st_minimumboundingcircle", new int[] 
                { ExpressionType.GEOMETRY_TYPE }, new ExpressionType(
                ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_minimumboundingcircle", new int[] 
                { ExpressionType.GEOMETRY_TYPE, Types.INTEGER }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_mlinefromtext", new int[] { Types.CLOB },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_mlinefromtext", new int[] { Types.CLOB, Types.INTEGER },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_mlinefromwkb", new int[] { Types.BLOB },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_mlinefromwkb", new int[] { Types.BLOB, Types.INTEGER },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_mpointfromtext", new int[] { Types.CLOB },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_mpointfromtext", new int[] { Types.CLOB, Types.INTEGER },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_mpointfromwkb", new int[] { Types.BLOB },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_mpointfromwkb", new int[] { Types.BLOB, Types.INTEGER },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_mpolyfromtext", new int[] { Types.CLOB },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_mpolyfromtext", new int[] { Types.CLOB, Types.INTEGER },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_mpolyfromwkb", new int[] { Types.BLOB },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_mpolyfromwkb", new int[] { Types.BLOB, Types.INTEGER },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_multi", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_multilinefromwkb", new int[] { Types.BLOB },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_multilinestringfromtext", new int[] { Types.CLOB },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_multipointfromtext", new int[] { Types.CLOB },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_multipointfromwkb", new int[] { Types.BLOB },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_multipolyfromwkb", new int[] { Types.BLOB },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_multipolygonfromtext", new int[] { Types.CLOB },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_ndims", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("st_npoints", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("st_nrings", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("st_numgeometries", new int[] { ExpressionType.GEOMETRY_TYPE },
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("st_numinteriorring", new int[] { ExpressionType.GEOMETRY_TYPE },
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("st_numinteriorrings", new int[] { ExpressionType.GEOMETRY_TYPE },
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("st_numpoints", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("st_orderingequals", new int[] 
                { ExpressionType.GEOMETRY_TYPE, ExpressionType.GEOMETRY_TYPE },
                new ExpressionType(Types.BOOLEAN)),
        new FunctionDef("st_overlaps", new int[] 
                { ExpressionType.GEOMETRY_TYPE, ExpressionType.GEOMETRY_TYPE },
                new ExpressionType(Types.BOOLEAN)),
        new FunctionDef("st_perimeter", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.DOUBLE)),
        new FunctionDef("st_perimeter2d", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.DOUBLE)),
        new FunctionDef("st_perimeter3d", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.DOUBLE)),
        new FunctionDef("st_point", new int[] { Types.DOUBLE, Types.DOUBLE }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_pointfromtext", new int[] { Types.CLOB },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_pointfromtext", new int[] { Types.CLOB, Types.INTEGER },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_pointfromwkb", new int[] { Types.BLOB },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_pointfromwkb", new int[] { Types.BLOB, Types.INTEGER },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_point_inside_circle", new int[] 
                { ExpressionType.GEOMETRY_TYPE, Types.DOUBLE, Types.DOUBLE,
                Types.DOUBLE }, new ExpressionType(Types.BOOLEAN)),
        new FunctionDef("st_pointn", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.INTEGER }, new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_pointonsurface", new int[] { ExpressionType.GEOMETRY_TYPE },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_polyfromtext", new int[] { Types.CLOB },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_polyfromtext", new int[] { Types.CLOB, Types.INTEGER },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_polyfromwkb", new int[] { Types.BLOB },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_polyfromwkb", new int[] { Types.BLOB, Types.INTEGER },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_polygon", new int[] { ExpressionType.GEOMETRY_TYPE, 
                Types.INTEGER}, new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_polygonfromtext", new int[] { Types.CLOB },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_polygonfromtext", new int[] { Types.CLOB, Types.INTEGER },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_polygonfromwkb", new int[] { Types.CLOB },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_polygonfromwkb", new int[] { Types.CLOB, Types.INTEGER },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_relate", new int[] { ExpressionType.GEOMETRY_TYPE,
                ExpressionType.GEOMETRY_TYPE, Types.CLOB }, 
                new ExpressionType(Types.BOOLEAN)),
        new FunctionDef("st_relate", new int[] { ExpressionType.GEOMETRY_TYPE,
                ExpressionType.GEOMETRY_TYPE }, new ExpressionType(Types.CLOB)),
        new FunctionDef("st_removepoint", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.INTEGER }, new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_reverse", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_rotatex", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.DOUBLE }, new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_rotatey", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.DOUBLE }, new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_rotatez", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.DOUBLE }, new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("strpos", new int[] { Types.CLOB, Types.CLOB }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("st_scale", new int[] { ExpressionType.GEOMETRY_TYPE, 
                Types.DOUBLE, Types.DOUBLE }, new ExpressionType(
                ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_scale", new int[] { ExpressionType.GEOMETRY_TYPE, 
                Types.DOUBLE, Types.DOUBLE, Types.DOUBLE }, new ExpressionType(
                ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_segmentize", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.DOUBLE}, new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_setpoint", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.INTEGER, ExpressionType.GEOMETRY_TYPE}, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_setsrid", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.INTEGER }, new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_shift_longitude", new int[] { ExpressionType.GEOMETRY_TYPE },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_shortestline", new int[] { ExpressionType.GEOMETRY_TYPE,
                ExpressionType.GEOMETRY_TYPE }, new ExpressionType(
                ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_simplify", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.DOUBLE }, new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_simplifypreservetopology", new int[] 
                { ExpressionType.GEOMETRY_TYPE, Types.DOUBLE },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_snaptogrid", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.DOUBLE }, new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_snaptogrid", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.DOUBLE, Types.DOUBLE }, new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_snaptogrid", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_snaptogrid", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_srid", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("st_startpoint", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_summary", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.CLOB)),
        new FunctionDef("st_symdifference", new int[] 
                { ExpressionType.GEOMETRY_TYPE, ExpressionType.GEOMETRY_TYPE },
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_touches", new int[] { ExpressionType.GEOMETRY_TYPE,
                ExpressionType.GEOMETRY_TYPE}, new ExpressionType(Types.BOOLEAN)),
        new FunctionDef("st_transform", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.INTEGER }, new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_translate", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.DOUBLE, Types.DOUBLE }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_translate", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.DOUBLE, Types.DOUBLE, Types.DOUBLE }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_transscale", new int[] { ExpressionType.GEOMETRY_TYPE,
                Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_width", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("st_within", new int[] { ExpressionType.GEOMETRY_TYPE,
                ExpressionType.GEOMETRY_TYPE }, new ExpressionType(
                Types.BOOLEAN)),
        new FunctionDef("st_wkbtosql", new int[] { Types.BLOB }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_wkttosql", new int[] { Types.CLOB }, 
                new ExpressionType(ExpressionType.GEOMETRY_TYPE)),
        new FunctionDef("st_x", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.DOUBLE)),
        new FunctionDef("st_y", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.DOUBLE)),
        new FunctionDef("st_z", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.DOUBLE)),
        new FunctionDef("st_zmflag", new int[] { ExpressionType.GEOMETRY_TYPE }, 
                new ExpressionType(Types.SMALLINT)),
        new FunctionDef("subdate", new int[] { Types.DATE, Types.NUMERIC },
                new ExpressionType(Types.DATE)),
        new FunctionDef("substr", new int[] { Types.CLOB, Types.INTEGER }, 
                new ExpressionType(Types.CLOB)),
        new FunctionDef("substr", new int[] { Types.CLOB, Types.INTEGER,
                Types.INTEGER }, new ExpressionType(Types.CLOB)),
        new FunctionDef("substring", new int[] { Types.CLOB, Types.INTEGER }, 
                new ExpressionType(Types.CLOB)),
        new FunctionDef("substring", new int[] { Types.CLOB, Types.INTEGER,
                Types.INTEGER }, new ExpressionType(Types.CLOB)),
        new FunctionDef("subtime", new int[] { Types.TIME, Types.TIME}, 
                new ExpressionType(Types.TIME)),
        new FunctionDef("tan", new int[] { Types.DOUBLE }, new ExpressionType(
                Types.DOUBLE)),
        new FunctionDef("text", new int[] { ExpressionType.INET_TYPE }, 
                new ExpressionType(Types.CLOB)),
        new FunctionDef("time", new int[] { Types.TIMESTAMP }, 
                new ExpressionType(Types.TIME)),
        new FunctionDef("timeofday", null, new ExpressionType(Types.CLOB)),
        new FunctionDef("timestamp", new int[] { Types.TIMESTAMP }, 
                new ExpressionType(Types.TIMESTAMP)),
        new FunctionDef("to_ascii", new int[] { Types.CLOB }, 
                new ExpressionType(Types.CLOB)),
        new FunctionDef("to_ascii", new int[] { Types.CLOB, Types.CLOB }, 
                new ExpressionType(Types.CLOB)),
        new FunctionDef("to_char", new int[] { Types.DOUBLE, Types.CLOB }, 
                new ExpressionType(Types.CLOB)),
        new FunctionDef("to_char", new int[] { Types.TIMESTAMP, Types.CLOB }, 
                new ExpressionType(Types.CLOB)),
        new FunctionDef("to_char", new int[] { ExpressionType.INTERVAL_TYPE, 
                Types.CLOB }, new ExpressionType(Types.CLOB)),
        new FunctionDef("to_date", new int[] { Types.CLOB, Types.CLOB }, 
                new ExpressionType(Types.TIMESTAMP)),
        new FunctionDef("to_hex", new int[] { Types.BIGINT }, 
                new ExpressionType(Types.CHAR, 16, 0, 0)),
        new FunctionDef("to_number", new int[] { Types.CLOB, Types.CLOB }, 
                new ExpressionType(Types.DOUBLE)),
        new FunctionDef("to_timestamp", new int[] { Types.CLOB, Types.CLOB }, 
                new ExpressionType(Types.TIMESTAMP)),
        new FunctionDef("to_timestamp", new int[] { Types.DOUBLE }, 
                new ExpressionType(Types.TIMESTAMP)),
        new FunctionDef("transaction_timestamp", null, new ExpressionType(
                Types.TIMESTAMP)),
        new FunctionDef("translate", new int[] { Types.CLOB, Types.CLOB, 
                Types.CLOB }, new ExpressionType(Types.CLOB)),
        new FunctionDef("trim", new int[] { Types.CLOB }, new ExpressionType(
                Types.CLOB)),
        new FunctionDef("trunc", new int[] { ExpressionType.MACADDR_TYPE }, 
                new ExpressionType(ExpressionType.MACADDR_TYPE)),
        new FunctionDef("trunc", new int[] { Types.BIGINT }, 
                new ExpressionType(Types.BIGINT)),
        new FunctionDef("trunc", new int[] { Types.DOUBLE }, 
                new ExpressionType(Types.DOUBLE)),
        new FunctionDef("trunc", new int[] { Types.BIGINT, Types.INTEGER }, 
                new ExpressionType(Types.BIGINT)),
        new FunctionDef("upper", new int[] { Types.CLOB }, new ExpressionType(
                Types.CLOB)),
        new FunctionDef("user", null, new ExpressionType(Types.CLOB)),
        new FunctionDef("version", null, new ExpressionType(Types.CLOB)),
        new FunctionDef("weekofyear", new int[] { Types.TIMESTAMP }, 
                new ExpressionType(Types.INTEGER)),
        new FunctionDef("width_bucket", new int[] { Types.DOUBLE, Types.DOUBLE,
                Types.DOUBLE, Types.INTEGER}, new ExpressionType(
                Types.INTEGER)),
        new FunctionDef("year", new int[] { Types.TIMESTAMP}, 
                new ExpressionType(Types.INTEGER))
    };

    private static HashMap<String, Collection<FunctionDef>> funcCache = null;

    private static DATATYPE_MATCH matchingTypes(int candidate, int definition) {
        if (candidate == definition)
            return DATATYPE_MATCH.EXACT_MATCH;
        switch (candidate) {
            case Types.TINYINT:
                switch (definition) {
                    case Types.SMALLINT:
                    case Types.INTEGER:
                    case Types.BIGINT:
                    case Types.FLOAT:
                    case Types.REAL:
                    case Types.DOUBLE:
                    case Types.NUMERIC:
                    case Types.DECIMAL:
                        return DATATYPE_MATCH.CAST_NO_LOSS;
                }
            case Types.SMALLINT:
                switch (definition) {
                    case Types.TINYINT:
                        return DATATYPE_MATCH.CAST_WITH_LOSS;
                    case Types.INTEGER:
                    case Types.BIGINT:
                    case Types.FLOAT:
                    case Types.REAL:
                    case Types.DOUBLE:
                    case Types.NUMERIC:
                    case Types.DECIMAL:
                        return DATATYPE_MATCH.CAST_NO_LOSS;
                }
            case Types.INTEGER:
                switch (definition) {
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.FLOAT:
                    case Types.REAL:
                        return DATATYPE_MATCH.CAST_WITH_LOSS;
                    case Types.BIGINT:
                    case Types.DOUBLE:
                    case Types.NUMERIC:
                    case Types.DECIMAL:
                        return DATATYPE_MATCH.CAST_NO_LOSS;
                }
            case Types.BIGINT:
                switch (definition) {
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.INTEGER:
                    case Types.FLOAT:
                    case Types.REAL:
                        return DATATYPE_MATCH.CAST_WITH_LOSS;
                    case Types.DOUBLE:
                    case Types.NUMERIC:
                    case Types.DECIMAL:
                        return DATATYPE_MATCH.CAST_NO_LOSS;
                }
            case Types.FLOAT:
                switch (definition) {
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.INTEGER:
                    case Types.BIGINT:
                    case Types.REAL:
                        return DATATYPE_MATCH.CAST_WITH_LOSS;
                    case Types.DOUBLE:
                    case Types.NUMERIC:
                    case Types.DECIMAL:
                        return DATATYPE_MATCH.CAST_NO_LOSS;
                }
            case Types.REAL:
                switch (definition) {
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.INTEGER:
                    case Types.BIGINT:
                    case Types.FLOAT:
                        return DATATYPE_MATCH.CAST_WITH_LOSS;
                    case Types.DOUBLE:
                    case Types.NUMERIC:
                    case Types.DECIMAL:
                        return DATATYPE_MATCH.CAST_NO_LOSS;
                }
            case Types.DOUBLE:
                switch (definition) {
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.INTEGER:
                    case Types.BIGINT:
                    case Types.FLOAT:
                    case Types.REAL:
                    case Types.NUMERIC:
                    case Types.DECIMAL:
                        return DATATYPE_MATCH.CAST_WITH_LOSS;
                }
            case Types.NUMERIC:
                switch (definition) {
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.INTEGER:
                    case Types.BIGINT:
                    case Types.FLOAT:
                    case Types.REAL:
                        return DATATYPE_MATCH.CAST_WITH_LOSS;
                    case Types.DOUBLE:
                    case Types.DECIMAL:
                        return DATATYPE_MATCH.CAST_NO_LOSS;
                }
            case Types.DECIMAL:
                switch (definition) {
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.INTEGER:
                    case Types.BIGINT:
                    case Types.FLOAT:
                    case Types.REAL:
                        return DATATYPE_MATCH.CAST_WITH_LOSS;
                    case Types.DOUBLE:
                    case Types.NUMERIC:
                        return DATATYPE_MATCH.CAST_NO_LOSS;
                }
            case Types.CHAR:
                switch (definition) {
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.CLOB:
                        return DATATYPE_MATCH.CAST_NO_LOSS;
                }
            case Types.VARCHAR:
                switch (definition) {
                    case Types.CHAR:
                    case Types.LONGVARCHAR:
                    case Types.CLOB:
                        return DATATYPE_MATCH.CAST_NO_LOSS;
                }
            case Types.LONGVARCHAR:
                switch (definition) {
                    case Types.CHAR:
                    case Types.VARCHAR:
                        return DATATYPE_MATCH.CAST_WITH_LOSS;
                    case Types.CLOB:
                        return DATATYPE_MATCH.CAST_NO_LOSS;
                }
            case Types.CLOB:
                switch (definition) {
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                        return DATATYPE_MATCH.CAST_WITH_LOSS;
                }
            case Types.BINARY:
                switch (definition) {
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                    case Types.BLOB:
                        return DATATYPE_MATCH.CAST_NO_LOSS;
                }
            case Types.VARBINARY:
                switch (definition) {
                    case Types.BINARY:
                    case Types.LONGVARBINARY:
                    case Types.BLOB:
                        return DATATYPE_MATCH.CAST_NO_LOSS;
                }
            case Types.LONGVARBINARY:
                switch (definition) {
                    case Types.BINARY:
                    case Types.VARBINARY:
                        return DATATYPE_MATCH.CAST_WITH_LOSS;
                    case Types.BLOB:
                        return DATATYPE_MATCH.CAST_NO_LOSS;
                }
            case Types.BLOB:
                switch (definition) {
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                        return DATATYPE_MATCH.CAST_WITH_LOSS;
                }
            case Types.DATE:
            case Types.TIME:
                if (definition == Types.TIMESTAMP)
                    return DATATYPE_MATCH.CAST_NO_LOSS;
            case Types.TIMESTAMP:
                switch (definition) {
                    case Types.DATE:
                    case Types.TIME:
                        return DATATYPE_MATCH.CAST_WITH_LOSS;
                }
        }
        if (definition == Types.NULL)
            return DATATYPE_MATCH.CAST_NO_LOSS;
        else
            return DATATYPE_MATCH.NO_MATCH;
    }

    private static DATATYPE_MATCH matchingParams(int[] candidate,
            int[] definition) {
        if (candidate == null && definition == null)
            return DATATYPE_MATCH.EXACT_MATCH;
        if (candidate == null || definition == null)
            return DATATYPE_MATCH.NO_MATCH;
        if (candidate.length == definition.length) {
            DATATYPE_MATCH result = DATATYPE_MATCH.EXACT_MATCH;
            for (int i = 0; i < candidate.length; i++) {
                DATATYPE_MATCH next = matchingTypes(candidate[i], definition[i]);
                if (next.ordinal() < result.ordinal()) {
                    result = next;
                    if (result == DATATYPE_MATCH.NO_MATCH)
                        return DATATYPE_MATCH.NO_MATCH;
                }
            }
            return result;
        }
        return DATATYPE_MATCH.NO_MATCH;
    }

    private static void loadBuiltins() {
        funcCache = new HashMap<String, Collection<FunctionDef>>();
        for (FunctionDef funcDef : BUILTIN_FUNCTIONS) {
            Collection<FunctionDef> overloads = funcCache.get(funcDef.funcName);
            if (overloads == null) {
                overloads = new LinkedList<FunctionDef>();
                funcCache.put(funcDef.funcName, overloads);
            }
            boolean exists = false;
            for (FunctionDef other : overloads) {
                if (matchingParams(funcDef.funcParamTypes, other.funcParamTypes) == DATATYPE_MATCH.EXACT_MATCH) {
                    exists = true;
                    break;
                }
            }
            if (!exists) {
                overloads.add(funcDef);
            }
        }
    }

    private static int loadConfigurations(String funcName) {
        int loaded = 0;
        Collection<FunctionDef> overloads = funcCache.get(funcName);
        if (overloads == null) {
            overloads = new LinkedList<FunctionDef>();
            funcCache.put(funcName, overloads);
        }
        for (int overloadid = 0;; overloadid++) {
            String function = funcName
                    + (overloadid == 0 ? "" : "," + overloadid);
            // Check paramcount
            String paramCountKey = "xdb.sqlfunction." + function
                    + ".paramcount";
            String paramCountVal = Property.get(paramCountKey);
            int[] params = null;
            if (paramCountVal != null) {
                try {
                    int paramCount = Integer.parseInt(paramCountVal);
                    params = new int[paramCount];
                } catch (NumberFormatException nfe) {
                    throw new XDBServerException(
                            "Invalid function definition parameter in the config file: "
                                    + paramCountKey);
                }
            } else {
                break;
            }
            for (int i = 0; i < params.length; i++) {
                String paramTypeStr = Property.get("xdb.sqlfunction."
                        + function + ".arg" + i);
                if (paramTypeStr == null) {
                    throw new XDBServerException("Parameter " + i
                            + " of function " + function
                            + " is not found in the config file");
                }
                params[i] = DataTypes.getJavaType(paramTypeStr);
            }
            ExpressionType exprT = null;
            String paramTypeStr = Property.get("xdb.sqlfunction." + function
                    + ".returntype");
            if (paramTypeStr != null) {
                int i = paramTypeStr.indexOf("(");
                if (i < 0) {
                    exprT = new ExpressionType(
                            DataTypes.getJavaType(paramTypeStr));
                } else {
                    int javaType;
                    int length = -1;
                    int precision = -1;
                    int scale = -1;
                    javaType = DataTypes.getJavaType(paramTypeStr.substring(0,
                            i).trim());
                    switch (javaType) {
                        case Types.VARCHAR:
                        case Types.FLOAT:
                        case Types.REAL:
                            // parse length
                            int j = paramTypeStr.indexOf(")", i + 1);
                            if (j > i + 1) {
                                try {
                                    length = Integer.parseInt(paramTypeStr
                                            .substring(i + 1, j).trim());
                                } catch (NumberFormatException nfe) {
                                }
                            }
                            break;
                        case Types.DECIMAL:
                        case Types.NUMERIC:
                            // parse precision and scale
                            j = paramTypeStr.indexOf(")", i + 1);
                            if (j > i + 1) {
                                int k = paramTypeStr.indexOf(",", i + 1);
                                if (k > i + 1 && k + 1 < j) {
                                    try {
                                        precision = Integer
                                                .parseInt(paramTypeStr
                                                        .substring(i + 1, k)
                                                        .trim());
                                    } catch (NumberFormatException nfe) {
                                    }
                                    try {
                                        scale = Integer.parseInt(paramTypeStr
                                                .substring(k + 1, j).trim());
                                    } catch (NumberFormatException nfe) {
                                    }
                                } else {
                                    try {
                                        precision = Integer
                                                .parseInt(paramTypeStr
                                                        .substring(i + 1, j)
                                                        .trim());
                                    } catch (NumberFormatException nfe) {
                                    }
                                    scale = 0;
                                }
                            }
                            break;
                        default:
                    }
                    exprT = new ExpressionType(javaType, length, precision,
                            scale);
                }
            }
            boolean exists = false;
            for (Iterator<FunctionDef> it = overloads.iterator(); it.hasNext();) {
                FunctionDef other = it.next();
                if (matchingParams(params, other.funcParamTypes) == DATATYPE_MATCH.EXACT_MATCH) {
                    if ((other.funcReturnType == null && exprT == null)
                            || exprT.equals(other.funcReturnType)) {
                        exists = true;
                    } else {
                        it.remove();
                    }
                    break;
                }
            }
            if (!exists) {
                overloads.add(new FunctionDef(funcName, params, exprT));
            }
        }
        return loaded;
    }

    public static ExpressionType analyzeFunction(SqlExpression funcExpr,
            Command commandToExecute) {
        if (funcCache == null) {
            loadBuiltins();
        }
        String funcName = funcExpr.getFunctionName();
        Collection<FunctionDef> overloads = funcCache.get(funcName);
        if (overloads == null) {
            if (loadConfigurations(funcName) == 0) {
                funcExpr.setExprDataType(new ExpressionType());
                return funcExpr.getExprDataType();
            }
            overloads = funcCache.get(funcName);
        }
        int[] params = null;
        List<SqlExpression> paramExprs = funcExpr.getFunctionParams();
        if (paramExprs != null && paramExprs.size() > 0) {
            params = new int[paramExprs.size()];
            for (int i = 0; i < paramExprs.size(); i++) {
                SqlExpression sqlexpr = paramExprs.get(i);
                SqlExpression
                        .setExpressionResultType(sqlexpr, commandToExecute);
                ExpressionType exprType = sqlexpr.getExprDataType();
                params[i] = exprType == null ? Types.NULL : exprType.type;
            }
        }
        FunctionDef matchingDef = null;
        DATATYPE_MATCH best_match = DATATYPE_MATCH.NO_MATCH;
        for (FunctionDef funcDef : overloads) {
            DATATYPE_MATCH match;
            match = matchingParams(params, funcDef.funcParamTypes);
            if (match.ordinal() > best_match.ordinal()) {
                matchingDef = funcDef;
                best_match = match;
                if (best_match == DATATYPE_MATCH.EXACT_MATCH)
                    break;
            }
        }
        return matchingDef == null ? new ExpressionType() : matchingDef.funcReturnType;
    }

    /**
     *
     * @return ExpressionType which is returned by this particular expression
     * @param commandToExecute
     * @param functionExpr The Sql Expression which holds information regarding the
     *            function
     */
    public static ExpressionType analyzeAverageParameter(
            SqlExpression functionExpr, Command commandToExecute) {
        // Get the SQL Arguments -- We expect only one argument for this
        // function - The function is a SET
        // Function and can take any expresson which yields a numeric result.
        ExpressionType expressionType = null;
        if (functionExpr.getFunctionParams().size() > 0) {

            SqlExpression aSqlExpression = functionExpr.getFunctionParams().get(0);
            expressionType = SqlExpression
            .setExpressionResultType(aSqlExpression, commandToExecute);
            if (!expressionType.isNumeric() && !expressionType.isBit()
                    && !expressionType.isCharacter()) {
                throw new XDBServerException(
                        ErrorMessageRepository.INVALID_DATATYPE + "( "
                        + aSqlExpression.getExprString() + " )", 0,
                        ErrorMessageRepository.INVALID_DATATYPE_CODE);
            }

            ExpressionType expressionTypeToReturn = null;

            switch (expressionType.type) {
            case ExpressionType.SMALLINT_TYPE:
            case ExpressionType.INT_TYPE:
            case ExpressionType.BIGINT_TYPE:
                expressionTypeToReturn = new ExpressionType(Types.NUMERIC);
                break;
            case ExpressionType.FLOAT_TYPE:
                expressionTypeToReturn = new ExpressionType(Types.DOUBLE);
                break;
            default:
                expressionTypeToReturn = expressionType;
            }
            return expressionTypeToReturn;

        }
        return expressionType;
    }

    /**
     *
     * @return ExpressionType which is returned by this particular expression
     * @param functionExpr The Sql Expression which holds information regarding the
     *            function
     */

    public static ExpressionType analyzeCountParameter(
            SqlExpression functionExpr) {
        ExpressionType aExpressionType = new ExpressionType();
        aExpressionType.setExpressionType(
                ExpressionType.BIGINT_TYPE, ExpressionType.BIGINTLEN,
                0, 0);
        return aExpressionType;
    }

    /**
     *
     * @param functionExpr
     * @return
     */
    public static ExpressionType analyzeSubstring(SqlExpression functionExpr) {
        SqlExpression aSqlExpression = functionExpr.getFunctionParams().get(0);
        ExpressionType paramType = aSqlExpression.getExprDataType();
        // The result is of the same type as the first parameter
        ExpressionType exprType = new ExpressionType();
        exprType.setExpressionType(paramType.type, paramType.length, 0, 0);
        return exprType;
    }

    /**
     *
     * @param functionExpr
     * @return
     */
    public static ExpressionType analyzeExtract(SqlExpression functionExpr) {
        ExpressionType exprType = new ExpressionType();
        exprType.setExpressionType(ExpressionType.INT_TYPE, 0, 0, 0);
        return exprType;
    }

    /**
     *
     * Parameters: ( LEADING | TRAILING | BOTH ) [str1] FROM str2 
     * Return type is the same as str2 (last argument) 
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     */

    public static ExpressionType analyzeTrim(SqlExpression functionExpr) {
        SqlExpression aSqlExpression = functionExpr.getFunctionParams().get(
                functionExpr.getFunctionParams().size() - 1);
        ExpressionType paramType = aSqlExpression.getExprDataType();
        ExpressionType exprType = new ExpressionType();
        exprType.setExpressionType(paramType.type, paramType.length, 0, 0);
        return exprType;
    }

    /**
     *
     * @param functionExpr
     * @return
     */
    public static ExpressionType analyzeOverlay(SqlExpression functionExpr) {
        SqlExpression arg1Expression = functionExpr.getFunctionParams().get(0);
        // Parameter with index 1 is the 'placing' keyword
        SqlExpression arg2Expression = functionExpr.getFunctionParams().get(2);
        ExpressionType param1Type = arg1Expression.getExprDataType();
        ExpressionType param2Type = arg2Expression.getExprDataType();
        ExpressionType exprType = new ExpressionType();
        if (param1Type.length > 0 && param2Type.length > 0)
            exprType.setExpressionType(Types.VARCHAR, 
                    param1Type.length + param2Type.length, 0, 0);
        else
            exprType.setExpressionType(Types.CLOB, 0, 0, 0);
        return exprType;
    }

    /**
     *
     * @param functionExpr
     * @return
     */
    public static ExpressionType analyzePosition(SqlExpression functionExpr) {
        ExpressionType exprType = new ExpressionType();
        exprType.setExpressionType(ExpressionType.INT_TYPE, 0, 0, 0);
        return exprType;
    }

    /**
     * Input : Any Output: Same as input
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     * @throws IllegalArgumentException
     *             Incase the argument type does not match the signature of the
     *             function
     */
    public static ExpressionType analyzeMax_MinParameter(
            SqlExpression functionExpr, Command commandToExecute) {
        // Only one parameter expected
        SqlExpression aSqlExpression = functionExpr.getFunctionParams().get(0);

        if (aSqlExpression.getExprDataType() == null
                || aSqlExpression.getExprDataType().type == 0) {
            SqlExpression.setExpressionResultType(aSqlExpression, commandToExecute);
        }
        if (aSqlExpression.getExprDataType().type == ExpressionType.INTERVAL_TYPE) {
            ExpressionType exprT = new ExpressionType();
            exprT.setExpressionType(ExpressionType.VARCHAR_TYPE, 50, 0, 0);
            return exprT;
        }

        return aSqlExpression.getExprDataType();

    }

    /**
     * Functions which take in Input - Numeric Type OutPut - Float Throw -
     * Exception on AlphaNumeric Type NumberOfParameterAllowed - 1
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     * @throws IllegalArgumentException
     *             Incase the argument type does not match the signature of the
     *             function
     */
    public static ExpressionType analyzeSumParameter(
            SqlExpression functionExpr, Command commandToExecute) {
        // Get the SQL Arguments -- We expect only one argument for this
        // function - The function is a SET
        // Function and can take any expresson which yields a numeric result.
        ExpressionType expressionTypeToReturn = null;
        ExpressionType expressionType = null;

        if (functionExpr.getFunctionParams().size() > 0) {
            SqlExpression aSqlExpression = functionExpr.getFunctionParams().get(0);
            expressionType = SqlExpression
            .setExpressionResultType(aSqlExpression, commandToExecute);
            if (!expressionType.isNumeric() && !expressionType.isBit()
                    && !expressionType.isCharacter()) {
                throw new XDBServerException(
                        ErrorMessageRepository.INVALID_DATATYPE + " ( "
                        + aSqlExpression.rebuildString() + " ) ", 0,
                        ErrorMessageRepository.INVALID_DATATYPE_CODE);
            }

            switch (expressionType.type) {
            case ExpressionType.BOOLEAN_TYPE:
            case ExpressionType.SMALLINT_TYPE:
            case ExpressionType.INT_TYPE:
                expressionTypeToReturn = new ExpressionType(Types.BIGINT);
                break;
            case ExpressionType.BIGINT_TYPE:
                expressionTypeToReturn = new ExpressionType(Types.NUMERIC);
                break;
            case ExpressionType.FLOAT_TYPE:
                expressionTypeToReturn = new ExpressionType(Types.DOUBLE);
                break;
            default:
                expressionTypeToReturn = expressionType;
            }
            return expressionTypeToReturn;
        }
        return expressionType;
    }

    /**
     *
     * Input: sqlExpr1, sqlExpr2 Output: Null/sqlExpr1
     *
     * @param functionExpr
     * @return
     */
    public static ExpressionType analyzeNullIf(SqlExpression functionExpr) {
        SqlExpression aSqlExpression = functionExpr.getFunctionParams().get(0);
        ExpressionType paramType = aSqlExpression.getExprDataType();
        // The result is of the same type as the first parameter
        ExpressionType exprType = new ExpressionType();
        exprType.setExpressionType(paramType.type, paramType.length, 
                paramType.precision, paramType.scale);
        return exprType;
    }

    /**
     * Input: sqlExpr Output: type of sqlExpr
     */
    public static ExpressionType analyzeBitAnd(SqlExpression functionExpr) {
        SqlExpression aSqlExpression1 = functionExpr.getFunctionParams().get(0);
        //set the return type same as that of sqlExpr
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(aSqlExpression1.getExprDataType().type, 
                aSqlExpression1.getExprDataType().length, 
                aSqlExpression1.getExprDataType().precision, 
                aSqlExpression1.getExprDataType().scale);
        return exprT;

    }

    /**
     * Input: BOOLEAN Output: BOOLEAN
     */
    public static ExpressionType analyzeBoolAnd(SqlExpression functionExpr) {

        functionExpr.getFunctionParams().get(0);
        //set the return type as BOOLEAN
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.BOOLEAN_TYPE, -1, -1, -1);
        return exprT;

    }

    /**
     *
     * Inout : Any data-type Out : Any data-type
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @param commandToExecute
     * @return ExpressionType which is returned by this particular expression
     */
    public static ExpressionType analyzeCoalesce(
            SqlExpression functionExpr, Command commandToExecute) {
        ExpressionType returnType = null;
        for (SqlExpression parameter : functionExpr.getFunctionParams()) {
            if (parameter.getExprDataType() != null && parameter.getExprDataType().type != Types.NULL) {
                if (returnType == null) {
                    returnType = parameter.getExprDataType();
                } else if (returnType.isNumeric() && parameter.getExprDataType().isNumeric()) {
                    returnType = ExpressionType.MergeNumericTypes(
                            returnType, parameter.getExprDataType());
                } else if (returnType.type == parameter.getExprDataType().type) {
                    if (parameter.getExprDataType().length > returnType.length)
                        returnType.length = parameter.getExprDataType().length; 
                } else {
                    ExpressionType exprT = new ExpressionType();
                    exprT.setExpressionType(Types.CLOB, 0, 0, 0);
                    return exprT;
                }
            }
        }
        return returnType;
    }

    // ---------------------String Function Start here
    // --------------------------------------------------

    /**
     *
     * @param functionExpr
     * @return
     */

    public static ExpressionType analyzeVarianceOrStddev(
            SqlExpression functionExpr) {
        ExpressionType exprT = null;
        if (functionExpr.getFunctionParams().get(0).getExprType() == ExpressionType.FLOAT_TYPE) {
            exprT = new ExpressionType(Types.DOUBLE);
        } else {
            exprT = new ExpressionType(Types.NUMERIC);
        }
        return exprT;
    }
    /**
     * Input: double precision, double precision Output: double precision
     */
    public static ExpressionType analyzeCoRegFunc(SqlExpression functionExpr) {
        //set the return type as DOUBLE PRECISION
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.DOUBLEPRECISION_TYPE, 32, 0, 0);
        return exprT;
    }

    /**
     * Input: double precision, double precision Output: bigint
     */
    public static ExpressionType analyzeRegrCount(SqlExpression functionExpr) {
        //set the return type as BIGINT
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.BIGINT_TYPE, 32, 0, 0);
        return exprT;
    }

    /**
     *
     * @param functionExpr
     * @return
     */

    public static ExpressionType analyzeConvert(SqlExpression functionExpr) {
        ExpressionType exprT = new ExpressionType();
        SqlExpression aSqlExp1 = functionExpr.getFunctionParams()
        .get(0);

        int requiredLength = aSqlExp1.getExprDataType().length;

        exprT.setExpressionType(ExpressionType.CHAR_TYPE,
                requiredLength * 2 + 1, 0, 0);
        return exprT;
    }

    /**
     * Check to see if the function is a group function
     *
     * @param functionId
     *            The function id of the function
     * @return true if it is a group function
     */
    public static boolean isGroupFunction(int functionId) {
        if (functionId == IFunctionID.COUNT_ID
                || functionId == IFunctionID.SUM_ID
                || functionId == IFunctionID.MAX_ID
                || functionId == IFunctionID.MIN_ID
                || functionId == IFunctionID.AVG_ID
                || functionId == IFunctionID.STDEV_ID
                || functionId == IFunctionID.STDEVPOP_ID
                || functionId == IFunctionID.STDEVSAMP_ID
                || functionId == IFunctionID.VARIANCE_ID
                || functionId == IFunctionID.VARIANCEPOP_ID
                || functionId == IFunctionID.VARIANCESAMP_ID
                || functionId == IFunctionID.BOOLAND_ID
                || functionId == IFunctionID.EVERY_ID
                || functionId == IFunctionID.BOOLOR_ID
                || functionId == IFunctionID.BITAND_ID
                || functionId == IFunctionID.BITOR_ID) {
            return true;
        } else {
            return false;
        }
    }
}
