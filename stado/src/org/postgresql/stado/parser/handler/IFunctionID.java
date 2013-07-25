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
package org.postgresql.stado.parser.handler;

/**
 * This interface holds ID's for all the Funtions that we support. Please change
 * the MAXID in this file so that we dont assign the same ID to two function.
 */
public interface IFunctionID {
    /**
     * Please update this variable as you bump the numbers up.
     */
    public static int MAXID = 137;

    /**
     * Date Time Function.
     */
    public static int SUBSTRING_ID = 20;

    public static int EXTRACT_ID = 109;

    public static int TRIM_ID = 72;

    public static int CONVERT_ID = 120;

    public static int OVERLAY_ID = 122;

    public static int POSITION_ID = 123;

    public static int CUSTOM_ID = 105;

    public static int CAST_ID = 106;

    public static int NULLIF_ID = 148;
    
    public static int COALESCE_ID = 188;

    // Aggreagate Functions
    public static int AVG_ID = 75;

    public static int COUNT_ID = 76;

    public static int COUNT_STAR_ID = 77;

    public static int STDEV_ID = 78;

    public static int VARIANCE_ID = 79;

    public static int MAX_ID = 80;

    public static int MIN_ID = 81;

    public static int SUM_ID = 82;

    public static int BITAND_ID = 164;

    public static int BITOR_ID = 165;

    public static int BOOLAND_ID = 166;

    public static int BOOLOR_ID = 167;

    public static int EVERY_ID = 168;

    // Statistical Aggreagate Functions
    public static int CORR_ID = 169;

    public static int COVARPOP_ID = 170;

    public static int COVARSAMP_ID = 171;

    public static int REGRAVX_ID = 172;

    public static int REGRAVY_ID = 173;

    public static int REGRCOUNT_ID = 174;

    public static int REGRINTERCEPT_ID = 175;

    public static int REGRR2_ID = 176;

    public static int REGRSLOPE_ID = 177;

    public static int REGRSXX_ID = 178;

    public static int REGRSXY_ID = 179;

    public static int REGRSYY_ID = 180;

    public static int STDEVPOP_ID = 181;

    public static int STDEVSAMP_ID = 182;

    public static int VARIANCEPOP_ID = 183;

    public static int VARIANCESAMP_ID = 184;
    
    // Misc Functions
    public static int CASE_ID = 86;

    public static int ST_EXTENT_ID = 201;

    public static int ST_EXTENT3D_ID = 202;

    public static int ST_COLLECT_AGG_ID = 203;
}
