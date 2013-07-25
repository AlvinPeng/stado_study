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
 * DataTypeHandler.java
 *
 *
 */
package org.postgresql.stado.parser.handler;

import java.sql.Types;
import java.util.HashMap;

import org.postgresql.stado.common.util.ParseCmdLine;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.parser.ExpressionType;
import org.postgresql.stado.parser.core.syntaxtree.BLOBDataType;
import org.postgresql.stado.parser.core.syntaxtree.BigIntDataType;
import org.postgresql.stado.parser.core.syntaxtree.BigSerialDataType;
import org.postgresql.stado.parser.core.syntaxtree.BitDataType;
import org.postgresql.stado.parser.core.syntaxtree.BooleanDataType;
import org.postgresql.stado.parser.core.syntaxtree.Box2DDataType;
import org.postgresql.stado.parser.core.syntaxtree.Box3DDataType;
import org.postgresql.stado.parser.core.syntaxtree.Box3DExtentDataType;
import org.postgresql.stado.parser.core.syntaxtree.CharachterDataType;
import org.postgresql.stado.parser.core.syntaxtree.CidrDataType;
import org.postgresql.stado.parser.core.syntaxtree.DateDataType;
import org.postgresql.stado.parser.core.syntaxtree.DecimalDataType;
import org.postgresql.stado.parser.core.syntaxtree.DoublePrecision;
import org.postgresql.stado.parser.core.syntaxtree.FixedDataType;
import org.postgresql.stado.parser.core.syntaxtree.FloatDataType;
import org.postgresql.stado.parser.core.syntaxtree.GeometryDataType;
import org.postgresql.stado.parser.core.syntaxtree.InetDataType;
import org.postgresql.stado.parser.core.syntaxtree.IntegerDataType;
import org.postgresql.stado.parser.core.syntaxtree.IntervalDataType;
import org.postgresql.stado.parser.core.syntaxtree.IntervalQualifier;
import org.postgresql.stado.parser.core.syntaxtree.LengthSpec;
import org.postgresql.stado.parser.core.syntaxtree.MacAddrDataType;
import org.postgresql.stado.parser.core.syntaxtree.NationalCharDataType;
import org.postgresql.stado.parser.core.syntaxtree.NodeOptional;
import org.postgresql.stado.parser.core.syntaxtree.NodeSequence;
import org.postgresql.stado.parser.core.syntaxtree.NodeToken;
import org.postgresql.stado.parser.core.syntaxtree.NumericDataType;
import org.postgresql.stado.parser.core.syntaxtree.PrecisionSpec;
import org.postgresql.stado.parser.core.syntaxtree.RealDataType;
import org.postgresql.stado.parser.core.syntaxtree.RegClassDataType;
import org.postgresql.stado.parser.core.syntaxtree.SerialDataType;
import org.postgresql.stado.parser.core.syntaxtree.SmallIntDataType;
import org.postgresql.stado.parser.core.syntaxtree.TextDataType;
import org.postgresql.stado.parser.core.syntaxtree.TimeDataType;
import org.postgresql.stado.parser.core.syntaxtree.TimeStampDataType;
import org.postgresql.stado.parser.core.syntaxtree.UnsignedZeroFillSpecs;
import org.postgresql.stado.parser.core.syntaxtree.VarBitDataType;
import org.postgresql.stado.parser.core.syntaxtree.VarCharDataType;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;

/**
 * This class represents a specific data type and contains information about different
 * attributes of a data type.
 *
 *
 *
 */
public class DataTypeHandler extends DepthFirstVoidArguVisitor implements TypeConstants {

    /**
     * This must be in synch with DatetimeField definition in grammar file, Order
     * is important !
     */
    private static final String[] INTERVAL_QUALIFIERS = { "YEAR", "QUARTER",
            "MONTH", "WEEK", "DAY", "HOUR", "MINUTE", "SECOND" };

    private int sqlType;

    private boolean isSerial;

    private int length = -1;

    private int precision = -1;

    private int scale = -1;

    private boolean unsigned;

    private boolean zerofill;

    /**
     * Class constructor.
     * @param sqlType The JDBC type code for the data type.
     * @param length The length of the data type.
     * @param precision The precision value (if any) associated with the data type.
     * @param scale The scale value (if any) associated with the data type.
     */
    public DataTypeHandler(int sqlType, int length, int precision, int scale) {
        this.sqlType = sqlType;
        this.length = length;
        this.precision = precision;
        this.scale = scale;
    }

    /**
     * Class constructor.
     */
    public DataTypeHandler() {
    }

    /**
     * f0 -> <SERIAL_>
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public void visit(SerialDataType n, Object argu) {
        sqlType = Types.INTEGER;
        isSerial = true;
    }

    /**
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public void visit(BigSerialDataType n, Object argu) {
        sqlType = Types.BIGINT;
        isSerial = true;
    }

    /**
     *  f0 -> ( <SMALLINT_> | <INT2_> ) [ <SERIAL_> ] UnsignedZeroFillSpecs()
     *       | <TINYINT_>
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public void visit(SmallIntDataType n, Object argu) {
        sqlType = Types.SMALLINT;
        switch (n.f0.which) {
        case 0:
            if (((NodeOptional) ((NodeSequence) n.f0.choice).elementAt(1)).present()) {
                isSerial = true;
            }
            break;
        }
        n.f0.accept(this, argu);
    }

    /**
     * Grammar production: f0 -> ( <BIGINT_> | <BIGINTEGER_> ) f1 -> [ <SERIAL_> ]
     * f2 -> UnsignedZeroFillSpecs()
     */
    @Override
    public void visit(BigIntDataType n, Object argu) {
        sqlType = Types.BIGINT;
        if (n.f1.present()) {
            isSerial = true;
        }
        n.f2.accept(this, argu);
    }

    /**
     * f0 -> ( <INT_> | <INTEGER_> ) f1 -> [ <SERIAL_> ] f2 ->
     * UnsignedZeroFillSpecs()
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public void visit(IntegerDataType n, Object argu) {
        sqlType = Types.INTEGER;
        if (n.f1.present()) {
            isSerial = true;
        }
        n.f2.accept(this, argu);
    }

    /**
     * Grammar production:
     * f0 -> ( <REAL_> | <SMALLFLOAT_> | <FLOAT4_> )
     * f1 -> UnsignedZeroFillSpecs()
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public void visit(RealDataType n, Object argu) {
        sqlType = Types.REAL;
        length = 24;
        n.f0.accept(this, argu);
    }

    /**
     * Grammar production: f0 -> <FLOAT_> f1 -> [ LengthSpec() ] f2 ->
     * UnsignedZeroFillSpecs()
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public void visit(FloatDataType n, Object argu) {
        sqlType = Types.FLOAT;
        length = 32; // Default
        n.f1.accept(this, argu);
        n.f2.accept(this, argu);
    }

    /**
     * f0 -> <DOUBLE_PRECISION_>
     *        | <FLOAT8_>
     * @param n
     * @param argu
     * @return
     */
    @Override
    public void visit(DoublePrecision n, Object argu) {
        sqlType = Types.DOUBLE;
        length = 48;
    }

    /**
     * f0 -> ( <NUMERIC_> | <NUMBER_> ) [ PrecisionSpec() ] UnsignedZeroFillSpecs()
     *       | <MONEY_>
     *       | <SMALLMONEY_>
     *       | <YEAR_>
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public void visit(NumericDataType n, Object argu) {
        sqlType = Types.NUMERIC;
        switch (n.f0.which) {
        case 1:
            //<MONEY_>
            precision = 19;
            scale = 4;
            break;
        case 2:
            //<SMALLMONEY_>
            precision = 10;
            scale = 4;
            break;
        case 3:
            //<YEAR_>
            precision = 4;
            scale = 0;
            break;
        }
        n.f0.accept(this, argu);
    }

    /**
     * Grammar production: f0 -> <BIT_> f1 -> [ LengthSpec() ]
     */
    @Override
    public void visit(BitDataType n, Object argu) {
        sqlType = Types.BINARY;
        length = 1; // Default
        scale = 0; // In bit or varbit types 0 is bit.
        n.f1.accept(this, argu);
    }

    /**
     * Grammar production: f0 -> ( <VARBIT_> | ( <BIT_> <VARYING_> ) ) f1 -> [
     * LengthSpec() ]
     */

    @Override
    public void visit(VarBitDataType n, Object argu) {
        sqlType = Types.VARBINARY;
        scale = 1; // In bit or varbit types 1 is varbit.
        if (n.f1.present()) {
            n.f1.accept(this, argu);
        } else {
            length = -1;
        }
    }

    /**
     * f0 -> ( <DECIMAL_> | <DEC_> )
     * f1 -> [ PrecisionSpec() ]
     * f2 -> UnsignedZeroFillSpecs()
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public void visit(DecimalDataType n, Object argu) {
        sqlType = Types.DECIMAL;
        n.f0.accept(this, argu);
        n.f1.accept(this, argu);
        n.f2.accept(this, argu);
    }

    /**
     * f0 -> <FIXED_> f1 -> [PrecisionSpec()] f2 -> UnsignedZeroFillSpecs()
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public void visit(FixedDataType n, Object argu) {
        sqlType = Types.NUMERIC;
        precision = 5; // Default
        scale = 0; // Default
        n.f1.accept(this, argu);
        n.f2.accept(this, argu);
    }

    /**
     * f0 -> <TIMESTAMP_> [ LengthSpec() ] [ ( <WITH_TIMEZONE_> | <WITHOUT_TIMEZONE_> ) ]
     *       | <DATETIME_>
     *       | <SAMLLDATETIME_>
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public void visit(TimeStampDataType n, Object argu) {
        sqlType = Types.TIMESTAMP;
        switch (n.f0.which) {
        case 0:
        case 1:
            length = 3;
            break;
        case 2:
            length = 0;
            break;
        }
        n.f0.accept(this, argu);
    }

    /**
     * f0 -> <TIME_>
     * f1 -> [ LengthSpec() ]
     * f2 -> [ ( <WITH_TIMEZONE_> | <WITHOUT_TIMEZONE_> ) ]
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public void visit(TimeDataType n, Object argu) {
        sqlType = Types.TIME;
        n.f1.accept(this, argu);
    }

    /**
     * f0 -> <DATE_>
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public void visit(DateDataType n, Object argu) {
        sqlType = Types.DATE;
    }

    /**
     * f0 -> <BOOLEAN_>
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public void visit(BooleanDataType n, Object argu) {
        sqlType = Types.BOOLEAN;
    }

    /**
     * Grammar production: f0 -> DatetimeField() f1 -> <TO_> f2 ->
     * DatetimeField()
     */
    @Override
    public void visit(IntervalQualifier n, Object argu) {
        precision = n.f0.f0.which;
        scale = n.f2.f0.which;
    }

    /**
     * Grammar production: f0 -> <INTERVAL_> f1 -> [ IntervalQualifier() ]
     */
    @Override
    public void visit(IntervalDataType n, Object argu) {
        sqlType = ExpressionType.INTERVAL_TYPE;
        if (n.f1.present()) {
            n.f1.accept(this, argu);
        }
    }

    /**
     * Grammar production: f0 -> <TEXT_>
     *       | <CLOB_>
     *       | <LONG_>
     *       | <LONG_VARCHAR_>
     *       | <LONGTEXT_>
     *       | <LVARCHAR_>
     *       | <MEDIUMTEXT_>
     */

    @Override
    public void visit(TextDataType n, Object argu) {
        sqlType = Types.CLOB;
    }

    /**
     * Grammar production: f0 -> <BLOB_>
     *       | <BYTEA_>
     *       | <BYTE_>
     *       | <BINARY_>
     *       | <IMAGE_>
     *       | <LONG_RAW_>
     *       | <RAW_>
     *       | <VARBINARY_>
     */
    @Override
    public void visit(BLOBDataType n, Object argu) {
        sqlType = Types.BLOB;
    }

    /**
     * Grammar production: f0 -> ( <CHARACHTER_> | <CHAR_> ) f1 -> [ <VARYING_> ]
     * f2 -> [LengthSpec()]
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public void visit(CharachterDataType n, Object argu) {
        if (n.f1.present()) {
            sqlType = Types.VARCHAR;
        } else {
            sqlType = Types.CHAR;
        }

        length = 1; // Default
        n.f2.accept(this, argu);
    }

    /**
     * f0 -> ( <VARCHAR_> | <VARCHAR2_> ) [ LengthSpec() ]
     *       | <TINYTEXT_>
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public void visit(VarCharDataType n, Object argu) {
        sqlType = Types.VARCHAR;
        // TINYTEXT has length 255, other have defaul length 1
        if (n.f0.which == 1) {
            length = 255;
        } else {
            length = 1;
        }
        n.f0.accept(this, argu);// Specs for varchar - similar to char
    }

    /**
     * f0 -> ( <NCHAR_> | <NATIONAL_> ( <CHAR_> | <CHARACHTER_> ) ) f1 -> [
     * <VARYING_> ] f2 -> [LengthSpec()]
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public void visit(NationalCharDataType n, Object argu) {
        if (n.f1.present()) {
            sqlType = Types.VARCHAR;
        } else {
            sqlType = Types.CHAR;
        }
        length = 1; // Default
        n.f2.accept(this, argu);
    }

    /**
     * f0 -> <PARENTHESIS_START_> f1 -> <DECIMAL_LITERAL> f2 ->
     * <PARENTHESIS_CLOSE_>
     *
     * @param n
     * @param argu
     * @return
     *
     */
    @Override
    public void visit(LengthSpec n, Object argu) {
        try {
            length = Integer.parseInt(n.f1.tokenImage);
        } catch (NumberFormatException notNumber) {
            throw new XDBServerException(
                    ErrorMessageRepository.ILLEGAL_PARAMETER, 0,
                    ErrorMessageRepository.ILLEGAL_PARAMETER_CODE);
        }
    }

    /**
     * f0 -> <PARENTHESIS_START_> f1 -> <DECIMAL_LITERAL> f2 -> [ <COMMA_>
     * <DECIMAL_LITERAL> ] f3 -> <PARENTHESIS_CLOSE_>
     *
     * @param n
     * @param argu
     * @return This function handles the precision spec
     */
    @Override
    public void visit(PrecisionSpec n, Object argu) {
        try {
            precision = Integer.parseInt(n.f1.tokenImage);
        } catch (NumberFormatException notNumber) {
            throw new XDBServerException(
                    ErrorMessageRepository.ILLEGAL_PARAMETER, 0,
                    ErrorMessageRepository.ILLEGAL_PARAMETER_CODE);
        }
        if (n.f2.present()) {
            NodeSequence aNodeSequence = (NodeSequence) n.f2.node;
            NodeToken scaleToken = (NodeToken) aNodeSequence.elementAt(1);
            try {
                scale = Integer.parseInt(scaleToken.tokenImage);
            } catch (Exception e) {
                throw new XDBServerException(
                        ErrorMessageRepository.ILLEGAL_PARAMETER, 0,
                        ErrorMessageRepository.ILLEGAL_PARAMETER_CODE);
            }
        }
    }

    /**
     * f0 -> (<UNSIGNED_>)? f1 -> (<ZEROFILL_>)?
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public void visit(UnsignedZeroFillSpecs n, Object argu) {
        unsigned = n.f0.present();
        zerofill = n.f1.present();
    }

    /**
     * f0 -> <MACADDR_>
     */

    @Override
    public void visit(MacAddrDataType n, Object argu) {
        sqlType = ExpressionType.MACADDR_TYPE;
    }

    /**
     * f0 -> <CIDR_>
     */

    @Override
    public void visit(CidrDataType n, Object argu) {
        sqlType = ExpressionType.CIDR_TYPE;
    }

    /**
     * f0 -> <INET_>
     */

    @Override
    public void visit(InetDataType n, Object argu) {
        sqlType = ExpressionType.INET_TYPE;
    }

    /**
     * f0 -> <GEOMETRY_>
     */

    @Override
    public void visit(GeometryDataType n, Object argu) {
        sqlType = ExpressionType.GEOMETRY_TYPE;
    }

    /**
     * f0 -> <BOX2D_>
     */

    @Override
    public void visit(Box2DDataType n, Object argu) {
        sqlType = ExpressionType.BOX2D_TYPE;
    }
    /**
     * f0 -> <BOX3D_>
     */

    @Override
    public void visit(Box3DDataType n, Object argu) {
        sqlType = ExpressionType.BOX3D_TYPE;
    }
    /**
     * f0 -> <BOX3DEXTENT_>
     */

    @Override
    public void visit(Box3DExtentDataType n, Object argu) {
        sqlType = ExpressionType.BOX3DEXTENT_TYPE;
    }

    /**
     * f0 -> <REGCLASS_>
     */

    @Override
    public void visit(RegClassDataType n, Object argu) {
        sqlType = ExpressionType.REGCLASS_TYPE;
    }

    /**
     * @return Returns the isSerial.
     */
    public boolean isSerial() {
        return isSerial;
    }

    /**
     * @return Returns the length.
     */
    public int getLength() {
        return length;
    }

    /**
     * @return Returns the precision.
     */
    public int getPrecision() {
        return precision;
    }

    /**
     * @return Returns the scale.
     */
    public int getScale() {
        return scale;
    }

    /**
     * @return Returns the sqlType.
     */
    public int getSqlType() {
        return sqlType;
    }

    /**
     * @return Returns the typeString.
     */
    public String getTypeString() {
        String typeString = "";
        HashMap<String,String> map = new HashMap<String,String>();
        map.put("length", "" + length);
        map.put("precision", "" + precision);
        map.put("scale", "" + scale);
        switch (sqlType) {
        case Types.INTEGER:
            typeString = INTEGER_TEMPLATE;
            break;
        case Types.BINARY:
            if (scale == 0) {
                typeString = BIT_TEMPLATE;
            } else {
                if (length == -1) {
                    typeString = VARBIT_TEMPLATE_WITHOUT_LENGTH;
                } else {
                    typeString = VARBIT_TEMPLATE;
                }
            }
            break;
        case Types.VARBINARY:
            if (length == -1) {
                typeString = VARBIT_TEMPLATE_WITHOUT_LENGTH;
            } else {
                typeString = VARBIT_TEMPLATE;
            }
            break;
        case Types.BIGINT:
            typeString = BIGINTEGER_TEMPLATE;
            break;
        case Types.SMALLINT:
            typeString = SMALLINT_TEMPLATE;
            break;
        case Types.REAL:
            typeString = REAL_TEMPLATE;
            break;
        case Types.FLOAT:
            typeString = FLOAT_TEMPLATE;
            break;
        case Types.DOUBLE:
            typeString = DOUBLE_TEMPLATE;
            break;
        case Types.DECIMAL:
        case Types.NUMERIC:
       	    if (precision == -1) {
                typeString = NUMERIC_TEMPLATE_WITHOUT_PRECISION;
            } else if (scale == -1) {
                typeString = NUMERIC_TEMPLATE_WITHOUT_SCALE;
            } else {
                typeString = NUMERIC_TEMPLATE;
            }
            break;
        case Types.TIMESTAMP:
            typeString = TIMESTAMP_TEMPLATE;
            break;
        case Types.TIME:
            typeString = TIME_TEMPLATE;
            break;
        case Types.DATE:
            typeString = DATE_TEMPLATE;
            break;
        case Types.BOOLEAN:
            typeString = BOOLEAN_TEMPLATE;
            break;
        case Types.VARCHAR:
                typeString = VARCHAR_TEMPLATE;
            break;
        case Types.CHAR:
            typeString = CHAR_TEMPLATE;
            break;
        case Types.CLOB:
            typeString = TEXT_TEMPLATE;
            break;
        case Types.BLOB:
            typeString = BLOB_TEMPLATE;
            break;
        case Types.NULL:
            typeString = "NULL";
            break;
        case ExpressionType.MACADDR_TYPE:
            typeString = MACADDR_TEMPLATE;
            break;
        case ExpressionType.CIDR_TYPE:
            typeString = CIDR_TEMPLATE;
            break;
        case ExpressionType.INET_TYPE:
            typeString = INET_TEMPLATE;
            break;
        case ExpressionType.GEOMETRY_TYPE:
            typeString = GEOMETRY_TEMPLATE;
            break;
        case ExpressionType.BOX2D_TYPE:
            typeString = BOX2D_TEMPLATE;
            break;
        case ExpressionType.BOX3D_TYPE:
            typeString = BOX3D_TEMPLATE;
            break;
        case ExpressionType.BOX3DEXTENT_TYPE:
            typeString = BOX3DEXTENT_TEMPLATE;
            break;
        case ExpressionType.REGCLASS_TYPE:
            typeString = REGCLASS_TEMPLATE;
            break;
        case ExpressionType.INTERVAL_TYPE:
            if (precision > -1 && scale > -1) {
                typeString = INTERVAL_TEMPLATE_QUALIFIED;
                map.put("from", INTERVAL_QUALIFIERS[precision]);
                map.put("to", INTERVAL_QUALIFIERS[scale]);
            } else {
                typeString = INTERVAL_TEMPLATE;
            }
            break;
        }
        return ParseCmdLine.substitute(typeString, map);
    }

    /**
     * @return Returns the unsigned.
     */
    public boolean isUnsigned() {
        return unsigned;
    }

    /**
     * @return Returns the zerofill.
     */
    public boolean isZerofill() {
        return zerofill;
    }

    /**
     * This function determines if the specific data type can act as the table partition key.
     *
     * @return A boolean value of true if the data type can participate in a partition key
     * otherwise false.
     */
    public boolean canBePartitioningKey() {
        if (Props.XDB_ALLOW_PARTITION_INTEGER
                && (sqlType == Types.INTEGER || sqlType == Types.BIGINT
                        || sqlType == Types.SMALLINT || sqlType == Types.TINYINT)) {
            return true;
        }
        if (Props.XDB_ALLOW_PARTITION_CHAR
                && (sqlType == Types.CHAR || sqlType == Types.VARCHAR)) {
            return true;
        }
        if (Props.XDB_ALLOW_PARTITION_DECIMAL
                && (sqlType == Types.DECIMAL || sqlType == Types.NUMERIC)) {
            return true;
        }
        if (Props.XDB_ALLOW_PARTITION_FLOAT
                && (sqlType == Types.FLOAT || sqlType == Types.REAL || sqlType == Types.DOUBLE)) {
            return true;
        }
        if (Props.XDB_ALLOW_PARTITION_DATETIME
                && (sqlType == Types.DATE || sqlType == Types.TIME || sqlType == Types.TIMESTAMP)) {
            return true;
        }
        if (Props.XDB_ALLOW_PARTITION_MACADDR
                && sqlType == ExpressionType.MACADDR_TYPE) {
            return true;
        }
        if (Props.XDB_ALLOW_PARTITION_INET
                && (sqlType == ExpressionType.INET_TYPE || sqlType == ExpressionType.CIDR_TYPE)) {
            return true;
        }
        return false;
    }
}
