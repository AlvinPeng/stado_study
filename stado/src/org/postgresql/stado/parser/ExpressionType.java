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
 * ExpressionType.java
 *
 */

package org.postgresql.stado.parser;

import java.io.Serializable;
import java.util.HashMap;

import org.postgresql.stado.common.util.ParseCmdLine;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.InvalidExpressionException;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.parser.handler.TypeConstants;


/**
 * Expression Type is an information class which holds the information about the
 * type of a particular SQLExpression.
 *
 */
public class ExpressionType implements Serializable {

    /**
	 * 
	 */
	private static final long serialVersionUID = -8936880257943037513L;

	public static final int VARCHAR_MAX = 8000;

    public static final int VARCHAR_UNICODE_MAX = 4000;

    public static final int VERSION = 2000;

    public static final int INTLEN = 22;

    public static final int BIGINTLEN = 40;

    public static final int DATELEN = 8;

    public static final int TIMELEN = 8;

    public static final int TIMESTAMPLEN = 26;

    public static final int MONTHORDAYNAME = 20;

    public static final int MAXCHARLEN = 255;

    public boolean isWithTimeZone;

    public int type;

    public int length = -1;

    public int scale = -1;

    public int precision = -1;

    // Type definitions for thoes not supported by java
    public static final int BLOB_TYPE = java.sql.Types.BLOB;

    public static final int BOOLEAN_TYPE = java.sql.Types.BOOLEAN;

    public static final int CLOB_TYPE = java.sql.Types.CLOB;

    public static final int DOUBLEPRECISION_TYPE = java.sql.Types.DOUBLE;

    //
    // public static final int NCHAR_TYPE =115 ;
    // public static final int NVCHAR_TYPE =116 ;
    // public static final int NCHARLOB_TYPE =117 ;
    // public static final int SERIAL_TYPE =118 ;

    public static final int MACADDR_TYPE = 119;

    public static final int CIDR_TYPE = 120;

    public static final int INET_TYPE = 121;

    public static final int INTERVAL_TYPE = 122;

    public static final int GEOMETRY_TYPE = 123;

    public static final int BOX2D_TYPE = 124;
    
    public static final int BOX3D_TYPE = 125;
    
    public static final int BOX3DEXTENT_TYPE = 126;

    public static final int REGCLASS_TYPE = 127;
    
    // Supported by Java
    public static final int BIT_TYPE = java.sql.Types.BIT;

    public static final int BINARY_TYPE = java.sql.Types.BINARY;

    public static final int VARBINARY_TYPE = java.sql.Types.VARBINARY;

    public static final int CHAR_TYPE = java.sql.Types.CHAR;

    public static final int VARCHAR_TYPE = java.sql.Types.VARCHAR;

    public static final int SMALLINT_TYPE = java.sql.Types.SMALLINT;

    public static final int INT_TYPE = java.sql.Types.INTEGER;

    public static final int BIGINT_TYPE = java.sql.Types.BIGINT;

    public static final int NUMERIC_TYPE = java.sql.Types.NUMERIC;

    public static final int REAL_TYPE = java.sql.Types.REAL;

    public static final int FLOAT_TYPE = java.sql.Types.FLOAT;

    public static final int DATE_TYPE = java.sql.Types.DATE;

    public static final int TIME_TYPE = java.sql.Types.TIME;

    public static final int DECIMAL_TYPE = java.sql.Types.DECIMAL;

    public static final int TIMESTAMP_TYPE = java.sql.Types.TIMESTAMP;

    public static final int NULL_TYPE = java.sql.Types.NULL;

    public static final int LEN_SERIAL = 10;

    /*
     * Creates a new instance of Expression Type
     */
    public ExpressionType() {

    }

    public ExpressionType(int javaType) {
        type = javaType;
    }

    public ExpressionType(int javaType, int length, int precision, int scale) {
        type = javaType;
        this.length = length;
        this.precision = precision;
        this.scale = scale;
    }

    public ExpressionType(SysColumn column) {
        type = column.getColType();
        length = column.getColLength();
        precision = column.getColPrecision();
        scale = column.getColScale();
    }

    // -------------------------------------------------------------------
    /**
     * Determines whether or not the value passed in is numeric.
     *
     * @param java.sql.types code value
     *
     * @return <code> true</code> returns true if the expression Type is
     *         numeric
     * @throws IllegalArgumentException :
     *             Throws an illegalArgumentException if the type is not a valid
     *             type.
     */
    public static boolean isNumeric(int typeCode) {
        switch (typeCode) {
        case BLOB_TYPE:
            return false;
        case BOOLEAN_TYPE:
            return false;
        case BIT_TYPE:
        case BINARY_TYPE:
            return false;
        case CHAR_TYPE:
            return false;
        case VARCHAR_TYPE:
            return false;
        case DATE_TYPE:
            return false;
        case TIME_TYPE:
            return false;
        case TIMESTAMP_TYPE:
            return false;
        case CLOB_TYPE:
            return false;
        case SMALLINT_TYPE:
            return true;
        case INT_TYPE:
            return true;
        case NUMERIC_TYPE:
            return true;
        case REAL_TYPE:
            return true;
        case FLOAT_TYPE:
            return true;

        case DECIMAL_TYPE:
            return true;

        case LEN_SERIAL:
            return true;
        case BIGINT_TYPE:
            return true;

        case DOUBLEPRECISION_TYPE:
            return true;
        case NULL_TYPE:
            return true;
        case ExpressionType.MACADDR_TYPE:
                return false;
        case ExpressionType.CIDR_TYPE:
                return false;
        case ExpressionType.INET_TYPE:
                return false;
        case ExpressionType.INTERVAL_TYPE:
            return false;
		case ExpressionType.GEOMETRY_TYPE:
			return false;
		case ExpressionType.BOX2D_TYPE:
			return false;
		case ExpressionType.BOX3D_TYPE:
			return false;
		case ExpressionType.BOX3DEXTENT_TYPE:
			return false;
		case ExpressionType.REGCLASS_TYPE:
			return false;
        default:
            throw new XDBServerException(
                    ErrorMessageRepository.INVALID_DATATYPE + "( " + typeCode
                            + " )", 0,
                    ErrorMessageRepository.INVALID_DATATYPE_CODE);
        }
    }

    /**
     *
     * @return <code> true</code> returns true if the expression Type is
     *         numeric
     * @throws IllegalArgumentException :
     *             Throws an illegalArgumentException if the type is not a valid
     *             type.
     */
    public boolean isNumeric() {
        return isNumeric(this.type);
    }

    // -------------------------------------------------------------------
    /**
     * Determines whether or not the value passed in is InExactNumeric
     * (float, real, double)
     *
     * @param java.sql.types code value
     *
     * @return <code> true</code> returns true if the expression Type is
     *         InExactNumeric
     */
    public static boolean isInExactNumeric(int typeCode) {
        if (typeCode == REAL_TYPE || typeCode == FLOAT_TYPE
                || typeCode == DOUBLEPRECISION_TYPE) {
            return true;
        } else {
            return false;
        }
    }

    /**
     *
     * @return <code> true</code> returns true if the expression Type is
     *         InExactNumeric (real, float, double)
     */
    public boolean isInExactNumeric() {
        return isInExactNumeric(this.type);
    }

    /**
     *
     * @return <code> true</code> returns true if the expression Type is
     *         bit
     * @throws IllegalArgumentException :
     *             Throws an illegalArgumentException if the type is not a valid
     *             type.
     */

    public boolean isBit() {
        switch (this.type) {
        case BOOLEAN_TYPE:
        case BIT_TYPE:
        case BINARY_TYPE:
        case NULL_TYPE:
            return true;
        default:
            return false;
        }
    }

    /**
     *
     * @return <code> true</code> returns true if the expression Type is
     *         varchar
     * @throws IllegalArgumentException :
     *             Throws an illegalArgumentException if the type is not a valid
     *             type.
     */

    public boolean isCharacter() {
        switch (this.type) {
        case VARCHAR_TYPE:
            return true;
        default:
            return false;
        }
    }

    // -----------------------------------------------------------------------
    /**
     *
     * @return A string which is the String representaion of this particular
     *         expression
     * @throws IllegalArgumentException :
     *             An exception is thrown when the Type is set to a unknown data
     *             type
     */
    public String getTypeString() {
        String typeString = "";

        HashMap<String, String> params = new HashMap<String, String>();
        params.put("length", new String(length + ""));
        params.put("scale", new String(scale + ""));
        params.put("precision", new String(precision + ""));

        switch (type) {
        case java.sql.Types.BIT:
            if (length > 1) {
                typeString = TypeConstants.BIT_TEMPLATE;
            } else {
                typeString = "BIT";
            }

            typeString = ParseCmdLine.substitute(typeString, params);
            break;
        case java.sql.Types.BINARY:
            if (length > 1) {
                typeString = TypeConstants.BIT_TEMPLATE;
            } else {
                typeString = "BIT";
            }

            typeString = ParseCmdLine.substitute(typeString, params);
            break;
        case java.sql.Types.VARBINARY:
            typeString = "VARBIT";
            break;

        case java.sql.Types.CHAR:
            typeString = TypeConstants.CHAR_TEMPLATE;
            typeString = ParseCmdLine.substitute(typeString, params);
            break;

        case java.sql.Types.VARCHAR:
            typeString = TypeConstants.VARCHAR_TEMPLATE;
            typeString = ParseCmdLine.substitute(typeString, params);
            break;

        case java.sql.Types.SMALLINT:
            typeString = TypeConstants.SMALLINT_TEMPLATE;
            typeString = ParseCmdLine.substitute(typeString, params);
            break;

        case java.sql.Types.INTEGER:
            typeString = TypeConstants.INTEGER_TEMPLATE;
            typeString = ParseCmdLine.substitute(typeString, params);
            break;

        case java.sql.Types.BIGINT:
            typeString = TypeConstants.BIGINTEGER_TEMPLATE;
            typeString = ParseCmdLine.substitute(typeString, params);
            break;

        case java.sql.Types.DECIMAL:
        case java.sql.Types.NUMERIC:
            if (precision == -1) {
                typeString = TypeConstants.NUMERIC_TEMPLATE_WITHOUT_PRECISION;
            } else if (scale == -1) {
                typeString = TypeConstants.NUMERIC_TEMPLATE_WITHOUT_SCALE;
            } else {
                typeString = TypeConstants.NUMERIC_TEMPLATE;
            }
            typeString = ParseCmdLine.substitute(typeString, params);
            break;


        case java.sql.Types.REAL:
            typeString = TypeConstants.REAL_TEMPLATE;
            break;

        case java.sql.Types.FLOAT:
            if (length > 0) {
                typeString = TypeConstants.FLOAT_TEMPLATE;
                typeString = ParseCmdLine.substitute(typeString, params);
            } else {
                typeString = TypeConstants.REAL_TEMPLATE;
            }
            break;

        case java.sql.Types.DATE:
            typeString = TypeConstants.DATE_TEMPLATE;
            typeString = ParseCmdLine.substitute(typeString, params);
            break;

        case java.sql.Types.TIME:
            typeString = TypeConstants.TIME_TEMPLATE;
            typeString = ParseCmdLine.substitute(typeString, params);
            break;

        case java.sql.Types.TIMESTAMP:
            typeString = TypeConstants.TIMESTAMP_TEMPLATE;
            typeString = ParseCmdLine.substitute(typeString, params);
            break;
        case java.sql.Types.DOUBLE:
            typeString = TypeConstants.DOUBLE_TEMPLATE;
            typeString = ParseCmdLine.substitute(typeString, params);
            break;
        case java.sql.Types.BOOLEAN:
            typeString = TypeConstants.BOOLEAN_TEMPLATE;
            typeString = ParseCmdLine.substitute(typeString, params);
            break;
        case java.sql.Types.CLOB:
            typeString = TypeConstants.TEXT_TEMPLATE;
            typeString = ParseCmdLine.substitute(typeString, params);
            break;
        case java.sql.Types.BLOB:
            typeString = TypeConstants.BLOB_TEMPLATE;
            typeString = ParseCmdLine.substitute(typeString, params);
            break;
        case java.sql.Types.NULL:
            // for null type we dont return any string
            break;
        case ExpressionType.MACADDR_TYPE:
            typeString = "MACADDR ";
            break;
        case ExpressionType.CIDR_TYPE:
            typeString = "CIDR ";
            break;
        case ExpressionType.INET_TYPE:
            typeString = "INET ";
            break;
		case ExpressionType.GEOMETRY_TYPE:
			typeString = "GEOMETRY ";
			break;
		case ExpressionType.BOX2D_TYPE:
			typeString = "BOX2D ";
			break;
		case ExpressionType.BOX3D_TYPE:
			typeString = "BOX3D ";
			break;
		case ExpressionType.BOX3DEXTENT_TYPE:
			typeString = "BOX3D_EXTENT ";
			break;
		case ExpressionType.REGCLASS_TYPE:
			typeString = "REGCLASS ";
			break;
		case ExpressionType.INTERVAL_TYPE:
	        typeString = "INTERVAL ";
	        break;
        default:
            throw new XDBServerException(
                    ErrorMessageRepository.INVALID_DATATYPE + "( " + type
                            + " )", 0,
                    ErrorMessageRepository.INVALID_DATATYPE_CODE);
        }

        return typeString;

    }

    /**
     *
     */

    public String getLengthString() {
        if (length > 0) {
            return "( " + length + " )";
        } else {
            return "";
        }
    }

    // -------------------------------------------------------------------
    /***************************************************************************
     * The function allows setting of all the variables that completely define
     * an expression. Pre Condition: None
     *
     * @param type
     *            This defines the data type of the expression, and the value is
     *            same as that of java.sql.Types
     * @param length
     *            This defines the length of the data
     * @param precision
     *            The total length of the datatype - only makes sense if the
     *            datatype is numeric, decimal.
     * @param scale
     *
     */

    public void setExpressionType(int type, int length, int precision, int scale) {
        this.type = type;
        this.length = length;
        this.precision = precision;
        this.scale = scale;
    }

    /***************************************************************************
     * PreCondition: The expressions should be compatible with each other,
     * incase it is not a illegal argument exception is thrown.
     *
     *
     * @param aExprTypeHelper
     *            A helper class which has 3 fields. ( 2 Exprtype , 1 Operator )
     *            Allows easy ineraction between functions
     * @return Just returns the expression type after checking the semantics of
     *         the expression.eg. if boolean and clob columns exist in the same
     *         expression then it will not accept it.
     *
     * The Function determines the new expression that will be returned when two
     * expressions are joined with the specified operator.
     */

    public ExpressionType GetExpressionType(ExprTypeHelper aExprTypeHelper) {

        ExpressionType exprType = checkSemanticsAndReturnOutputType(aExprTypeHelper);
        return exprType;
    }

    /***************************************************************************
     *
     * @param aExprTypeHelper
     * @return A expression type , after combining the different data types
     *
     * The function not only checks if these two expressions can be joined by
     * the given operator but also return the appropriate output expression
     * type.
     */
    private ExpressionType checkSemanticsAndReturnOutputType(
            ExprTypeHelper aExprTypeHelper) {
        ExpressionType exprTypeToReturn = null;
        int leftType = aExprTypeHelper.leftExprType.type;
        int rightType = aExprTypeHelper.righExprType.type;

        exprTypeToReturn = HandleOperator(aExprTypeHelper.Operator);
        if (exprTypeToReturn != null) {
            return exprTypeToReturn;
        }

        // Determine if any of the expressions is a BIT TYPE
        if (leftType == ExpressionType.INTERVAL_TYPE
                || rightType == ExpressionType.INTERVAL_TYPE) {
            exprTypeToReturn = HandleInterval(aExprTypeHelper);
            return exprTypeToReturn;
        }

        if (leftType == ExpressionType.CIDR_TYPE
                || rightType == ExpressionType.CIDR_TYPE) {
            exprTypeToReturn = HandleCidr(aExprTypeHelper);
            return exprTypeToReturn;
        }
        if (leftType == ExpressionType.INET_TYPE
                || rightType == ExpressionType.INET_TYPE) {
            exprTypeToReturn = HandleInet(aExprTypeHelper);
            return exprTypeToReturn;
        }
        if (leftType == ExpressionType.GEOMETRY_TYPE
                || rightType == ExpressionType.GEOMETRY_TYPE) {
            exprTypeToReturn = HandleGeometry(aExprTypeHelper);
            return exprTypeToReturn;
        }        
        if (leftType == ExpressionType.BOX2D_TYPE
                || rightType == ExpressionType.BOX2D_TYPE) {
            exprTypeToReturn = HandleBox2D(aExprTypeHelper);
            return exprTypeToReturn;
        }        
        if (leftType == ExpressionType.BOX3D_TYPE
                || rightType == ExpressionType.BOX3D_TYPE) {
            exprTypeToReturn = HandleBox3D(aExprTypeHelper);
            return exprTypeToReturn;
        }        
        if (leftType == ExpressionType.BOX3DEXTENT_TYPE
                || rightType == ExpressionType.BOX3DEXTENT_TYPE) {
            exprTypeToReturn = HandleBox3DExtent(aExprTypeHelper);
            return exprTypeToReturn;
        }        
        if (leftType == ExpressionType.TIME_TYPE
                || rightType == ExpressionType.TIME_TYPE) {

            exprTypeToReturn = HandleTimeType(aExprTypeHelper);
        }
        if (leftType == ExpressionType.CLOB_TYPE
                || rightType == ExpressionType.CLOB_TYPE) {
            exprTypeToReturn = HandleClobType(aExprTypeHelper);

        } else if (leftType == ExpressionType.BLOB_TYPE
                || rightType == ExpressionType.BLOB_TYPE) {
            exprTypeToReturn = HandleBlobType(aExprTypeHelper);
        } else if (leftType == ExpressionType.BINARY_TYPE
                || rightType == ExpressionType.BINARY_TYPE) {
            exprTypeToReturn = HandleBinaryType(aExprTypeHelper);
        } else if (leftType == ExpressionType.BIT_TYPE
                || rightType == ExpressionType.BIT_TYPE) {
            // Allow the bit type Handler to take care of it -- TODO
            // Specifications
            // for BIT type handling are not availabe .
            exprTypeToReturn = HandleBitType(aExprTypeHelper);
        }
        // The boolean type is handled the same way as BIT TYPE
        else if (leftType == ExpressionType.BOOLEAN_TYPE
                || rightType == ExpressionType.BOOLEAN_TYPE) {
            //
            exprTypeToReturn = HandleBooleanType(aExprTypeHelper);

        } else if (leftType == ExpressionType.DOUBLEPRECISION_TYPE
                || rightType == ExpressionType.DOUBLEPRECISION_TYPE) {
            exprTypeToReturn = HandleDoublePrecison(aExprTypeHelper);
        } else if (leftType == ExpressionType.CHAR_TYPE
                || rightType == ExpressionType.CHAR_TYPE) {
            exprTypeToReturn = HandleCharType(aExprTypeHelper);

        } else if (leftType == ExpressionType.VARCHAR_TYPE
                || rightType == ExpressionType.VARCHAR_TYPE) {
            exprTypeToReturn = HandleVarCharType(aExprTypeHelper);

        } else if (leftType == ExpressionType.SMALLINT_TYPE
                || rightType == ExpressionType.SMALLINT_TYPE) {
            exprTypeToReturn = HandleSmallInt(aExprTypeHelper);

        } else if (leftType == ExpressionType.INT_TYPE
                || rightType == ExpressionType.INT_TYPE) {
            exprTypeToReturn = HandleInt(aExprTypeHelper);

        } else if (leftType == ExpressionType.BIGINT_TYPE
                || rightType == ExpressionType.BIGINT_TYPE) {
            exprTypeToReturn = HandleBigInt(aExprTypeHelper);

        } else if (leftType == ExpressionType.NUMERIC_TYPE
                || rightType == ExpressionType.NUMERIC_TYPE) {
            exprTypeToReturn = HandleNumeric(aExprTypeHelper);
        } else if (leftType == ExpressionType.REAL_TYPE
                || rightType == ExpressionType.REAL_TYPE) {
            exprTypeToReturn = HandleReal(aExprTypeHelper);

        } else if (leftType == ExpressionType.FLOAT_TYPE
                || rightType == ExpressionType.FLOAT_TYPE) {
            exprTypeToReturn = HandleFloat(aExprTypeHelper);

        } else if (leftType == ExpressionType.DATE_TYPE
                || rightType == ExpressionType.DATE_TYPE) {
            exprTypeToReturn = HandleDate(aExprTypeHelper);

        } else if (leftType == ExpressionType.DECIMAL_TYPE
                || rightType == ExpressionType.DECIMAL_TYPE) {
            exprTypeToReturn = HandleDecimal(aExprTypeHelper);
        } else if (leftType == ExpressionType.TIMESTAMP_TYPE
                || rightType == ExpressionType.TIMESTAMP_TYPE) {
            exprTypeToReturn = HandleTimeStamp(aExprTypeHelper);
        }

        if (exprTypeToReturn == null) {
            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);
        } else {
            return exprTypeToReturn;
        }

    }

    /* At present we don't have any specification for it -- */
    /**
     * This function takes care of combining BIT type with other datatypes and
     * returns a vaild expression type.
     *
     * @param aExprTypeHelper -
     *            This is a helper object which contains 3 items 2 expression
     *            and 1 Operator.
     * @return The expression type which will be a result after combining these
     *         two expression
     */
    private ExpressionType HandleBitType(ExprTypeHelper aExprTypeHelper) {
        ExpressionType exprTypeToReturn = null;
        int leftType = aExprTypeHelper.leftExprType.type;
        int rightType = aExprTypeHelper.righExprType.type;

        if (leftType == ExpressionType.BOOLEAN_TYPE
                || rightType == ExpressionType.BOOLEAN_TYPE) {

        } else if (leftType == ExpressionType.BLOB_TYPE
                || rightType == ExpressionType.BLOB_TYPE) {

        } else if (leftType == ExpressionType.BOOLEAN_TYPE
                || rightType == ExpressionType.BOOLEAN_TYPE) {

        } else if (leftType == ExpressionType.CLOB_TYPE
                || rightType == ExpressionType.CLOB_TYPE) {

        } else if (leftType == ExpressionType.DOUBLEPRECISION_TYPE
                || rightType == ExpressionType.DOUBLEPRECISION_TYPE) {

        } else if (leftType == ExpressionType.CHAR_TYPE
                || rightType == ExpressionType.CHAR_TYPE) {

        } else if (leftType == ExpressionType.VARCHAR_TYPE
                || rightType == ExpressionType.VARCHAR_TYPE) {

        } else if (leftType == ExpressionType.SMALLINT_TYPE
                || rightType == ExpressionType.SMALLINT_TYPE) {

        } else if (leftType == ExpressionType.INT_TYPE
                || rightType == ExpressionType.INT_TYPE) {

        } else if (leftType == ExpressionType.NUMERIC_TYPE
                || rightType == ExpressionType.NUMERIC_TYPE) {

        } else if (leftType == ExpressionType.REAL_TYPE
                || rightType == ExpressionType.REAL_TYPE) {

        } else if (leftType == ExpressionType.FLOAT_TYPE
                || rightType == ExpressionType.FLOAT_TYPE) {

        } else if (leftType == ExpressionType.DATE_TYPE
                || rightType == ExpressionType.DATE_TYPE) {

        } else if (leftType == ExpressionType.DECIMAL_TYPE
                || rightType == ExpressionType.DECIMAL_TYPE) {

        } else if (leftType == ExpressionType.TIMESTAMP_TYPE
                || rightType == ExpressionType.TIMESTAMP_TYPE) {

        }
        return exprTypeToReturn;
    }

    /* At present we don't have any specification for it -- */
    /**
     * This function takes care of combining BIT type with other datatypes and
     * returns a vaild expression type.
     *
     * @param aExprTypeHelper -
     *            This is a helper object which contains 3 items 2 expression
     *            and 1 Operator.
     * @return The expression type which will be a result after combining these
     *         two expression
     */
    private ExpressionType HandleBinaryType(ExprTypeHelper aExprTypeHelper) {
        ExpressionType exprTypeToReturn = null;
        int otherType;
        if (aExprTypeHelper.leftExprType.type == ExpressionType.BINARY_TYPE) {
        	otherType = aExprTypeHelper.righExprType.type;
        } else {
        	otherType = aExprTypeHelper.leftExprType.type;
        }

        if (otherType == ExpressionType.BINARY_TYPE
                || otherType == ExpressionType.CHAR_TYPE
        		|| otherType == ExpressionType.VARCHAR_TYPE) {
        	int leftLen = aExprTypeHelper.leftExprType.length;
        	int rightLen = aExprTypeHelper.righExprType.length;
        	// Rely on backend check if length can not be determined 
        	if (leftLen == -1 || rightLen == -1) {
        		leftLen = -1;
        		rightLen = -1;
        	}
        	if (leftLen == rightLen) {
        		exprTypeToReturn = new ExpressionType();
        		exprTypeToReturn.setExpressionType(ExpressionType.BINARY_TYPE, leftLen, 0, 0);
        	} 
        }
        return exprTypeToReturn;
    }

    // -- Handling Blob Type Start
    /**
     * This function takes care of combining BLOB type with other datatypes and
     * returns a vaild expression type - Since we cannot have any expressions
     * with blobs we throw a IllegalArgumentException.
     *
     * @param aExprTypeHelper -
     *            This is a helper object which contains 3 items 2 expression
     *            and 1 Operator.
     * @return The expression type which will be a result after combining these
     *         two expression
     */
    private ExpressionType HandleBlobType(ExprTypeHelper aExprTypeHelper) {
        ExpressionType exprTypeToReturn = new ExpressionType();

        int leftType = aExprTypeHelper.leftExprType.type;
        int rightType = aExprTypeHelper.righExprType.type;

        if (leftType == ExpressionType.BLOB_TYPE
                || rightType == ExpressionType.BLOB_TYPE
                || leftType == ExpressionType.NULL_TYPE
                || rightType == ExpressionType.NULL_TYPE) {
            exprTypeToReturn.setExpressionType(ExpressionType.BLOB_TYPE, -1,
                    -1, -1);

        } else if (leftType == ExpressionType.BOOLEAN_TYPE
                || rightType == ExpressionType.BOOLEAN_TYPE) {

            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);

            // throw new XDBSemanticException("Boolean variable and a Blob
            // cannot be used with binary operator :" + Operator);

        } else if (leftType == ExpressionType.CLOB_TYPE
                || rightType == ExpressionType.CLOB_TYPE) {
            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);

            // throw new XDBSemanticException(" variable and a Blob cannot be
            // used with binary operator :" + Operator);

        } else if (leftType == ExpressionType.DOUBLEPRECISION_TYPE
                || rightType == ExpressionType.DOUBLEPRECISION_TYPE) {

            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);

            // hrow new XDBSemanticException("Double and a Blob cannot be used
            // with binary operator :" + Operator);

        } else if (leftType == ExpressionType.CHAR_TYPE
                || rightType == ExpressionType.CHAR_TYPE) {
            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);

            // throw new XDBSemanticException("Char and a Blob cannot be used
            // with binary operator :" + Operator);
        } else if (leftType == ExpressionType.VARCHAR_TYPE
                || rightType == ExpressionType.VARCHAR_TYPE) {
            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);

            // throw new XDBSemanticException("Var Char and a Blob cannot be
            // used with binary operator :" + Operator);

        } else if (leftType == ExpressionType.SMALLINT_TYPE
                || rightType == ExpressionType.SMALLINT_TYPE) {
            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);

            // throw new XDBSemanticException("Small Int and a Blob cannot be
            // used with binary operator :" + Operator);

        } else if (leftType == ExpressionType.INT_TYPE
                || rightType == ExpressionType.INT_TYPE) {
            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);

        } else if (leftType == ExpressionType.BIGINT_TYPE
                || rightType == ExpressionType.BIGINT_TYPE) {
            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);

            // throw new XDBSemanticException("INT and a Blob cannot be used
            // with binary operator :" + Operator);
        } else if (leftType == ExpressionType.NUMERIC_TYPE
                || rightType == ExpressionType.NUMERIC_TYPE) {

            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);

            // throw new XDBSemanticException("INT and a Blob cannot be used
            // with binary operator :" + Operator);

        } else if (leftType == ExpressionType.REAL_TYPE
                || rightType == ExpressionType.REAL_TYPE) {

            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);

            // throw new XDBSemanticException("REAL and a Blob cannot be used
            // with binary operator :" + Operator);

        } else if (leftType == ExpressionType.FLOAT_TYPE
                || rightType == ExpressionType.FLOAT_TYPE) {

            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);

            // throw new XDBSemanticException("FLOAT and a Blob cannot be used
            // with binary operator :" + Operator);

        } else if (leftType == ExpressionType.DATE_TYPE
                || rightType == ExpressionType.DATE_TYPE) {
            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);

            // throw new XDBSemanticException("DATE and a Blob cannot be used
            // with binary operator :" + Operator);

        } else if (leftType == ExpressionType.DECIMAL_TYPE
                || rightType == ExpressionType.DECIMAL_TYPE) {

            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);

            // throw new XDBSemanticException("DECIMAL and a Blob cannot be used
            // with binary operator :" + Operator);

        } else if (leftType == ExpressionType.TIMESTAMP_TYPE
                || rightType == ExpressionType.TIMESTAMP_TYPE) {
            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);

            // throw new XDBSemanticException("TIME STAMP and a Blob cannot be
            // used with binary operator :" + Operator);
        }
        return exprTypeToReturn;
    }

    // Handling Bolb type ends

    // Handling Boolean Typ starts
    /**
     * This function takes care of combining BOOLEAN type with other datatypes
     * and returns a vaild expression type - Please noted that we will not
     * handle BIT type with boolean here as it is already taken care of in
     * handleBitType()
     *
     * @param aExprTypeHelper -
     *            This is a helper object which contains 3 items 2 expression
     *            and 1 Operator.
     * @return The expression type which will be a result after combining these
     *         two expression
     */
    private ExpressionType HandleBooleanType(ExprTypeHelper aExprTypeHelper) {
        // Allocate Memory for the Expression to return
        ExpressionType exprTypeToReturn = new ExpressionType();

        int leftType = aExprTypeHelper.leftExprType.type;

        // This will be filled with the NON- BOOLEAN expression
        ExpressionType typeToCheck;

        // Check to see which one is boolean and assign the non boolean to
        // "typeToCheck"
        if (leftType == ExpressionType.BOOLEAN_TYPE) {
            typeToCheck = aExprTypeHelper.righExprType;
        } else {
            typeToCheck = aExprTypeHelper.leftExprType;
        }

        // Incase the Other Type is also boolean -- Set the expression type to
        // boolean
        // We donot have any consideration for precision,scale or length

        if (typeToCheck.type == ExpressionType.BOOLEAN_TYPE
                || typeToCheck.type == ExpressionType.NULL_TYPE) {
            exprTypeToReturn.setExpressionType(ExpressionType.BOOLEAN_TYPE, -1,
                    -1, -1);

        }
        // The CLOB datatype cannot interact with Boolean Data Type - There fore
        // we just throw
        // an exception here
        else if (typeToCheck.type == ExpressionType.CLOB_TYPE) {
            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);

            // throw new XDBSemanticException("CLOB and Boolean cannot be used
            // with binary operator :" + Operator);

        }
        // Incase we have BLOB
        else if (typeToCheck.type == ExpressionType.CLOB_TYPE) {
            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);

            // throw new XDBSemanticException("BLOB and Boolean cannot be used
            // with binary operator :" + Operator);

        }

        // Incase of Double we simply return a double with the length and
        // precision of a double.
        // assuming that the boolean variable will be 0 or 1

        // Some DBs do not allow interaction between boolean varibles and other
        // types
        else if (typeToCheck.type == ExpressionType.DOUBLEPRECISION_TYPE) {

            exprTypeToReturn.setExpressionType(
                    ExpressionType.DOUBLEPRECISION_TYPE, typeToCheck.length,
                    typeToCheck.precision, typeToCheck.scale);

        }
        // For Char type we dont think of any valid expression, there fore like
        // NVCHAR,VARCHAR,CHARLOB etc.
        // We throw an exception.
        else if (typeToCheck.type == ExpressionType.CHAR_TYPE) {
            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);

            // throw new XDBSemanticException("Boolean and Char cannot be used
            // with binary operator :" + Operator);

        }
        // For VarChar -- similar treatment as CHAR etc.
        else if (typeToCheck.type == ExpressionType.VARCHAR_TYPE) {
            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);

            // throw new XDBSemanticException("Boolean and VarChar cannot be
            // used with binary operator :" + Operator);
        }
        // SMALL INT is a numeric type there fore we will allow the boolean to
        // take a value of 0 or 1
        // The length , precison and scale are all -1.However a small int is of
        // 6 fields long.
        // However this might not make much change.
        else if (typeToCheck.type == ExpressionType.SMALLINT_TYPE
                || typeToCheck.type == NUMERIC_TYPE
                | typeToCheck.type == REAL_TYPE
                || typeToCheck.type == FLOAT_TYPE
                || typeToCheck.type == DECIMAL_TYPE
                || typeToCheck.type == INT_TYPE
                || typeToCheck.type == BIGINT_TYPE) {
            // Some DBs do not support Boolean
            // interaction with any other type
            // but we still leave this here so that it can complain when we go
            // and hit the database.
            exprTypeToReturn.setExpressionType(typeToCheck.type,
                    typeToCheck.length, typeToCheck.precision,
                    typeToCheck.scale);
        } else if (typeToCheck.type == ExpressionType.DATE_TYPE) {
            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);

            // throw new XDBSemanticException("Boolean and Date cannot be used
            // with binary operator :" + Operator);
        }

        // Incase this is a TIMESTAMP no possible interaction
        else if (typeToCheck.type == ExpressionType.TIMESTAMP_TYPE) {
            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);

            // throw new XDBSemanticException("Boolean and TimeStamp cannot be
            // used with binary operator :" + Operator);
        }
        // Incase this is a TIME
        else if (typeToCheck.type == ExpressionType.TIME_TYPE) {
            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);

            // throw new XDBSemanticException("Boolean and Time cannot be used
            // with binary operator :" + Operator);
        }
        return exprTypeToReturn;
    }

    // Handling Boolean Type Ends

    private ExpressionType HandleCharType(ExprTypeHelper aExprTypeHelper) {
        ExpressionType exprTypeToReturn = new ExpressionType();
        int leftType = aExprTypeHelper.leftExprType.type;
        // int rightType = aExprTypeHelper.righExprType.type;
        String Operator = aExprTypeHelper.Operator;
        ExpressionType typeToCheck;

        ExpressionType aCharType = null;
        if (Operator.equals("||") == false) {
            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);

            // throw new XDBSemanticException("Only Supported operation between
            // a Char and Any other Datatype is concatenation");
        }
        if (leftType == ExpressionType.CHAR_TYPE) {
            typeToCheck = aExprTypeHelper.righExprType;
            aCharType = aExprTypeHelper.leftExprType;
        } else {
            typeToCheck = aExprTypeHelper.leftExprType;
            aCharType = aExprTypeHelper.righExprType;
        }

        if (typeToCheck.type == ExpressionType.CHAR_TYPE
                || typeToCheck.type == ExpressionType.NULL_TYPE) {

            // if both are char type then we should have add the lengths of both
            // left and right
            // The precision and scale do not matter in this case
            exprTypeToReturn.setExpressionType(ExpressionType.CHAR_TYPE,
                    aCharType.length + typeToCheck.length, -1, -1);

        } else if (typeToCheck.type == ExpressionType.VARCHAR_TYPE) {
            exprTypeToReturn.setExpressionType(ExpressionType.VARCHAR_TYPE,
                    aCharType.length + typeToCheck.length, -1, -1);

        } else if (typeToCheck.type == ExpressionType.SMALLINT_TYPE) {
            // The default size of
            ConvertSmallIntToCharType(exprTypeToReturn, aCharType);
        } else if (typeToCheck.type == INT_TYPE) {
            // The default size of
            ConvertIntToCharType(exprTypeToReturn, aCharType);
        } else if (typeToCheck.type == BIGINT_TYPE) {
            // The default size of
            ConvertBigIntToCharType(exprTypeToReturn, aCharType);
        } else if (typeToCheck.type == ExpressionType.REAL_TYPE
                || typeToCheck.type == ExpressionType.FLOAT_TYPE) {
            ConvertRealToChar(typeToCheck, exprTypeToReturn, aCharType);

        } else if (typeToCheck.type == ExpressionType.DATE_TYPE) {
            // The date lenght is less than 3o for sure - I am not sure how
            // long it can be.
            exprTypeToReturn.setExpressionType(ExpressionType.CHAR_TYPE,
                    aCharType.length + 30, -1, -1);

        } else if (typeToCheck.type == ExpressionType.DECIMAL_TYPE
                || typeToCheck.type == ExpressionType.NUMERIC_TYPE)

        {
            ConvertNumTypeCharType(typeToCheck, exprTypeToReturn, aCharType);

        } else if (typeToCheck.type == ExpressionType.TIMESTAMP_TYPE) {
            // The Lenght of the time stamp varies in DBs, but to be on the
            // safer side we are putting it at 30
            exprTypeToReturn.setExpressionType(ExpressionType.CHAR_TYPE,
                    aCharType.length + 30, -1, -1);
        }
        return exprTypeToReturn;
    }

    /*
     * This function converts a Real to Char - The real can be specifed as Real ,
     * or Real(10) . where 10 is the length.
     *
     * Therefore in case we just have real - we
     * allow a length of 16 incase it is user specified, that is used.
     *
     * This same thing is valid for VARCHAR,NCHAR,NVCHAR, and CHAR
     */

    /**
     * This is a helper function which changes the expression type to the right
     * value basically it sets the lenght to 16 + the lenght already specified.
     *
     * @param typeToCheck
     * @param exprTypeToReturn
     * @param aCharType
     */
    private void ConvertRealToChar(ExpressionType typeToCheck,
            ExpressionType exprTypeToReturn, ExpressionType aCharType) {
        // The length of a real is 16. Incase the user has specified the lenght
        // we can use that length
        if (typeToCheck.length <= 0) {
            exprTypeToReturn.setExpressionType(aCharType.type,
                    aCharType.length + 16, -1, -1);
        } else {
            exprTypeToReturn.setExpressionType(aCharType.type, aCharType.length
                    + typeToCheck.length, -1, -1);
        }
    }

    /*
     * This function converts a Numeric Type to a character Type -- according to
     * some DBs, a Numeric or decimal type with out any specification is mapped
     * to FIXED(5) When ever there is specification , the specs is used to
     * create the column.
     *
     * Therefore we have used char.length + 5 in the default case
     *
     * and
     *
     * char.length + typeToCheck.length in the case when the precision and scale
     * are specified.
     *
     */
    /**
     *
     * @param typeToCheck
     * @param exprTypeToReturn
     * @param aCharType
     */
    private void ConvertNumTypeCharType(ExpressionType typeToCheck,
            ExpressionType exprTypeToReturn, ExpressionType aCharType) {
        // Incase the user does not specify any value for
        // decimal -- some DBs use length 5 as default

        if (typeToCheck.length <= 0) {
            exprTypeToReturn.setExpressionType(aCharType.type,
                    aCharType.length + 5, -1, -1);
        } else {
            exprTypeToReturn.setExpressionType(aCharType.type, aCharType.length
                    + typeToCheck.length, -1, -1);
        }
    }

    // Handle Char ends

    // Handle Var Char Starts
    /**
     * This funtion handles VarChar Combining with other datatypes
     *
     * @param aExprTypeHelper
     * @return
     */
    private ExpressionType HandleVarCharType(ExprTypeHelper aExprTypeHelper) {

        ExpressionType exprTypeToReturn = new ExpressionType();
        int leftType = aExprTypeHelper.leftExprType.type;
        // int rightType = aExprTypeHelper.righExprType.type;
        String Operator = aExprTypeHelper.Operator;
        ExpressionType typeToCheck = null;

        ExpressionType aVarCharType = null;

        // Check if we are comparing strings
        if (Operator.equals(">") ||
                Operator.equals(">=") ||
                Operator.equals("<") ||
                Operator.equals("<=") ||
                Operator.equals("!=") ||
                Operator.equals("<>") ||
                Operator.equals("=") ||
                Operator.equals("is") ||
                Operator.equals("is not") ||
                Operator.equals("~") ||
                Operator.equals("!~") ||
                Operator.equals("~*") ||
                Operator.equals("!~*"))
                {
        // return BOOLEAN 
        	exprTypeToReturn.setExpressionType(ExpressionType.BOOLEAN_TYPE,
                -1, -1, -1);
            return exprTypeToReturn;
        }

        if (leftType == ExpressionType.CHAR_TYPE) {
            typeToCheck = aExprTypeHelper.righExprType;
            aVarCharType = aExprTypeHelper.leftExprType;
        } else {
            typeToCheck = aExprTypeHelper.leftExprType;
            aVarCharType = aExprTypeHelper.righExprType;
        }

        if (Operator.equals("||") == false) {
            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);

            // throw new XDBSemanticException("Only Supported operation between
            // a Char and Any other Datatype is concatenation");
        }

        if (typeToCheck.type == ExpressionType.VARCHAR_TYPE
                || typeToCheck.type == ExpressionType.NULL_TYPE) {
            // If both are var char type we can add the lengths for both these
            // variables
            exprTypeToReturn.setExpressionType(ExpressionType.VARCHAR_TYPE,
                    aVarCharType.length + typeToCheck.length, -1, -1);

        } else if (typeToCheck.type == ExpressionType.SMALLINT_TYPE) {
            // The default size of
            ConvertSmallIntToCharType(exprTypeToReturn, aVarCharType);
        } else if (typeToCheck.type == INT_TYPE) {
            // The default size of
            ConvertIntToCharType(exprTypeToReturn, aVarCharType);
        } else if (typeToCheck.type == BIGINT_TYPE) {
            // The default size of
            ConvertBigIntToCharType(exprTypeToReturn, aVarCharType);

        } else if (typeToCheck.type == ExpressionType.NUMERIC_TYPE
                || typeToCheck.type == ExpressionType.DECIMAL_TYPE) {
            ConvertNumTypeCharType(typeToCheck, exprTypeToReturn, aVarCharType);

        } else if (typeToCheck.type == ExpressionType.REAL_TYPE
                || typeToCheck.type == FLOAT_TYPE) {
            // Converting real type to Var Char type
            ConvertRealToChar(typeToCheck, exprTypeToReturn, aVarCharType);
        } else if (typeToCheck.type == ExpressionType.DATE_TYPE) {
            exprTypeToReturn.setExpressionType(ExpressionType.VARCHAR_TYPE,
                    aVarCharType.length + 30, -1, -1);
        } else if (typeToCheck.type == ExpressionType.TIMESTAMP_TYPE) {
            exprTypeToReturn.setExpressionType(ExpressionType.VARCHAR_TYPE,
                    aVarCharType.length + 30, -1, -1);
        }
        return exprTypeToReturn;
    }

    /*
     * This function converts a Small int or Int type to char type , where char
     * type can be -- NVCHAR,CHAR,VCHAR and NCHAR.
     *
     * Even Though - smallint is 6 units in size we use 10 , which should not
     * make much difference. Use 10 units in length
     */

    /**
     *
     * @param exprTypeToReturn
     * @param aCharType
     */
    public void ConvertSmallIntToCharType(ExpressionType exprTypeToReturn,
            ExpressionType aCharType) {
        exprTypeToReturn.setExpressionType(aCharType.type,
                aCharType.length + 10, -1, -1);

    }

    /**
     *
     * @param exprTypeToReturn
     * @param aCharType
     */
    public void ConvertIntToCharType(ExpressionType exprTypeToReturn,
            ExpressionType aCharType) {
        exprTypeToReturn.setExpressionType(aCharType.type,
                aCharType.length + 11, -1, -1);

    }

    /**
     *
     * @param exprTypeToReturn
     * @param aCharType
     */
    public void ConvertBigIntToCharType(ExpressionType exprTypeToReturn,
            ExpressionType aCharType) {
        exprTypeToReturn.setExpressionType(aCharType.type,
                aCharType.length + 17, -1, -1);

    }

    // Handle Var Char Ends

    // Handle TimeStamp
    /**
     *
     * @param aExprTypeHelper
     * @return ExpressionType - The expression Type is a valid expression type
     *         which is a result of combining the two expressions
     */
    private ExpressionType HandleTimeStamp(ExprTypeHelper aExprTypeHelper) {
        ExpressionType exprTypeToReturn = new ExpressionType();
        int leftType = aExprTypeHelper.leftExprType.type;
        int rightType = aExprTypeHelper.righExprType.type;
        if (leftType == ExpressionType.TIMESTAMP_TYPE
                && rightType == ExpressionType.TIMESTAMP_TYPE
                && aExprTypeHelper.Operator.equals("-")) {
            exprTypeToReturn.setExpressionType(ExpressionType.INTERVAL_TYPE,
                    -1, -1, -1);
            return exprTypeToReturn;

        }
        if (leftType == ExpressionType.TIMESTAMP_TYPE
                || rightType == ExpressionType.TIMESTAMP_TYPE
                || leftType == ExpressionType.NULL_TYPE
                || rightType == ExpressionType.NULL_TYPE) {
            exprTypeToReturn.setExpressionType(ExpressionType.TIMESTAMP_TYPE,
                    ExpressionType.TIMESTAMPLEN, -1, -1);
        }
        return exprTypeToReturn;
    }

    private ExpressionType HandleInterval(ExprTypeHelper aExprTypeHelper) {
        ExpressionType exprTypeToReturn = new ExpressionType();
        exprTypeToReturn.setExpressionType(ExpressionType.INTERVAL_TYPE, -1,
                -1, -1);
        return exprTypeToReturn;
    }

    // Return INET type if  any of the expression types is INET
    private ExpressionType HandleInet(ExprTypeHelper aExprTypeHelper) {
        ExpressionType exprTypeToReturn = new ExpressionType();
        String Operator = aExprTypeHelper.Operator;

        if (Operator.equals("&") == true ||
                Operator.equals("|") == true ||
                Operator.equals("+") == true ||
                Operator.equals("-") == true) {
            exprTypeToReturn.setExpressionType(ExpressionType.INET_TYPE,
                -1, -1, -1);
            return exprTypeToReturn;
        }

        // return BOOLEAN for other operators
        exprTypeToReturn.setExpressionType(ExpressionType.BOOLEAN_TYPE,
                -1, -1, -1);
        return exprTypeToReturn;
    }

    // Return CIDR type if  any of the expression types is CIDR
    private ExpressionType HandleCidr(ExprTypeHelper aExprTypeHelper) {
        ExpressionType exprTypeToReturn = new ExpressionType();
        String Operator = aExprTypeHelper.Operator;

        if (Operator.equals("&") == true ||
                Operator.equals("|") == true ||
                Operator.equals("+") == true ||
                Operator.equals("-") == true) {
            exprTypeToReturn.setExpressionType(ExpressionType.CIDR_TYPE,
                    -1, -1, -1);
            return exprTypeToReturn;
        }

        // return BOOLEAN for other operators
        exprTypeToReturn.setExpressionType(ExpressionType.BOOLEAN_TYPE,
                -1, -1, -1);
        return exprTypeToReturn;
    }

    // Return BOOLEAN type if  any of the expression types is GEOMETRY
    private ExpressionType HandleGeometry(ExprTypeHelper aExprTypeHelper) {
        ExpressionType exprTypeToReturn = new ExpressionType();

        // return BOOLEAN for other operators
        exprTypeToReturn.setExpressionType(ExpressionType.BOOLEAN_TYPE,
                -1, -1, -1);
        return exprTypeToReturn;
    }

    // Return BOOLEAN type if  any of the expression types is BOX2D
    private ExpressionType HandleBox2D(ExprTypeHelper aExprTypeHelper) {
        ExpressionType exprTypeToReturn = new ExpressionType();

        // return BOOLEAN for other operators
        exprTypeToReturn.setExpressionType(ExpressionType.BOOLEAN_TYPE,
                -1, -1, -1);
        return exprTypeToReturn;
    }

    // Return BOOLEAN type if  any of the expression types is BOX3D
    private ExpressionType HandleBox3D(ExprTypeHelper aExprTypeHelper) {
        ExpressionType exprTypeToReturn = new ExpressionType();

        // return BOOLEAN for other operators
        exprTypeToReturn.setExpressionType(ExpressionType.BOOLEAN_TYPE,
                -1, -1, -1);
        return exprTypeToReturn;
    }
    
    // Return BOOLEAN type if  any of the expression types is BOX3DEXTENT
    private ExpressionType HandleBox3DExtent(ExprTypeHelper aExprTypeHelper) {
        ExpressionType exprTypeToReturn = new ExpressionType();

        // return BOOLEAN for other operators
        exprTypeToReturn.setExpressionType(ExpressionType.BOOLEAN_TYPE,
                -1, -1, -1);
        return exprTypeToReturn;
    }
        
    // Handle Time Stamp

    // Handle Decimal
    /**
     *
     * @param aExprTypeHelper
     * @return ExpressionType - The expression Type is a valid expression type
     *         which is a result of combining the two expressions
     */

    private ExpressionType HandleDecimal(ExprTypeHelper aExprTypeHelper) {

        ExpressionType exprTypeToReturn = new ExpressionType();
        int leftType = aExprTypeHelper.leftExprType.type;
        int rightType = aExprTypeHelper.righExprType.type;
        String Operator = aExprTypeHelper.Operator;
        ExpressionType typeToCheck = null;

        ExpressionType aDecType = null;

        if (Operator.equals("||") == true) {
            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);

            // throw new XDBSemanticException("Concatenation is not supported
            // between decimals");
        }

        if (leftType == ExpressionType.CHAR_TYPE) {
            typeToCheck = aExprTypeHelper.righExprType;
            aDecType = aExprTypeHelper.leftExprType;
        } else {
            typeToCheck = aExprTypeHelper.leftExprType;
            aDecType = aExprTypeHelper.righExprType;
        }
        if (typeToCheck.type == ExpressionType.DECIMAL_TYPE
                || typeToCheck.type == ExpressionType.NULL_TYPE) {
            exprTypeToReturn = MergeNumericTypes(typeToCheck, aDecType);
        } else if (leftType == ExpressionType.TIMESTAMP_TYPE
                || rightType == ExpressionType.TIMESTAMP_TYPE) {
            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);

            // throw new XDBSemanticException("Operation Not Compatibke");
        }
        return exprTypeToReturn;
    }

    // Handle Decimal end

    // Handle Date Starts
    /**
     *
     * @param aExprTypeHelper
     * @return ExpressionType - The expression Type is a valid expression type
     *         which is a result of combining the two expressions
     */
    private ExpressionType HandleDate(ExprTypeHelper aExprTypeHelper) { // We
        // should
        // never
        // come
        // here
        ExpressionType exprTypeToReturn;

        int leftType = aExprTypeHelper.leftExprType.type;
        int rightType = aExprTypeHelper.righExprType.type;
        int typeToCheck = -1;
        if (leftType == ExpressionType.DATE_TYPE) {
            typeToCheck = rightType;
        } else {
            typeToCheck = leftType;
        }
        if (typeToCheck == ExpressionType.NULL_TYPE) {
            exprTypeToReturn = new ExpressionType();
            exprTypeToReturn.type = ExpressionType.DATE_TYPE;
            exprTypeToReturn.precision = -1;
            exprTypeToReturn.length = ExpressionType.DATELEN;
            exprTypeToReturn.scale = -1;
            return exprTypeToReturn;
        }
        if (typeToCheck == ExpressionType.INTERVAL_TYPE) {
            exprTypeToReturn = new ExpressionType();
            exprTypeToReturn.type = ExpressionType.TIMESTAMP_TYPE;
            exprTypeToReturn.precision = -1;
            exprTypeToReturn.length = ExpressionType.DATELEN;
            exprTypeToReturn.scale = -1;
            return exprTypeToReturn;
        }

        if (typeToCheck == ExpressionType.TIME_TYPE) {
            exprTypeToReturn = new ExpressionType();
            exprTypeToReturn.type = ExpressionType.TIME_TYPE;
            exprTypeToReturn.precision = -1;
            exprTypeToReturn.length = ExpressionType.TIMELEN;
            exprTypeToReturn.scale = -1;
            return exprTypeToReturn;
        }

        if (typeToCheck == ExpressionType.DATE_TYPE) {
            exprTypeToReturn = new ExpressionType();
            exprTypeToReturn.type = ExpressionType.DATE_TYPE;
            exprTypeToReturn.precision = -1;
            exprTypeToReturn.length = ExpressionType.DATELEN;
            exprTypeToReturn.scale = -1;
            return exprTypeToReturn;
        }

        if (typeToCheck == ExpressionType.TIMESTAMP_TYPE) {
            exprTypeToReturn = new ExpressionType();
            exprTypeToReturn.type = ExpressionType.VARCHAR_TYPE;
            exprTypeToReturn.length = ExpressionType.VARCHAR_MAX;
            exprTypeToReturn.precision = -1;
            exprTypeToReturn.scale = -1;
            return exprTypeToReturn;
        } else {
            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);
        }

        // throw new XDBSemanticException("The Operation is invalid");

    }

    // Handle Date Ends

    // Handle Float
    /**
     *
     * @param aExprTypeHelper
     * @return ExpressionType - The expression Type is a valid expression type
     *         which is a result of combining the two expressions
     */
    private ExpressionType HandleFloat(ExprTypeHelper aExprTypeHelper) {
        ExpressionType exprTypeToReturn = new ExpressionType();
        int leftType = aExprTypeHelper.leftExprType.type;
        ExpressionType typeToCheck = null;
        ExpressionType aFloatType = null;

        if (leftType == ExpressionType.FLOAT_TYPE) {
            typeToCheck = aExprTypeHelper.righExprType;
            aFloatType = aExprTypeHelper.leftExprType;
        } else {
            typeToCheck = aExprTypeHelper.leftExprType;
            aFloatType = aExprTypeHelper.righExprType;
        }
        if (typeToCheck.type == ExpressionType.FLOAT_TYPE
                || typeToCheck.type == DECIMAL_TYPE
                || typeToCheck.type == ExpressionType.NULL_TYPE) {
            exprTypeToReturn = MergeNumericTypes(typeToCheck, aFloatType);
        } else if (typeToCheck.type == ExpressionType.DATE_TYPE) {
            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);

            // throw new XDBSemanticException("The Operation is invlaid");
        } else if (typeToCheck.type == ExpressionType.TIMESTAMP_TYPE) {
            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);

            // throw new XDBSemanticException("The Operation is invlaid");
        }
        return exprTypeToReturn;
    }

    // Handle Float Ends

    // Handle Real Values
    /**
     *
     * @param aExprTypeHelper
     * @return ExpressionType - The expression Type is a valid expression type
     *         which is a result of combining the two expressions
     */
    private ExpressionType HandleReal(ExprTypeHelper aExprTypeHelper) {

        ExpressionType exprTypeToReturn = new ExpressionType();
        int leftType = aExprTypeHelper.leftExprType.type;
        ExpressionType typeToCheck = null;
        ExpressionType aRealType = null;

        if (leftType == ExpressionType.REAL_TYPE) {
            typeToCheck = aExprTypeHelper.righExprType;
            aRealType = aExprTypeHelper.leftExprType;
        } else {
            typeToCheck = aExprTypeHelper.leftExprType;
            aRealType = aExprTypeHelper.righExprType;
        }

        if (typeToCheck.type == ExpressionType.REAL_TYPE
                || typeToCheck.type == FLOAT_TYPE
                || typeToCheck.type == DECIMAL_TYPE
                || typeToCheck.type == ExpressionType.NULL_TYPE) {
            exprTypeToReturn = MergeNumericTypes(typeToCheck, aRealType);
        } else if (typeToCheck.type == ExpressionType.DATE_TYPE) {
            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);

            // throw new XDBSemanticException("Operation is invalid");

        } else if (typeToCheck.type == ExpressionType.TIMESTAMP_TYPE) {
            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);

            // throw new XDBSemanticException("Operation is invalid");
        }
        return exprTypeToReturn;
    }

    // Handle Real Ends

    // Handle Numeric
    /**
     *
     * @param aExprTypeHelper
     * @return ExpressionType - The expression Type is a valid expression type
     *         which is a result of combining the two expressions
     */
    private ExpressionType HandleNumeric(ExprTypeHelper aExprTypeHelper) {
        ExpressionType exprTypeToReturn = new ExpressionType();
        int leftType = aExprTypeHelper.leftExprType.type;
        ExpressionType typeToCheck = null;
        ExpressionType aNumericType = null;

        if (leftType == ExpressionType.NUMERIC_TYPE) {
            typeToCheck = aExprTypeHelper.righExprType;
            aNumericType = aExprTypeHelper.leftExprType;
        } else {
            typeToCheck = aExprTypeHelper.leftExprType;
            aNumericType = aExprTypeHelper.righExprType;
        }

        if (typeToCheck.type == ExpressionType.NUMERIC_TYPE
                || typeToCheck.type == ExpressionType.REAL_TYPE
                || typeToCheck.type == ExpressionType.FLOAT_TYPE
                || typeToCheck.type == ExpressionType.DECIMAL_TYPE
                || typeToCheck.type == ExpressionType.NULL_TYPE) {

            exprTypeToReturn = MergeNumericTypes(typeToCheck, aNumericType);

        } else if (typeToCheck.type == ExpressionType.DATE_TYPE) {
            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);

            // throw new XDBSemanticException("The Types are not compatible for
            // this operation");
        } else if (typeToCheck.type == ExpressionType.TIMESTAMP_TYPE) {
            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);

            // throw new XDBSemanticException("The Types are not compatible for
            // this operation");
        }
        return exprTypeToReturn;
    }

    // Handle Numeric ends
    /**
     * This function is responsible for merging interaction between two numeric
     * expressiontype
     *
     * @param expra
     * @param exprb
     * @return ExpressionType - The expression Type is a valid expression type
     *         which is a result of combining the two expressions
     */
    public static ExpressionType MergeNumericTypes(ExpressionType expra,
            ExpressionType exprb) {
        /*
         * + Precedence Matrix ForNULL SmallInt Serial Int Float Real Numeric
         * Decimal DoublePrecision
         * ------------------------------------------------------------------- 7
         * 6 5 4 3 3 2 2 1
         *
         * The algorithm is as follows : 1. Find the precedence of the type 2.
         * Choose the one which is the highest 3. Calculate the
         * length,precision,scale and type of the new type.
         *
         * ..REAL and FLOAT have lengths ..INT ,SMALLINT , SERIAL ,DOUBLE
         * PRECISION have none .. NUMERIC , DECIMAL have precision and scale
         *
         * If the Precision goes higher than 38 we return a float
         */

        int length = -1;
        int precision = -1;
        int scale = -1;
        int type = 0;

        int exprap = expra.getTypePrecedence();
        int exprbp = exprb.getTypePrecedence();
        type = exprbp < exprap ? exprb.type : expra.type;

        ExpressionType exprType = new ExpressionType();

        // Incase we have a case of double precision - there is no need to do
        // any analysis
        // Just return the double precision value
        if (type == ExpressionType.DOUBLEPRECISION_TYPE) {
            exprType.setExpressionType(type, 38, 0, 0);
            return exprType;
        }

        // Now we know what we are converting to what and from what
        // The "determinePrecisionValues()" function change the length for float
        // and real into precision and scale.
        expra.determinePrecisionValues(expra);
        exprb.determinePrecisionValues(exprb);

        int precision1 = expra.precision;
        int scale1 = expra.scale;

        int precision2 = exprb.precision;
        int scale2 = exprb.scale;

        precision = precision1 + precision2;
        scale = scale1 + scale2;
        // For eg ( 20 , 10 ) ( 30, 20) - ( 50, 40) -- ( 38, 20)
        // but by this we will lose the most significant digits -
        if (precision > 38) {
            // Just return float
            precision = 0;
            scale = 0;
            length = 38;
            type = ExpressionType.FLOAT_TYPE;

        } else if (precision < 38) {
            // This is the normal case - let us stay as we are
        }
        length = precision;
        exprType.setExpressionType(type, length, precision, scale);
        return exprType;
    }

    /**
     * This function determines the precision value of the type if it is
     * converted to another
     *
     * @param toConvert
     */
    void determinePrecisionValues(ExpressionType toConvert) {
        if (toConvert.type == ExpressionType.INT_TYPE) {
            toConvert.length = 10;
            toConvert.precision = 10;
            toConvert.scale = 0;
        } else if (toConvert.type == ExpressionType.SMALLINT_TYPE) {
            toConvert.length = 5;
            toConvert.precision = 5;
            toConvert.scale = 0;
        } else if (toConvert.type == ExpressionType.BIGINT_TYPE) {
            toConvert.length = 16;
            toConvert.precision = 16;
            toConvert.scale = 0;
        } else if (toConvert.type == ExpressionType.FLOAT_TYPE
                || toConvert.type == ExpressionType.REAL_TYPE) {
            if (toConvert.length == 0) {
                toConvert.length = 16;
                toConvert.precision = 16;
                toConvert.scale = 0;
            } else {
                toConvert.precision = length;
                scale = 0;

            }
        } else if (toConvert.type == NUMERIC_TYPE
                || toConvert.type == DECIMAL_TYPE) {
            length = toConvert.length;
            if (toConvert.precision == -1) {
                precision = 0;
            } else {
                precision = toConvert.precision;
            }

            if (toConvert.scale == -1) {
                scale = 0;
            } else {
                scale = toConvert.scale;
            }

        } else if (toConvert.type == DOUBLEPRECISION_TYPE) {
            length = 38;
            precision = 38;
            scale = 0;
        }
    }

    /**
     * Handles INT interaction with other data types
     *
     * @param aExprTypeHelper
     * @return ExpressionType - The expression Type is a valid expression type
     *         which is a result of combining the two expressions
     */
    private ExpressionType HandleInt(ExprTypeHelper aExprTypeHelper) {
        ExpressionType exprTypeToReturn = new ExpressionType();
        int leftType = aExprTypeHelper.leftExprType.type;
        ExpressionType typeToCheck = null;
        ExpressionType aIntType = null;

        String Operator = aExprTypeHelper.Operator;
        
        if (Operator.equals(">") == true ||
                Operator.equals(">=") == true ||
                Operator.equals("<") == true ||
                Operator.equals("<=") == true ||
                Operator.equals("!=") == true ||
                Operator.equals("<>") == true) {
        // return BOOLEAN 
        	exprTypeToReturn.setExpressionType(ExpressionType.BOOLEAN_TYPE,
                -1, -1, -1);
            return exprTypeToReturn;
        }
        
        if (leftType == ExpressionType.INT_TYPE) {
            typeToCheck = aExprTypeHelper.righExprType;
            aIntType = aExprTypeHelper.leftExprType;
        } else {
            typeToCheck = aExprTypeHelper.leftExprType;
            aIntType = aExprTypeHelper.righExprType;
        }

        if (typeToCheck.type == ExpressionType.INT_TYPE
                || typeToCheck.type == ExpressionType.SMALLINT_TYPE
                || typeToCheck.type == ExpressionType.BIGINT_TYPE
                || typeToCheck.type == ExpressionType.NULL_TYPE) {
            exprTypeToReturn.setExpressionType(ExpressionType.FLOAT_TYPE, 32,
                    0, 0);
        } else if (typeToCheck.type == ExpressionType.NUMERIC_TYPE
                || typeToCheck.type == ExpressionType.REAL_TYPE
                || typeToCheck.type == ExpressionType.FLOAT_TYPE
                || typeToCheck.type == ExpressionType.DECIMAL_TYPE) {
            exprTypeToReturn = MergeNumericTypes(typeToCheck, aIntType);
        } else if (typeToCheck.type == ExpressionType.DATE_TYPE) {
            exprTypeToReturn.setExpressionType(DATE_TYPE,
                    ExpressionType.DATELEN, -1, -1);
        } else if (typeToCheck.type == ExpressionType.TIMESTAMP_TYPE) {
            exprTypeToReturn.setExpressionType(TIMESTAMP_TYPE,
                    ExpressionType.TIMESTAMPLEN, -1, -1);
        }
        return exprTypeToReturn;

    }

    /**
     * Handles BIGINT interaction with other data types
     *
     * @param aExprTypeHelper
     * @return ExpressionType - The expression Type is a valid expression type
     *         which is a result of combining the two expressions
     */
    private ExpressionType HandleBigInt(ExprTypeHelper aExprTypeHelper) {
        ExpressionType exprTypeToReturn = new ExpressionType();
        int leftType = aExprTypeHelper.leftExprType.type;
        ExpressionType typeToCheck = null;
        ExpressionType aIntType = null;

        if (leftType == ExpressionType.BIGINT_TYPE) {
            typeToCheck = aExprTypeHelper.righExprType;
            aIntType = aExprTypeHelper.leftExprType;
        } else {
            typeToCheck = aExprTypeHelper.leftExprType;
            aIntType = aExprTypeHelper.righExprType;
        }

        if (typeToCheck.type == ExpressionType.INT_TYPE
                || typeToCheck.type == ExpressionType.SMALLINT_TYPE
                || typeToCheck.type == ExpressionType.BIGINT_TYPE
                || typeToCheck.type == ExpressionType.NULL_TYPE) {
            exprTypeToReturn.setExpressionType(ExpressionType.FLOAT_TYPE, 32,
                    0, 0);
        } else if (typeToCheck.type == ExpressionType.NUMERIC_TYPE
                || typeToCheck.type == ExpressionType.REAL_TYPE
                || typeToCheck.type == ExpressionType.FLOAT_TYPE
                || typeToCheck.type == ExpressionType.DECIMAL_TYPE) {
            exprTypeToReturn = MergeNumericTypes(typeToCheck, aIntType);
        } else if (typeToCheck.type == ExpressionType.DATE_TYPE) {
            exprTypeToReturn.setExpressionType(DATE_TYPE,
                    ExpressionType.DATELEN, -1, -1);
        } else if (typeToCheck.type == ExpressionType.TIMESTAMP_TYPE) {
            exprTypeToReturn.setExpressionType(TIMESTAMP_TYPE,
                    ExpressionType.TIMESTAMPLEN, -1, -1);
        }
        return exprTypeToReturn;

    }

    /**
     * Handles Small Int's interaction with other data type
     *
     * @param aExprTypeHelper
     * @return ExpressionType - The expression Type is a valid expression type
     *         which is a result of combining the two expressions
     */
    private ExpressionType HandleSmallInt(ExprTypeHelper aExprTypeHelper) {
        ExpressionType exprTypeToReturn = new ExpressionType();
        int leftType = aExprTypeHelper.leftExprType.type;
        // The value returned by these function should depened on the operator
        // used.

        // For eg. If the operator is + , -, *
        // or if the operator is divide
        ExpressionType typeToCheck = null;
        ExpressionType aSmallIntType = null;
        if (leftType == ExpressionType.SMALLINT_TYPE) {
            typeToCheck = aExprTypeHelper.righExprType;
            aSmallIntType = aExprTypeHelper.leftExprType;
        } else {
            typeToCheck = aExprTypeHelper.leftExprType;
            aSmallIntType = aExprTypeHelper.righExprType;
        }
        if (typeToCheck.type == ExpressionType.NUMERIC_TYPE
                || typeToCheck.type == ExpressionType.REAL_TYPE
                || typeToCheck.type == ExpressionType.FLOAT_TYPE
                || typeToCheck.type == ExpressionType.DECIMAL_TYPE
                || typeToCheck.type == ExpressionType.INT_TYPE
                || typeToCheck.type == ExpressionType.BIGINT_TYPE
                || typeToCheck.type == ExpressionType.NULL_TYPE) {
            exprTypeToReturn = MergeNumericTypes(typeToCheck, aSmallIntType);
        } else if (typeToCheck.type == ExpressionType.DATE_TYPE) {
            exprTypeToReturn.setExpressionType(DATE_TYPE,
                    ExpressionType.DATELEN, -1, -1);
        } else if (typeToCheck.type == ExpressionType.TIMESTAMP_TYPE) {
            exprTypeToReturn.setExpressionType(TIMESTAMP_TYPE,
                    ExpressionType.TIMESTAMPLEN, -1, -1);
        }
        return exprTypeToReturn;
    }

    /**
     * Handles Double Precision
     *
     * @param aExprTypeHelper
     * @return ExpressionType - The expression Type is a valid expression type
     *         which is a result of combining the two expressions
     */
    private ExpressionType HandleDoublePrecison(ExprTypeHelper aExprTypeHelper) {
        ExpressionType exprTypeToReturn = new ExpressionType();
        int leftType = aExprTypeHelper.leftExprType.type;

        ExpressionType typeToCheck = null;
        ExpressionType aDoublePrecisionType = null;
        if (leftType == ExpressionType.DOUBLEPRECISION_TYPE) {
            typeToCheck = aExprTypeHelper.righExprType;
            aDoublePrecisionType = aExprTypeHelper.leftExprType;
        } else {
            typeToCheck = aExprTypeHelper.leftExprType;
            aDoublePrecisionType = aExprTypeHelper.righExprType;
        }
        if (typeToCheck.type == ExpressionType.DOUBLEPRECISION_TYPE
                || typeToCheck.type == ExpressionType.INT_TYPE
                || typeToCheck.type == ExpressionType.BIGINT_TYPE
                || typeToCheck.type == ExpressionType.NUMERIC_TYPE
                || typeToCheck.type == ExpressionType.REAL_TYPE
                || typeToCheck.type == ExpressionType.SMALLINT_TYPE
                || typeToCheck.type == ExpressionType.FLOAT_TYPE
                || typeToCheck.type == DECIMAL_TYPE
                || typeToCheck.type == ExpressionType.NULL_TYPE) {
            if (aExprTypeHelper.Operator == "~") {
                exprTypeToReturn.setExpressionType(ExpressionType.BOOLEAN_TYPE,
                        -1, -1, -1);
            } else {
                exprTypeToReturn = MergeNumericTypes(typeToCheck,
                        aDoublePrecisionType);
            }
        } else if (typeToCheck.type == ExpressionType.DATE_TYPE) {
            exprTypeToReturn.setExpressionType(DATE_TYPE,
                    ExpressionType.DATELEN, -1, -1);
        } else if (typeToCheck.type == ExpressionType.TIMESTAMP_TYPE) {
            exprTypeToReturn.setExpressionType(TIMESTAMP_TYPE,
                    ExpressionType.TIMESTAMPLEN, -1, -1);
        } else if (typeToCheck.type == ExpressionType.CHAR_TYPE
                || typeToCheck.type == ExpressionType.VARCHAR_TYPE) {
            exprTypeToReturn.setExpressionType(typeToCheck.type,
                    typeToCheck.length + aDoublePrecisionType.length, -1, -1);
        }

        if (exprTypeToReturn.type == 0) {
            exprTypeToReturn.setExpressionType(
                    ExpressionType.DOUBLEPRECISION_TYPE, -1, -1, -1);
        }
        return exprTypeToReturn;

    }

    /**
     * Handles CLOB type
     *
     * @param aExprTypeHelper
     * @return ExpressionType - The expression Type is a valid expression type
     *         which is a result of combining the two expressions
     */
    private ExpressionType HandleClobType(ExprTypeHelper aExprTypeHelper) {
        ExpressionType exprTypeToReturn = new ExpressionType();
        exprTypeToReturn.setExpressionType(ExpressionType.CLOB_TYPE, 0, 0, 0);
        return exprTypeToReturn;
    }

    /**
     * Handles Time Type
     *
     * @param aExprTypeHelper
     * @return ExpressionType - The expression Type is a valid expression type
     *         which is a result of combining the two expressions
     */
    private ExpressionType HandleTimeType(ExprTypeHelper aExprTypeHelper) {
        ExpressionType exprTypeToReturn = new ExpressionType();
        int leftType = aExprTypeHelper.leftExprType.type;

        ExpressionType typeToCheck = null;
        ExpressionType aTimeType = null;
        if (leftType == ExpressionType.TIME_TYPE) {
            typeToCheck = aExprTypeHelper.righExprType;
            aTimeType = aExprTypeHelper.leftExprType;

        } else {
            typeToCheck = aExprTypeHelper.leftExprType;
            aTimeType = aExprTypeHelper.righExprType;
        }

        if (typeToCheck.type == ExpressionType.NULL_TYPE
                || typeToCheck.type == ExpressionType.DATE_TYPE) {
            exprTypeToReturn = aTimeType;
            return exprTypeToReturn;
        } else {
            throw new InvalidExpressionException(aExprTypeHelper.leftExprType,
                    aExprTypeHelper.Operator, aExprTypeHelper.righExprType);
        }
        // throw new XDBSemanticException("Operation not allowed for this
        // particular data type");
    }

    /**
     * Determine data type returned by operator if possible
     * 
     * @param operator
     * @return
     */
    private ExpressionType HandleOperator(String operator) {
        ExpressionType eType = null;
        if ("=".equals(operator) || "!=".equals(operator)
                || "<=".equals(operator) || ">=".equals(operator)
                || "<".equals(operator) || ">".equals(operator)
                || "<>".equals(operator) || "is".equals(operator)
                || "is not".equals(operator)) {
            eType = new ExpressionType();
            eType.setExpressionType(BOOLEAN_TYPE, 0, 0, 0);
            return eType;
        }
        return null;
    }

    // For SmallInt Serial Int Float Real Numeric Decimal DoublePrecision
    // -------------------------------------------------------------------
    // 6 5 4 3 3 2 2 1
    /**
     * Each numeric data type is given a precedence and if two numeric types are
     * joined with a numeric type of another data type, The one with a higher
     * value is converted to the lower value.
     *
     * This function just checks the type of the expression and returns
     * appropriate value.
     *
     * @return a integer which is the precedence value
     */
    public int getTypePrecedence() {
        int precedenceNumber = 0;
        switch (type) {
        case SMALLINT_TYPE:
            precedenceNumber = 6;
            break;
        case INT_TYPE:
            precedenceNumber = 5;
            break;
        case BIGINT_TYPE:
            precedenceNumber = 4;
            break;
        case REAL_TYPE:
            precedenceNumber = 3;
            break;
        case FLOAT_TYPE:
            precedenceNumber = 3;
            break;
        case NUMERIC_TYPE:
            precedenceNumber = 2;
            break;
        case DECIMAL_TYPE:
            precedenceNumber = 2;
            break;
        case DOUBLEPRECISION_TYPE:
            precedenceNumber = 1;
            break;
        case NULL_TYPE:
            precedenceNumber = 7;
            break;

        }
        return precedenceNumber;
    }

    /**
     * Returns the byte length of the column. Note this may vary slightly from
     * vendor to vendor
     */
    public int getByteLength() {
        int byteLength;

        switch (this.type) {
        case java.sql.Types.BIT:
            byteLength = 1;
            break;
        case java.sql.Types.BINARY:
            byteLength = this.length;
            break;
        case java.sql.Types.VARBINARY:
            byteLength = this.length;
            break;

        case java.sql.Types.CHAR:
            byteLength = this.length;
            break;

        case java.sql.Types.VARCHAR:
        case ExpressionType.REGCLASS_TYPE:
            byteLength = this.length / 3;
            break;

        case java.sql.Types.SMALLINT:
            byteLength = 2;
            break;

        case java.sql.Types.INTEGER:
            byteLength = 4;
            break;

        case java.sql.Types.BIGINT:
            byteLength = 8;
            break;

        case java.sql.Types.DECIMAL:
            byteLength = 4; // should be ok?
            break;

        case java.sql.Types.NUMERIC:
            byteLength = 4; // should be ok?
            break;

        case java.sql.Types.REAL:
            byteLength = 8;
            break;

        case java.sql.Types.FLOAT:
            byteLength = 8;
            break;

        case java.sql.Types.DATE:
            byteLength = 2;
            break;

        case java.sql.Types.TIME:
            byteLength = 2;
            break;

        case java.sql.Types.TIMESTAMP:
            byteLength = 4;
            break;
        case java.sql.Types.DOUBLE:
            byteLength = 8;
            break;

        case java.sql.Types.BOOLEAN:
            byteLength = 1;
            break;
        case java.sql.Types.CLOB:
            byteLength = 1;
            break;
        case java.sql.Types.BLOB:
            byteLength = 1;
            break;
        case ExpressionType.MACADDR_TYPE:
            byteLength = 19;
            break;
        case ExpressionType.CIDR_TYPE:
            byteLength = 19;
            break;
        case ExpressionType.INET_TYPE:
            byteLength = 19;
            break;
        case ExpressionType.GEOMETRY_TYPE:
        case ExpressionType.BOX2D_TYPE:
        case ExpressionType.BOX3D_TYPE:
        case ExpressionType.BOX3DEXTENT_TYPE:
        	byteLength = this.length;
        	break;
        case ExpressionType.INTERVAL_TYPE:
            byteLength = 12;
            break;
        default:
            throw new XDBServerException(
                    ErrorMessageRepository.INVALID_DATATYPE + "(" + type + " )",
                    0, ErrorMessageRepository.INVALID_DATATYPE_CODE);
        }

        return byteLength;
    }

}
