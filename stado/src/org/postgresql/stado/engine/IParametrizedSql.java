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
/**
 * 
 */
package org.postgresql.stado.engine;

import org.postgresql.stado.exception.XDBServerException;

/**
 * The interface should be implemented by SQL statement that supports parameters
 * 
 * 
 * @version draft
 */
public interface IParametrizedSql // ?? extends IPreparable
{
    /**
     * Returns number of parameters found in the statement
     * 
     * @return the number of parameters
     * @throws XDBServerException
     *                 if error occurs, e.g. statement has not been parsed
     */
    int getParamCount() throws XDBServerException;

    /**
     * Get value for specific parameter
     * 
     * @param index
     *                Zero-based index (0 - first parameter, 1 - second, etc.)
     * @return Value for the parameter
     * @throws ArrayIndexOutOfBoundsException
     *                 if parameter with specified index does not exist
     * @throws XDBServerException
     *                 if error occurs
     * 
     */
    String getParamValue(int index) throws ArrayIndexOutOfBoundsException,
            XDBServerException;

    /**
     * Get values for all parameters
     * 
     * @return the values
     * @throws ArrayIndexOutOfBoundsException
     *                 if supplied array is longer than number of parameters
     * @throws XDBServerException
     *                 if error occurs
     */
    String[] getParamValues() throws ArrayIndexOutOfBoundsException,
            XDBServerException;

    /**
     * Set value for specific parameter
     * 
     * @param index
     *                Zero-based index (0 - first parameter, 1 - second, etc.)
     * @param value
     *                Value for the parameter
     * @throws ArrayIndexOutOfBoundsException
     *                 if parameter with specified index does not exist
     * @throws XDBServerException
     *                 if error occurs
     */
    void setParamValue(int index, String value)
            throws ArrayIndexOutOfBoundsException, XDBServerException;

    /**
     * Set value for all parameters
     * 
     * @param values
     *                the values
     * @throws ArrayIndexOutOfBoundsException
     *                 if supplied array is longer than number of parameters
     * @throws XDBServerException
     *                 if error occurs
     */
    void setParamValues(String[] values) throws ArrayIndexOutOfBoundsException,
            XDBServerException;

    /**
     * Get data type for specific parameter
     * 
     * @param index
     *                Zero-based index (0 - first parameter, 1 - second, etc.)
     * @return Data type of the parameter
     * @throws ArrayIndexOutOfBoundsException
     *                 if parameter with specified index does not exist
     * @throws XDBServerException
     *                 if error occurs
     * 
     */
    int getParamDataType(int index) throws ArrayIndexOutOfBoundsException,
            XDBServerException;

    /**
     * Get data types for all parameters
     * 
     * @return the data types
     * @throws ArrayIndexOutOfBoundsException
     *                 if supplied array is longer than number of parameters
     * @throws XDBServerException
     *                 if error occurs
     */
    int[] getParamDataTypes() throws ArrayIndexOutOfBoundsException,
            XDBServerException;

    /**
     * Set data type for specific parameter
     * 
     * @param index
     *                Zero-based index (0 - first parameter, 1 - second, etc.)
     * @param dataType
     *                Data Type for the parameter
     * @throws ArrayIndexOutOfBoundsException
     *                 if parameter with specified index does not exist
     * @throws XDBServerException
     *                 if error occurs
     */
    void setParamDataType(int index, int dataType)
            throws ArrayIndexOutOfBoundsException, XDBServerException;

    /**
     * Set data types for all parameters
     * 
     * @param data
     *                types the data types
     * @throws ArrayIndexOutOfBoundsException
     *                 if supplied array is longer than number of parameters
     * @throws XDBServerException
     *                 if error occurs
     */
    void setParamDataTypes(int[] dataTypes)
            throws ArrayIndexOutOfBoundsException, XDBServerException;
}
