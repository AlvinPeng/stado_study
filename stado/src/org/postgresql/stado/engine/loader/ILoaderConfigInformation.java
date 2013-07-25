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

package org.postgresql.stado.engine.loader;

import java.util.List;

import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.partitions.PartitionMap;
import org.postgresql.stado.parser.ExpressionType;


/**
 * Puropose:
 *
 * The main purpose of this interface is to able to easily obtain
 * data loader's configuration information so that we can launch mutiple data-processor threads
 * with the same configuration settings.
 *
 * (for further information see DataProcessorThread class).
 *
 *
 */
public interface ILoaderConfigInformation {

    public Loader               getLoaderInstance() ;
    public XDBSessionContext    getClient() ;
    public PartitionMap         getPartitionMap() ;
    public SysTable             getTableInformation() ;
    public List<TableColumnDescription> getTableColumnsInformation() ;
    public INodeWriterFactory   getWriterFactory() ;
    public IUniqueValueProvider getRowIdProvider() ;
    public IUniqueValueProvider getSerialProvider() ;

    public char getSeparator() ;

    public boolean noParse() ;
    public boolean destinationTypeNodeId() ;
    public boolean suppressSendingNodeId() ;

    public int getTableColumnsCount() ;
    public int getSerialColumnPosition() ;
    public int getSerialColumnSequence() ;
    public int getPartitionColumnSequence() ;
    public ExpressionType getHashDataType() ;
    public int getXRowidColumnSequence() ;

    public String getNULLValue();
    public char getQuoteChar();
    public char getQuoteEscape();
}
