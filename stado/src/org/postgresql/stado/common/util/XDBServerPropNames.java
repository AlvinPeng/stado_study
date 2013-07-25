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
 * XDBServerPropNames.java
 * 
 *  
 */
package org.postgresql.stado.common.util;

/**
 *  
 */
public interface XDBServerPropNames {
    public static final String ALL_PROCEDURES_ARE_CALLABLE = "allProceduresAreCallable";

    public static final String ALL_TABLES_ARE_SELECTABLE = "allTablesAreSelectable";

    public static final String DATA_DEFINITION_CAUSES_COMMIT = "dataDefinitionCausesCommit";

    public static final String DATA_DEFINITION_IGNORED_IN_TRANS = "dataDefinitionIgnoredInTrans";

    public static final String DELETES_ARE_DETECTED = "deletesAreDetected";

    public static final String DOES_MAX_ROW_SIZE_INCLUDE_BLOBS = "doesMaxRowSizeIncludeBlobs";

    public static final String CATALOG_SEPARATOR = "catalogSeparator";

    public static final String CATALOG_TERM = "catalogTerm";

    public static final String DATABASE_MAJOR_VERSION = "databaseMajorVersion";

    public static final String DATABASE_MINOR_VERSION = "databaseMinorVersion";

    public static final String DATABASE_PRODUCT_NAME = "databaseProductName";

    public static final String DATABASE_VERSION = "databaseVersion";

    public static final String DEFAULT_TRANS_ISOLATION = "defaultTransIsolation";

    public static final String EXTRA_NAME_CHARACTERS = "extraNameCharacters";

    public static final String IDENTIFIER_QUOTE_STRING = "identifierQuoteString";

    public static final String MAX_BINARY_LITERAL_LENGTH = "maxBinaryLiteralLength";

    public static final String MAX_CATALOG_NAME_LENGTH = "maxCatalogNameLength";

    public static final String MAX_CHAR_LITERAL_LENGTH = "maxCharLiteralLength";

    public static final String MAX_COLUMN_NAME_LENGTH = "maxColumnNameLength";

    public static final String MAX_COLUMNS_IN_GROUPBY = "maxColumnsInGroupBy";

    public static final String MAX_COLUMNS_IN_INDEX = "maxColumnsInIndex";

    public static final String MAX_COLUMNS_IN_ORDERBY = "maxColumnsInOrderBy";

    public static final String MAX_COLUMNS_IN_SELECT = "maxColumnsInSelect";

    public static final String MAX_COLUMNS_IN_TABLE = "maxColumnsInTable";

    public static final String MAX_CONNECTIONS = "maxConnections";

    public static final String MAX_CURSOR_NAME_LENGTH = "maxCursorNameLength";

    public static final String MAX_INDEX_LENGTH = "maxIndexLength";

    public static final String MAX_PROCEDURE_NAME_LENGTH = "maxProcedureNameLength";

    public static final String MAX_ROW_SIZE = "maxRowSize";

    public static final String MAX_SCHEMA_NAME_LENGTH = "maxSchemaNameLength";

    public static final String MAX_STATEMENT_LENGTH = "maxStatementLength";

    public static final String MAX_STATEMENTS = "maxStatements";

    public static final String MAX_TABLE_NAME_LENGTH = "maxTableNameLength";

    public static final String MAX_TABLES_IN_SELECT = "maxTablesInSelect";

    public static final String MAX_USER_NAME_LENGTH = "maxUserNameLength";

    public static final String NUMERIC_FUNCTIONS = "numericFunctions";

    public static final String PROCEDURE_TERM = "procedureTerm";

    public static final String RESULT_SET_HOLDABILITY = "resultSetHoldability";

    public static final String SQL_KEYWORDS = "SQLKeywords";

    public static final String SQL_STATE_TYPE = "SQLStateType";

    public static final String SCHEMA_TERM = "schemaTerm";

    public static final String SEARCH_STRING_ESCAPE = "searchStringEscape";

    public static final String STRING_FUNCTIONS = "StringFunctions";

    public static final String SYSTEM_FUNCTIONS = "SystemFunctions";

    public static final String TIME_DATE_FUNCTIONS = "TimeDateFunctions";

    public static final String INSERTS_ARE_DETECTED = "insertsAreDetected";

    public static final String CATALOG_AT_START = "catalogAtStart";

    public static final String READ_ONLY = "readOnly";

    public static final String LOCATORS_UPDATE_COPY = "locatorsUpdateCopy";

    public static final String NULL_PLUS_NON_NULL_IS_NULL = "nullPlusNonNullIsNull";

    public static final String NULLS_SORTED_AT_END = "nullsSortedAtEnd";

    public static final String NULLS_SORTED_AT_START = "nullsSortedAtStart";

    public static final String NULLS_SORTED_HIGH = "nullsSortedHigh";

    public static final String NULLS_SORTED_LOW = "nullsSortedLow";

    public static final String OTHERS_DELETES_VISIBLE = "othersDeletesVisible";

    public static final String OTHERS_INSERTS_VISIBLE = "othersInsetrsVisible";

    public static final String OTHERS_UPDATES_VISIBLE = "othersUpdatesVisible";

    public static final String OWN_DELETES_VISIBLE = "ownDeletesVisible";

    public static final String OWN_INSERTS_VISIBLE = "ownInsetrsVisible";

    public static final String OWN_UPDATES_VISIBLE = "ownUpdatesVisible";

    public static final String STORES_LOWERCASE_IDENTIFIERS = "storesLowercaseIdentifiers";

    public static final String STORES_LOWERCASE_QUOTED_IDENTIFIERS = "storesLowercaseQuotedIdentifiers";

    public static final String STORES_MIXEDCASE_IDENTIFIERS = "storesMixedcaseIdentifiers";

    public static final String STORES_MIXEDCASE_QUOTED_IDENTIFIERS = "storesMixedcaseQuotedIdentifiers";

    public static final String STORES_UPPERCASE_IDENTIFIERS = "storesUppercaseIdentifiers";

    public static final String STORES_UPPERCASE_QUOTED_IDENTIFIERS = "storesUppercaseQuotedIdentifiers";

    public static final String SUPPORTS_ANSI92_ENTRY_LEVEL = "supportsAnsi92EntryLevel";

    public static final String SUPPORTS_ANSI92_INTERMEDIATE = "supportsAnsi92Intermediate";

    public static final String SUPPORTS_ANSI92_FULL = "supportsAnsi92Full";

    public static final String SUPPORTS_ALTER_TABLE_ADD_COLUMN = "supportsAlterTableAddColumn";

    public static final String SUPPORTS_ALTER_TABLE_DROP_COLUMN = "supportsAlterTableDropColumn";

    public static final String SUPPORTS_BATCH_UPDATES = "supportsBatchUpdates";

    public static final String SUPPORTS_CATALOGS_IN_DML = "supportsCatalogsInDML";

    public static final String SUPPORTS_CATALOGS_IN_INDEX_DEFS = "supportsCatalogsInIndexDefs";

    public static final String SUPPORTS_CATALOGS_IN_PRIVILEGE_DEFS = "supportsCatalogsInPrivilegeDefs";

    public static final String SUPPORTS_CATALOGS_IN_PROCEDURE_CALLS = "supportsCatalogsInProcedureCalls";

    public static final String SUPPORTS_CATALOGS_IN_TABLE_DEFS = "supportsCatalogsInTableDefs";

    public static final String SUPPORTS_COLUMN_ALIASING = "supportsColumnAliasing";

    public static final String SUPPORTS_CONVERT = "supportsConvert";

    public static final String SUPPORTS_CORE_SQL_GRAMMAR = "supportsCoreSQLGrammar";

    public static final String SUPPORTS_CORELLATED_CUBQUERIES = "supportsCorellatedSubqueries";

    public static final String SUPPORTS_DDL_AND_DML_IN_TRANSACTIONS = "supportsDDLAndDMLInTransactions";

    public static final String SUPPORTS_DML_IN_TRANSACTIONS_ONLY = "supportsDMLInTransactionsOnly";

    public static final String SUPPORTS_DIFFERENT_TABLE_CORELLATION_NAMES = "supportsDifferentTableCorellationNames";

    public static final String SUPPORTS_EXPRESSIONS_IN_ORDER_BY = "supportsExpressionsInOrderBy";

    public static final String SUPPORTS_EXTENDED_SQL_GRAMMAR = "supportsExtendedSQLGrammar";

    public static final String SUPPORTS_FULL_OUTER_JOINS = "supportsFullOuterJoins";

    public static final String SUPPORTS_GET_GENERATED_KEYS = "supportsGetGeneratedKeys";

    public static final String SUPPORTS_GROUP_BY = "supportsGroupBy";

    public static final String SUPPORTS_GROUP_BY_BEYOND_SELECT = "supportsGroupByBeyondSelect";

    public static final String SUPPORTS_GROUP_BY_UNRELATED = "supportsGroupByUnrelated";

    public static final String SUPPORTS_INTEGRITY_ENHANCEMENT_FACILITY = "supportsIntegrityEnhancementFacility";

    public static final String SUPPORTS_LIKE_ESCAPE_CLAUSE = "supportsLikeEscapeClause";

    public static final String SUPPORTS_LIMITED_OUTER_JOINS = "supportsLimitedOuterJoins";

    public static final String SUPPORTS_MINIMUM_SQL_GRAMMAR = "supportsMinimumSQLGrammar";

    public static final String SUPPORTS_MIXED_CASE_IDENTIFIERS = "supportsMixedCaseIdentifiers";

    public static final String SUPPORTS_MIXED_CASE_QUOTED_IDENTIFIERS = "supportsMixedCaseQuotedIdentifiers";

    public static final String SUPPORTS_MULTIPLE_OPEN_RESULTS = "supportsMultipleOpenResults";

    public static final String SUPPORTS_MULTIPLE_RESULT_SETS = "supportsMultipleResultSets";

    public static final String SUPPORTS_MULTIPLE_TRANSACTIONS = "supportsMultipleTransactions";

    public static final String SUPPORTS_NAMED_PARAMETERS = "supportsNamedParameters";

    public static final String SUPPORTS_NON_NULLABLE_COLUMNS = "supportsNonNullableColumns";

    public static final String SUPPORTS_OPEN_CURSORS_ACROSS_COMMIT = "supportsOpenCursorsAcrossCommit";

    public static final String SUPPORTS_OPEN_CURSORS_ACROSS_ROLLBACK = "supportsOpenCursorsAcrossRollback";

    public static final String SUPPORTS_OPEN_STATEMENTS_ACROSS_COMMIT = "supportsOpenStatementsAcrossCommit";

    public static final String SUPPORTS_OPEN_STATEMENTS_ACROSS_ROLLBACK = "supportsOpenStatementsAcrossRollback";

    public static final String SUPPORTS_ORDER_BY_UNRELATED = "supportsOrderByUnrelated";

    public static final String SUPPORTS_OUTER_JOINS = "supportsOuterJoins";

    public static final String SUPPORTS_POSITIONED_DELETE = "supportsPositionedDelete";

    public static final String SUPPORTS_POSITIONED_UPDATE = "supportsPositionedUpdate";

    public static final String SUPPORTS_RESULT_SET_CONCURRENCY = "supportsResultSetConcurrency";

    public static final String SUPPORTS_RESULT_SET_HOLDABILITY = "supportsResultSetHoldability";

    public static final String SUPPORTS_RESULT_SET_TYPE = "supportsResultSetType";

    public static final String SUPPORTS_SAVEPOINTS = "supportsSavepoints";

    public static final String SUPPORTS_SCHEMAS_IN_DML = "supportsSchemasInDML";

    public static final String SUPPORTS_SCHEMAS_IN_INDEX_DEFS = "supportsSchemasInIndexDefs";

    public static final String SUPPORTS_SCHEMAS_IN_PRIVILEGE_DEFS = "supportsSchemasInPrivilegeDefs";

    public static final String SUPPORTS_SCHEMAS_IN_PROCEDURE_CALLS = "supportsSchemasInProcedureCalls";

    public static final String SUPPORTS_SCHEMAS_IN_TABLE_DEFS = "supportsSchemasInTableDefs";

    public static final String SUPPORTS_SELECT_FOR_UPDATE = "supportsSelectForUpdate";

    public static final String SUPPORTS_STATEMENT_POOLING = "supportsStatementPooling";

    public static final String SUPPORTS_STORED_PROCEDURES = "supportsStoredProcedures";

    public static final String SUPPORTS_SUBQUERIES_IN_COMPARISONS = "supportsSubqueriesInComparisons";

    public static final String SUPPORTS_SUBQUERIES_IN_EXISTS = "supportsSubqueriesInExists";

    public static final String SUPPORTS_SUBQUERIES_IN_INS = "supportsSubqueriesInIns";

    public static final String SUPPORTS_SUBQUERIES_IN_QUANIFIEDS = "supportsSubqueriesInQuantifieds";

    public static final String SUPPORTS_TABLE_CORELLATION_NAMES = "supportsTableCorellationNames";

    public static final String SUPPORTS_TRANSACTION_ISOLATION_LEVEL = "supportsTransactionIsolationLevel";

    public static final String SUPPORTS_TRANSACTIONS = "supportsTransactions";

    public static final String SUPPORTS_UNION = "supportsUnion";

    public static final String SUPPORTS_UNION_ALL = "supportsUnionAll";

    public static final String UPDATES_ARE_DETECTED = "updatesAreDetected";

    public static final String USES_LOCAL_FILE_PER_TABLE = "usesLocalFilePerTable";

    public static final String USES_LOCAL_FILES = "usesLocalFiles";

}
