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
package org.postgresql.stado.exception;

/**
 * 
 * 
 */

public class ErrorMessageRepository {

    /**
     * USER Failures
     */

    public static String CLASS_PATH_ERROR = "CLASS PATH ERROR JDBC DRIVER MISSING :: ";

    public static int CLASS_PATH_ERROR_CODE = -10001;

    public static String SCALAR_QUERY_RESULT_ERROR = "Scalar Query Returned More Than 1 Result";

    public static int SCALAR_QUERY_RESULT_ERROR_CODE = -10002;

    public static String UNKOWN_TABLE_NAME = "Table not found ";

    public static int UNKOWN_TABLE_NAME_CODE = -10003;

    public static String INTEGRITY_VIOLATION = "Integrity violation ";

    public static int INTEGRITY_VIOLATION_CODE = -10004;

    public static String UNKOWN_COLUMN_NAME = "Column not found ";

    public static int UNKOWN_COLUMN_NAME_CODE = -10005;

    public static String INVALID_DATATYPE = "Invalid datatype :(DATATYPE [ , EXPRESSION ])  : ";

    public static int INVALID_DATATYPE_CODE = -10006;

    public static String DUP_COLUMN_NAME = "Duplicate Column Name ( COLUMN NAME) :";

    public static int DUP_COLUMN_NAME_CODE = -10007;

    public static String DUP_DATABASE_ERROR = " Duplicate Database (DBNAME) : ";

    public static int DUP_DATABASE_ERROR_CODE = -10008;

    public static String COLUMN_REFFERENCES_EXIST = "The Column(s) are refered by other tables :";

    public static int COLUMN_REFFERENCES_EXIST_CODE = -10009;

    public static String NO_TABLES_FOUND = "NO TABLES FOUND ( TABLENAME) :";

    public static int NO_TABLE_FOUND_CODE = -10010;

    public static String PARSER_ERROR = "PARSER ERROR : Illegal Statement";

    public static int PARSER_ERROR_CODE = -10011;

    public static String COMMAND_NOT_IMPLEMENTED = " COMMAND NOT IMPLEMENTED ";

    public static int COMMAND_NOT_IMPLEMENTED_CODE = -10012;

    public static String AMBIGUOUS_COLUMN_REF = "Ambiguous Column Reference ( Column Name )";

    public static int AMBIGUOUS_COLUMN_REF_CODE = -10013;

    public static String TABLE_ALIAS_FOUND_AMBIGOUS = "TABLE ALIAS  FOUND  BUT AMBIGOUS (Table Alias.Expression ) ";

    public static int TABLE_ALIAS_FOUND_AMBIGOUS_CODE = -10014;

    public static String TABLE_ALIAS_NOT_FOUND_FROM_CLAUSE = "TABLE ALIAS NOT FOUND ( Table Alias) ";

    public static int TABLE_ALIAS_NOT_FOUND_FROM_CLAUSE_CODE = -10015;

    public static String ALIAS_COUNT_UNEQUAL_SUBQUERY_PROJ = "Alias Count Should Be Equal To "
            + "The Projected Elements Of The Subquery ";

    public static int ALIAS_COUNT_UNEQUAL_SUBQUERY_PROJ_CODE = -10016;

    public static String XDB_CREATED_INDEX = "XDB Created Sys Index (Index Name) :";

    public static int XDB_CREATED_INDEX_CODE = -10018;

    public static String INDEX_REF_IN_CONSTRAINT = "Index Is Referenced In Constraint ( Index Referenced) :";

    public static int INDEX_REF_IN_CONSTRAINT_CODE = -10019;

    public static String NO_SUCH_INDEX = "No Such Index ( Index Searched) :";

    public static int NO_SUCH_INDEX_CODE = -10020;

    public static String COLUMN_PRECISION_SPECIFIED_NOT_NUMERIC = "Column PRECISION Is Not Numeric (ColumnName) : ";

    public static int COLUMN_PRECISION_SPECIFIED_NOT_NUMERIC_CODE = -10021;

    public static String COLUMN_REFERENCES_MULTIPLE_COLUMNS = "Column References Multiple Columns (COLUMN NAME) :";

    public static int COLUMN_REFERENCES_MULTIPLE_COLUMNS_CODE = -10022;

    public static String INVALID_COLUMN_NAME = "INVALID COLUMN NAME (COLUMN NAME) : ";

    public static int INVALID_COLUMN_NAME_CODE = -10023;

    public static String CASE_STATEMENT_TYPE_MISMATCH = "Case Statement Expression Type MISMATCH "
            + "Between ( Expr1 , Expr2) ";

    public static int CASE_STATEMENT_TYPE_MISMATCH_CODE = -10024;

    public static String PRIMARYKEY_DEFINED_IN_COLDEF = "The Primary Key Of The Table is already defined"
            + "in the COLUMN DEFINITION : (ColumnName)";

    public static int PRIMARYKEY_DEFINED_IN_COLDEF_CODE = -10025;

    public static String PRIMARYKEY_REFRENCED = "The Primary Key Of The Table is Referenced by another Table "
            + "( TableName ) : ";

    public static int PRIMARYKEY_REFRENCED_CODE = -10026;

    public static String PRIMARYKEY_COLUMN_OF_TABLE = "The Column Is The Primary Key Of The Table  "
            + "(Primary Key Column, TableName) : ";

    public static int PRIMARYKEY_COLUMN_OF_TABLE_CODE = -10027;

    public static String PRIMARYKEY_COL_CANNOT_HAVE_NULL = "The Column Is/Part Of The Primary Key Of The Table  "
            + "and cannot take null value : ";

    public static int PRIMARYKEY_COL_CANNOT_HAVE_NULL_CODE = -10027;

    public static String PARTITIONING_COLUMN_OF_TABLE = "The Column Is The Partitioning Key Of The Table  "
            + "(Partitioing Key Column, TableName) : ";

    public static int PARTITIONING_COLUMN_OF_TABLE_CODE = -10028;

    public static String PRIMARY_INDEX_ALREADY_PRESENT = "Table  Already Has A  PrimaryKey Index (TableName) ";

    public static int PRIMARY_INDEX_ALREADY_PRESENT_CODE = -10029;

    public static String NO_PRIMARY_UNQIUE_INDEX = "Table Does Not Have Any Primary Or Unique Index (Tablename) :  ";

    public static int NO_PRIMARY_UNQIUE_INDEX_CODE = -10030;

    public static String COLUMN_NOT_IN_TABLE = "COLUMN NOT FOUND IN TABLE (COLUMN , TABLE) : ";

    public static int COLUMN_NOT_IN_TABLE_CODE = -10031;

    public static String INVALID_SQL_COMMAND = "SQL COMMAND ILLEGAL (SQL) : ";

    public static int INVALID_SQL_COMMAND_CODE = -10032;

    public static String COLUMN_LENGTH_SPECIFIED_NOT_NUMERIC = "Parameter Column Length Specifed Is Not Numeric (COLUMN NAME): ";

    public static int COLUMN_LENGTH_SPECIFIED_NOT_NUMERIC_CODE = -10033;

    public static String COLUMN_SCALE_SPECIFIED_NOT_NUMERIC = "Parameter Column Scale Specifed Is Not Numeric (COLUMN NAME) : ";

    public static int COLUMN_SCALE_SPECIFIED_NOT_NUMERIC_CODE = -10034;

    public static String ORDERBY_CLAUSE_POINTS_TO_ILLEGAL_PROJ_COLUMN = "Order By Clause Points To A Illegal Projection Column (Column Number) :";

    public static int ORDERBY_CLAUSE_POINTS_TO_ILLEGAL_PROJ_COLUMN_CODE = -10035;

    public static String EXPRESSION_NOT_ALPHANUMERIC = "EXPRESSION NOT ALPHA NUMERIC (EXPR, [FUNCTION]): ";

    public static int EXPRESSION_NOT_ALPHANUMERIC_CODE = -10036;

    public static String EXPRESSION_NOT_TIME = "EXPRESSION NOT  TIME (EXPR): ";

    public static int EXPRESSION_NOT_TIME_CODE = -10037;

    public static String EXPRESSION_NOT_DATE = "EXPRESSION NOT DATE  (EXPR): ";

    public static int EXPRESSION_NOT_DATE_CODE = -10038;

    public static String EXPRESSION_NOT_DATE_TIMESTAMP = "EXPRESSION NOT DATE OR TIMESTAMP (EXPR): ";

    public static int EXPRESSION_NOT_DATE_TIMESTAMP_CODE = -10039;

    public static String EXPRESSION_NOT_NUMERIC = "EXPRESSION NOT NUMERIC (EXPR): ";

    public static int EXPRESSION_NOT_NUMERIC_CODE = -10040;

    public static String EXPRESSION_NOT_TIME_TIMESTAMP = "EXPRESSION NOT TIME OR TIMESTAMP (EXPR): ";

    public static int EXPRESSION_NOT_TIME_TIMESTAMP_CODE = -10041;

    public static String INVALID_EXPRESSION = "INVALID EXPRESSION : (LEFT EXPR ,OPERATOR,RIGHT EXPR)";

    public static int INVALID_EXPRESSION_CODE = -10042;

    public static String NODEINFO_CORRUPT = "The Specifed Node Not Registered With XDB For Table (TABLE,NODEID) :";

    public static int NODEINFO_CORRUPT_CODE = -10043;

    public static String SEMANTIC_CHECK_FAILED = " SEMANTIC CHECK FAILED ";

    public static int SEMANTIC_CHECK_FAILED_CODE = -10044;

    public static String NODE_NOT_REGISTERED = " The Node is not registered with XBD (NODEID):  ";

    public static int NODE_NOT_REGISTERED_CODE = -10045;

    public static String BAD_ARG_ERROR = " Bad Arguments Error ";

    public static int BAD_ARG_ERROR_CODE = -10046;

    public static String TABLE_NOT_FOUND = " Bad Arguments Error ";

    public static int TABLE_NOT_FOUND_CODE = -10046;

    public static String COMMAND_USAGE_ERROR = "COMMAND USAGE Error (USAGE): ";

    public static int COMMAND_USAGE_ERROR_CODE = -10047;

    public static String COLUMN_MUST_PRIMARY_KEY_OR_UNIQUE_INDEX = "COLUMN MUST BE PRIMARY KEY OR UNIQUE_INDEX ";

    public static int COLUMN_MUST_PRIMARY_KEY_OR_UNIQUE_INDEX_CODE = -10048;

    public static String KEY_COUNT_NOT_EQUAL = "Key Count is not equal to the keys specified in the ForeginKey";

    public static int KEY_COUNT_NOT_EQUAL_CODE = -10049;

    public static String DATA_TYPE_MISMATCH = "Columns should have matching data types (COLUMN_LOCAL, COLUMN_FOREIGN): ";

    public static int DATA_TYPE_MISMATCH_CODE = -10050;

    public static String COLUMN_NAME_CANNOT_EXCEED_255 = "COLUMN_NAME_CANNOT_EXCEED_255 (Column Name):";

    public static int COLUMN_NAME_CANNOT_EXCEED_255_CODE = -10051;

    public static String PARTITION_COLUMN_NOT_FOUND_IN_COLUMNLIST = "PARTITION_COLUMN_NOT_FOUND_IN_COLUMNLIST (Column Name)";

    public static int PARTITION_COLUMN_NOT_FOUND_IN_COLUMNLIST_CODE = -10052;

    public static String TABLE_NAME_CANNOT_EXCEED_255 = "TABLE_NAME_CANNOT_EXCEED_255 (Table Name):";

    public static int TABLE_NAME_CANNOT_EXCEED_255_CODE = -10053;

    public static String NO_INDEX_FOUND = "INDEX NOT FOUND (INDEX NAME) :";

    public static int NO_INDEX_FOUND_CODE = -10054;

    public static String EXPRESSION_NOT_MACADDR = "EXPRESSION NOT MACADDR (EXPR): ";

    public static int EXPRESSION_NOT_MACADDR_CODE = -10055;

    public static String EXPRESSION_NOT_NUMERIC_MACADDR = "EXPRESSION NOT NUMERIC OR MACADDR (EXPR): ";

    public static int EXPRESSION_NOT_NUMERIC_MACADDR_CODE = -10056;    
    
    public static String EXPRESSION_NOT_CIDR = "EXPRESSION NOT CIDR (EXPR): ";

    public static int EXPRESSION_NOT_CIDR_CODE = -10057;

    public static String EXPRESSION_NOT_INET = "EXPRESSION NOT INET (EXPR): ";

    public static int EXPRESSION_NOT_INET_CODE = -10058;
    
    public static String DECLARE_CURSOR_TRANS = "DECLARE CURSOR can only be used in transaction blocks";

    public static int DECLARE_CURSOR_TRANS_CODE = -10059;

    /***************************************************************************
     * System Failures
     */

    // System Failures -2
    public static String NODE_DOWN = "Could not connect to node ( Node URL ) :";

    public static int NODE_DOWN_CODE = -20001;

    public static String CANNOT_INITIALIZE_NODE_CONNECTION = "CANNOT INITIALIZE NODE CONNECTION  ( Node URL,[ User Name], [Password ] ) :";

    public static int CANNOT_INITIALIZE_NODE_CONNECTION_CODE = -20002;

    public static String CONNECTION_TO_NODE_LOST = "CONNECTION TO NODE LOST  ( Node URL ) :";

    public static int CONNECTION_TO_NODE_LOST_CODE = -20003;

    public static String SQL_STATEMENT_CREATE_FAILURE = "Could Not Create a Statement Object For :";

    public static int SQL_STATEMENT_CREATE_FAILURE_CODE = -20004;

    //public static String SQL_EXEC_FAILURE = "Failed To Get Results For ( SQL , NodeURL)  : ";

    public static int SQL_EXEC_FAILURE_CODE = -20005;

    public static String SQL_BEGIN_TRANSACTION_FAILURE = "Failure To Start Transaction on all nodes ";

    public static int SQL_BEGIN_TRANSACTION_FAILURE_CODE = -20006;

    public static String SQL_TARGET_TABLE_FAILURE = "Error creating target schema: ";

    public static int SQL_TARGET_TABLE_FAILURE_CODE = -20007;

    public static String INSERT_FAILED_ON_NODE = "Insert Failed : (NODEID,INSERTSTRING) ";

    public static int INSERT_FAILED_ON_NODE_CODE = -20008;

    public static String SQL_ROLLBACK_ERROR = "Could not rollback  : (NodeURL ) ";

    public static int SQL_ROLLBACK_ERROR_CODE = -20009;

    public static String SQL_COMMIT_ERROR = "Could not commit changes : (NodeURL ) ";

    public static int SQL_COMMIT_ERROR_CODE = -20010;

    public static String METADATA_DB_INFO_READ_ERROR = "Metadata information read error ( Node URL ) ";

    public static int METADATA_DB_INFO_READ_ERROR_CODE = -20011;

    // Configuration File Errors
    public static String MALFORMED_PROPERTY = "THE PROPERTY MALFORMED IN CONFIG FILE ( PROPERTY ) : ";

    public static int MALFORMED_PROPERTY_CODE = -20012;

    public static String DB_NOT_REGISTERED = "The Database is not reigistered with XDB (DBNAME) : ";

    public static int DB_NOT_REGISTERED_CODE = -20013;

    public static String XDB_CONFIG_FILE_FAILURE = "Could Not Initialize From xdb.config : ";

    public static int XDB_CONFIG_FILE_FAILURE_CODE = -20015;

    public static String XDB_CONFIG_FILE_ILLEGAL_NODEINFO = "Illegal Node Number in xdb.config : ";

    public static int XDB_CONFIG_FILE_ILEGAL_NODEIFO_CODE = -20016;

    public static String XDB_CONFIG_FILE_ILLEGAL_PARAMETER = "Illegal Parameter Information ( Parameter , Value)*  :";

    public static int XDB_CONFIG_FILE_ILLEGAL_PARAMETER_CODE = -20017;

    public static String ADD_BATCH_FAILED_ON_NODE = "Adding to Batch Failed : (NODEID,COMMAND) ";

    public static int ADD_BATCH_FAILED_ON_NODE_CODE = -20018;

    public static String EXEC_BATCH_FAILED_ON_NODE = "Executing Batch Failed : (NODEID) ";

    public static int EXEC_BATCH_FAILED_ON_NODE_CODE = -20019;

    public static String SQL_BEGIN_TRANSACTION_SAVEPOINT_FAILURE = "Failure To Start Transaction Savepoint on all nodes ";

    public static int SQL_BEGIN_TRANSACTION_SAVEPOINT_FAILURE_CODE = -20020;

    public static String SQL_ROLLBACK_SAVEPOINT_ERROR = "Could not rollback  : (NodeURL ) ";

    public static int SQL_ROLLBACK_SAVEPOINT_ERROR_CODE = -20021;

    public static String SQL_DDL_IN_TRANSACTION = "No Data Definition Statements allowed in a transaction";

    public static int SQL_DDL_IN_TRANSACTION_CODE = -20022;

    /***************************************************************************
     * 
     * XDB Failures
     */

    // XDB Failure -3
    public static String UPDATE_STAT_TABLE_LIST_EMPTY = "The Update Statistics Failed : No Tables Specified";

    public static int UPDATE_STAT_TABLE_LIST_EMPTY_CODE = -30001;

    public static String ERROR_GETTING_TYPE_INFO = "Unable To Determine Type For (SQLEXPRESSION):";

    public static int ERROR_GETTING_TYPE_INFO_CODE = -30002;

    public static String SUBQUERY_MANAGE_ERROR = "Unable to Manage form SubQuery";

    public static int SUBQUERY_MANAGE_ERROR_CODE = -30003;

    public static String NULL_COLUMN_NAME = "Column Name Became NULL - Code bug";

    public static int NULL_COLUMN_NAME_CODE = -30004;

    public static String QUERY_PROJ_LIST_EMPTY = "Projection List Empty,Corrput Code ";

    public static int QUERY_PROJ_LIST_EMPTY_CODE = -30005;

    public static String IMBALANCE_IN_EXPRESSION_STACK = "STACK OF EXPRESSION CORRUPT";

    public static int IMBALANCE_IN_EXPRESSION_STACK_CODE = -30006;

    public static String ILLEGAL_FUNCTION_CALL = "The Precondition for calling this function were not met (FUNCTIONNAME)";

    public static int ILLEGAL_FUNCTION_CALL_CODE = -30007;

    public static String SQL_EXEC_STEPS = "Error Executing Steps";

    public static int SQL_EXEC_STEPS_CODE = -30008;

    public static String ERROR_RECIEVING_MESSAGE = "Error In  Recieving Message";

    public static int ERROR_RECIEVING_MESSAGE_CODE = -30009;

    public static String ILLEGAL_HASH_VALUE = "Could not determine hash value for";

    public static int ILLEGAL_HASH_VALUE_CODE = -30010;

    public static String ERROR_PASSING_MESSAGE = "Error Passing Message:";

    public static int ERROR_PASSING_MESSAGE_CODE = -30011;

    public static String ERROR_WAITING_FOR_THREAD = "Error waiting for producer thread";

    public static int ERROR_WAITING_FOR_THREAD_CODE = -30012;

    public static String THREAD_INTERRUPTED = "Thread Interrupted";

    public static int THREAD_INTERRUPTED_CODE = -30013;

    public static String ERROR_SEND_RESULT = "Error in sending the results (NodeURL) : ";

    public static int ERROR_SEND_RESULT_CODE = -30014;

    public static String INVALID_MESSAGE_TYPE = "Message Type Is Invalid  In State (GOT, STATE)";

    public static int INVALID_MESSAGE_TYPE_CODE = -30015;

    public static String INSERT_FAILED = "Insert Failed : ";

    public static int INSERT_FAILED_CODE = -30016;

    public static String UNEXPECTED_MESSAGE_RECIEVED = "UNEXPECTED_MESSAGE_RECIEVED (GOT, EXPECTED) :";

    public static int UNEXPECTED_MESSAGE_RECIEVED_CODE = -30017;

    public static String ERROR_ACK_LOST = "Could not send/retrieve an ACK ";

    public static int ERROR_ACK_LOST_CODE = -30018;

    public static String ERROR_COORD_ACK_LOST = "Error while coordinator waiting for acknowledgement";

    public static int ERROR_COORD_ACK_LOST_CODE = -30019;

    public static String TABLE_DEF_CORRUPTED = "Table Definition corrupted (TableName)";

    public static int TABLE_DEF_CORRUPTED_CODE = -30020;

    public static String SUBPLAN_ASSOC_ERROR = " Error: could not associate subplan";

    public static int SUBPLAN_ASSOC_ERROR_CODE = -30021;

    public static String ERROR_EXEC_STEP = " Error: executing steps";

    public static int ERROR_EXEC_STEP_CODE = -30022;

    public static String SUBQUERY_RESULT_EMPTY = "SUBQUERY  YEILDS NO RESULTS (SUBQUERY) : ";

    public static int SUBQUERY_RESULT_EMPTY_CODE = -30022;

    public static String LOOKUP_ERROR = " Error: could not find selection (LOCATION, OBJECT)";

    public static int LOOKUP_ERROR_CODE = -30022;

    public static String INVALID_JOIN = "Invalid join";

    public static int INVALID_JOIN_CODE = -30022;

    public static String CORRELATED_NODE_LOST = "Error: could not find correlated node.";

    public static int CORRELATED_NODE_LOST_CODE = -30022;

    public static String ILLEGAL_PARAMETER = "Illegal Parameter";

    public static int ILLEGAL_PARAMETER_CODE = -30022;

    public static String EXPRESSION_TYPE_UNDETERMINED = "Cannot Determine The Expression For (EXPRESSION)";

    public static int EXPRESSION_TYPE_UNDETERMINED_CODE = -30023;

    public static String ILLEGAL_RELATIONNODE_TYPE = "Unknown Relation Node Type";

    public static int ILLEGAL_RELATIONNODE_TYPE_CODE = -30024;

    public static String REFERENCE_NODE_NOTFOUND = "Could not find referencing node ";

    public static int REFERENCE_NODE_NOTFOUND_CODE = -30025;

    public static String CORELATED_QUERY_ERROR = "Error Processing CO RELATED QUERY";

    public static int CORELATED_QUERY_ERROR_CODE = -30026;

    public static String MOVINGCONDITION_NOTFOUND = "moving condition not found";

    public static int MOVINGCONDITION_NOTFOUND_CODE = -30027;

    public static String NOPARENTNODE_FOUND = "Error: no parent node.";

    public static int NOPARENTNODE_FOUND_CODE = -30028;

    public static String ILLEGAL_COLUMN_TYPE = "Illegal Column Type (COLUMNNAME) : ";

    public static int ILLEGAL_COLUMN_TYPE_CODE = -30029;

    public static String TABLE_COLUMN_NOT_FILLED = "The TableName or ColumnName for"
            + " this column attribute are not "
            + "completely specified- Therefore we cannot find the columnType";

    public static int TABLE_COLUMN_NOT_FILLED_CODE = -30030;

    public static String METADATA_LOCKED = "MetaData Shared Object Locked :";

    public static int METADATA_LOCKED_CODE = -30031;

    public static String SYSINDEX_CORRUPT = "IllegalValue for Index key (INDEXID) : ";

    public static int SYSINDEX_CORRUPT_CODE = -30032;

    public static String SYSREFKEYS_CORRUPT = "IllegalValue for Ref key (REFID) : ";

    public static int SYSREFKEYS_CORRUPT_CODE = -30034;

    public static String SYSINDEX_READ_FAILURE = "The Sys Index Value Could Not Be Retrived (IDXNAME/IDXID) : ";

    public static int SYSINDEX_READ_FAILURE_CODE = -30035;

    public static String REFERENCE_FAILURE = "Error obtaining database Foreign key information for (REFID) :";

    public static int REFERENCE_FAILURE_CODE = -30036;

    public static String PARTITIONINFO_CORRUPT = "The Partition Information Corrupted (TableName) :";

    public static int PARTITIONINFO_CORRUPT_CODE = -30037;

    // Programming Error
    public static String NODE_INFORMATION_LOST_FROM_NODE_TABLE = "The Node Information Is Lost : (Node ID)";

    public static int NODE_INFORMATION_LOST_FROM_NODE_TABLE_CODE = -30038;

    public static String UNEXPECTED_TOKEN = "A Unexpected Token:";

    public static int UNEXPECTED_TOKEN_CODE = -30039;

    public static String DELETE_FAIELD_FROM_METADATA = "Delete Fails From MetaData (TABLE NAME)";

    public static int DELETE_FAIELD_FROM_METADATA_CODE = -30040;

    public static String CONNECTION_OBJECT_NULL = "Connection Object is NULL";

    public static int CONNECTION_OBJECT_NULL_CODE = -30041;

    public static String ILLEGAL_EXPRESSION_OBJECT_NULL = "The argument sent in should be of type function";

    public static int ILLEGAL_EXPRESSION_OBJECT_NULL_CODE = -30042;

    /**
     * The messages below are not used at present but might be used later
     */
    public static String DUP_TABLE_NAME = "Duplicate Table Name :";

    public static int DUP_TABLE_NAME_CODE = -14;

    public static String DUP_NAME = "Duplicate Name :";

    public static int DUP_NAME_CODE = -16;

    public static String DUP_INDEX_NAME = "Duplicate index name :";

    public static int DUP_INDEX_NAME_CODE = -17;

    public static String DUP_REF_NAME = "Duplicate reference name:";

    public static int DUP_REF_NAME_CODE = -18;

    public static String FK_KEY_MUST_EXIST = "Foreign Key Must Exist :";

    public static int FK_KEY_MUST_EXIST_CODE = -20;

    public static String METADATA_LOAD_FAILURE = "CANNOT LOAD META DATA BECAUSE :  ";

    public static int META_DATA_LOAD_FAILURE = -24;

    public static String ERROR_CREATING_EXEC_STR = "Error Building EXEC String (STRING BUILT) : ";

    public static int ERROR_CREATING_EXEC_STR_CODE = -70004;

    public static String METADATA_INDEX_ENTRY_LOST = "Index Entry Missing From Metadata : ";

    public static int METADATA_INDEX_ENTRY_LOST_CODE = -70004;

    public static String DUP_KEY_INDEX = " Duplicate key in index ";

    public static int DUP_KEY_INDEX_CODE = -10003;

    public static String DROP_DB_FAILED = "DROP DATABASE FAILED :";

    public static int DROP_DB_FAILED_CODE = -1;

    public static String EXEC_FAILED_PARSE_AGAIN = " Execution failed, parse again ";

    public static int EXEC_FAILED_PARSE_AGAIN_CODE = -2;

    public static String DUP_KEY = " Duplicate key ";

    public static int DUP_KEY_CODE = (-3);

    public static String DUPLICATE_SECONDARY_KEY = " Duplicate secondary key ";

    public static int DUP_SECONDARY_KEY_CODE = (-4);

    public static String REF_INTEGRITY_VIOLATION = " Referential integrity violated ";

    public static int REF_INTEGRITY_VIOLATION_CODE = (-6);

    public static String FOREIGN_KEY_INTEGRITY_VIOLATION = " Foreignkey integrity violated ";

    public static int FOREIGN_KEY_INTEGRITY_VIOLATION_CODE = (-7);

    public static String TO_COMPLICATED_SQL_STATEMENT = "SQL Statement Too Complicated ";

    public static int TO_COMPLICATED_SQL_STATEMENT_CODE = (-9);

    public static String INVALID_SQL_STMT = "Invalid SQL statement : ";

    public static int INVALID_SQL_STMT_CODE = (-11);

    public static String INV_SESSION_TIMEOUT = "Session time out ";

    public static int INV_SESSION_TIMEOUT_CODE = (-12);

    public static String INV_MIX_OF_FUNC_PARAM = "Invalid mixture of functions and columns";

    public static int INV_MIX_OF_FUNC_PARAM_CODE = -13;

}
