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

import java.math.BigDecimal;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Vector;

import org.postgresql.stado.common.util.ParseCmdLine;
import org.postgresql.stado.common.util.Property;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.ColumnNotFoundException;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.parser.Command;
import org.postgresql.stado.parser.ExprTypeHelper;
import org.postgresql.stado.parser.ExpressionType;
import org.postgresql.stado.parser.handler.CastTemplates;
import org.postgresql.stado.parser.handler.DataTypeHandler;
import org.postgresql.stado.parser.handler.IFunctionID;
import org.postgresql.stado.parser.handler.IdentifierHandler;
import org.postgresql.stado.parser.handler.SQLExpressionHandler;

/**
 *
 * SqlExpression holds SQL expression elements, whether a column, constant, or
 * compound expression. This should be changed in the future with a base
 * SqlExpression class and inherited subclasses based on the particular
 * expression type.
 *
 * For leftExpr and rightExpr, it is intended to be processed left-depth first.
 *
 */
public class SqlExpression implements IRebuildString {
	private static final XLogger logger = XLogger
			.getLogger(SqlExpression.class);

	/**
	 * for time and timestamps, precision for subseconds. Ingres does not
	 * support subseconds.
	 */
	private static final String subsecondBaseString = "000000000";

	private static final int CONSTANT_DOUBLE_SCALE_VALUE = 10;

	private static final int CONSTANT_PRECISION_DOUBLE_VALUE = 32;

	// Type Mapping constants (see mappedExpression)
	public static final int INTERNALMAPPING = 1;

	public static final int EXTERNALMAPPING = 2;

	public static final int ORIGINAL = 3;

	// enumerate types here
	public static final int SQLEX_CONSTANT = 2;

	public static final int SQLEX_COLUMN = 4;

	public static final int SQLEX_UNARY_EXPRESSION = 8;

	public static final int SQLEX_OPERATOR_EXPRESSION = 16;

	public static final int SQLEX_FUNCTION = 32;

	public static final int SQLEX_SUBQUERY = 64;

	public static final int SQLEX_CONDITION = 128;

	public static final int SQLEX_CASE = 256;

	public static final int SQLEX_LIST = 512;

	public static final int SQLEX_PARAMETER = 1024;

	public static final int SQLEX_COLUMNLIST = 512;

	// To get all SqlExpression
	public static final int SQLEX_ALL = SQLEX_CONSTANT | SQLEX_COLUMN
			| SQLEX_UNARY_EXPRESSION | SQLEX_OPERATOR_EXPRESSION
			| SQLEX_FUNCTION | SQLEX_CONDITION | SQLEX_CASE | SQLEX_COLUMNLIST
			| SQLEX_LIST;

        private boolean isProjection = false;  /* set if in SELECT list */
        
	/** Which SQLEX_* type this is */
	private int exprType;

	private ExpressionType exprDataType;

	/** String representation of expression */
	private String exprString = "";

	/** This is set after the expression have been produced */
	private QueryTree belongsToTree;

	/**
	 * This is used in case the top level expression is aliased for purposes in
	 * subquerying, or display
	 */
	private String alias = "";

	/** Another member added here for help with handling aggregates */
	private String aggAlias = "";

	/** Additional alias used in outer handling */
	private String outerAlias = "";

	/** This is for labelling in the final ResultSet */
	private String projectionLabel = null;

	/** For constants */
	private String constantValue = "";

	/** For columns */
	private AttributeColumn column;

	/** For complex expressions */
	private SqlExpression leftExpr;

	private SqlExpression rightExpr;

	/** can be NOT */
	private String unaryOperator = "";

	/**
	 * This is for support of having query conditions inside sql expression
	 * allowing for a*b + ( A ==B ) there fore if A = 1, B= 1 and a = 1 , b =1
	 * The resultant must be 3.
	 */
	private QueryCondition aQueryCondition = null;

	/**
	 * this is required in context of !!, ||/, |/ and @ unary operators where
	 * operand may have sign e.g. !! -4 or !!(-4)
	 */
	private String operandSign = "";

	/** +, *, -, etc. */
	private String operator = "";

	/** For mapped expression - reference to another SqlExpression */
	private int mapped = ORIGINAL;

	private SqlExpression mappedExpression = null;

	// For prepared statements
	/** track the parameter number */
	private int paramNumber;

	/** holds PreparedStatement parameter value */
	private String paramValue = null;

	/** For functions */
	private int functionId;

	/** Function name */
	private String functionName = "";

	/** Parameters that this function uses */
	private List<SqlExpression> functionParams = new ArrayList<SqlExpression>();

	private String argSeparator = ", ";

	private boolean needParenthesisInFunction = true;

	// For CAST
	private DataTypeHandler expTypeOfCast;

	// For aggregate functions
	/**
	 * if this expression is a distinct group, like count(distinct x)
	 */
	private boolean isDistinctGroupFunction = false;

	/**
	 * If we need to break this aggregate function into two steps where we just
	 * get column info on the first
	 */
	private boolean isDeferredGroup = false;

	private boolean isAllCountGroupFunction = false;

	/** Used to help with SELECT COUNT (DISTINCT x) ... */
	private boolean isDistinctExtraGroup = false;

	/** For CASE */
	private SCase caseConstruct = new SCase();

	/** For subquery */
	private QueryTree subqueryTree;

	// Misc
	// Do not rebuild expression string if this flag is set
	private boolean isTemporaryExpression = false;

	/**
	 * This variable is only valid when we have this expression as a subtree All
	 * subtrees are contained in a relation node as they represent a relation
	 */
	private RelationNode parentContainerNode;

	/**
	 * This is added here for convenience instead of creating a wrapper class
	 * for projection list items. It is used to indicate that the item was not
	 * originally in the list, but added because it appears in the order by or
	 * having clause
	 */
	private boolean isAdded = false;

	/**
	 * This vector was added to provide support for the list of elements for
	 * e.g. the list (1,2,3) - This is also an expression but of a diffrent
	 * nature we cannot do operation on this SQLExpression
	 */
	private List<SqlExpression> expressionList = new ArrayList<SqlExpression>();

	/**
	 *
	 *
	 *
	 * @return
	 *
	 */

	public QueryTree getBelongsToTree() {
		return belongsToTree;
	}

	/**
	 *
	 *
	 *
	 * @param belongsToTree
	 *
	 */

	public void setBelongsToTree(QueryTree belongsToTree) {
		this.belongsToTree = belongsToTree;
		// Also get the columns in this SQL Expression and set them to
		// this tree.
		if (this.subqueryTree != null) {
			// In this case we dont do anything
			// as while analyzing the sql expressions we have already
			// set the tree.
		} else {
			for (SqlExpression aSqlExpression : getNodes(this, SQLEX_COLUMN)) {
				aSqlExpression.getColumn().setMyParentTree(belongsToTree);
			}

		}
	}

	/**
	 *
	 * Set the Expression to temporary
	 *
	 * @param isTemp
	 *
	 */

	public void setTempExpr(boolean isTemp) {
		isTemporaryExpression = isTemp;
	}

	/**
	 *
	 *
	 *
	 * @return
	 *
	 */

	public boolean isTempExpr() {
		return isTemporaryExpression;
	}

	/**
	 *
	 *
	 *
	 * @return
	 *
	 * @param sqlexpr
	 *
	 * @param nodetype
	 *
	 */

	public static Vector<SqlExpression> getNodes(SqlExpression sqlexpr,
			int nodetype) {
		return getNodes(sqlexpr, nodetype, null);
	}

	/**
	 * This will return us the list of column nodes which belong to this Sql
	 * Expression
	 *
	 *
	 * @return
	 *
	 * @param sqlexpr
	 *
	 * @param nodetype
	 *
	 * @param hsVisited
	 *
	 */

	private static Vector<SqlExpression> getNodes(SqlExpression sqlexpr,
			int nodetype, HashSet<SqlExpression> hsVisited) {

		if (hsVisited == null) {
			hsVisited = new HashSet<SqlExpression>();
		}

		// See if we have already processed this one
		if (hsVisited.contains(sqlexpr)) {
			// return an empty vector to avoid null exception
			return new Vector<SqlExpression>();
		}

		hsVisited.add(sqlexpr);

		// Get all Nodes of a particular type
		Vector<SqlExpression> columnnodes = new Vector<SqlExpression>();

		SqlExpression sqlr = sqlexpr.rightExpr;
		SqlExpression sqll = sqlexpr.leftExpr;

		/* Get all the nodes from the right */
		if (sqlr != null) {
			columnnodes.addAll(getNodes(sqlr, nodetype, hsVisited));
		}

		/* Get all the nodes from the left */
		if (sqll != null) {
			columnnodes.addAll(getNodes(sqll, nodetype, hsVisited));
		}

		/*
		 * Check if the expression we are evaluation is a tree - in which case
		 * we will assume that this is a sub tree and we now have to return not
		 * the actual expression columns but the projected columns
		 */
		if ((sqlexpr.getExprType() & SQLEX_SUBQUERY) > 0) {

			// We have 4 different types of subqueries
			// a. Scalar - Correlated
			// b. Scalar - non corelated
			// a.NonScalar - Correlated
			// b.NonScalar - Non Correlated
			// The parent container node is the feature of only nonscalar
			// queries
			// since the implementation of the scalar correlated is not yet
			// decided we are
			// not sure how that will be taken care off.

			// Comment End
			// If it is not a scalar query
			if ((sqlexpr.subqueryTree.getQueryType() & QueryTree.SCALAR) == 0
                                        && sqlexpr.parentContainerNode != null) {
				for (SqlExpression aSqlExpression : sqlexpr.parentContainerNode
						.getProjectionList()) {
					if (hsVisited == null
							|| !hsVisited.contains(aSqlExpression)) {
						columnnodes.addAll(getNodes(aSqlExpression, nodetype,
								hsVisited));
					}
				}
			}

			if ((sqlexpr.subqueryTree.getQueryType() & QueryTree.CORRELATED) > 0
					&& sqlexpr.subqueryTree != null) {
				for (QueryCondition aQC : sqlexpr.subqueryTree
						.getConditionList()) {
					for (QueryCondition aQCSE : QueryCondition.getNodes(aQC,
							QueryCondition.QC_SQLEXPR)) {
						columnnodes.addAll(getNodes(aQCSE.getExpr(), nodetype,
								hsVisited));
					}
				}
			}
		}

		/*
		 * Check if the expression we are evaluting is a function -- in this
		 * case we will also explore the Function params for which we are
		 * looking
		 */
		if ((sqlexpr.getExprType() & SQLEX_FUNCTION) > 0) {
			for (SqlExpression aSqlExpression : sqlexpr.functionParams) {
				columnnodes
						.addAll(getNodes(aSqlExpression, nodetype, hsVisited));
			}
		}

		/*
		 * When we have the expression of type CASE
		 */
		if ((sqlexpr.getExprType() & SQLEX_CASE) > 0) {
			// - This should contain the SQL Expressions from both
			// query conditions as well as sql expressions
			for (SqlExpression aSqlExpression : sqlexpr.getCaseConstruct()
					.getSQLExpressions()) {
				columnnodes.addAll(SqlExpression.getNodes(aSqlExpression,
						nodetype, hsVisited));
			}
		}

		/* Check if this node is of type we are expecting */
		if ((sqlexpr.getExprType() & nodetype) > 0) {
			columnnodes.add(sqlexpr);
		}

		return columnnodes;
	}

	/**
	 *
	 * Constructor
	 *
	 * @param aQueryTree
	 *
	 */

	public SqlExpression(QueryTree aQueryTree) {
		this.setExprType(SQLEX_SUBQUERY);
		this.subqueryTree = aQueryTree;
	}

	public SqlExpression() {

	}

	/**
	 * Create constant SqlExpression with specified value and data type
	 *
	 * @param constantValue
	 * @param dataType
	 */
	public SqlExpression(String constantValue, ExpressionType dataType) {
		exprType = SQLEX_CONSTANT;
		this.constantValue = constantValue;
		exprString = constantValue;
		exprDataType = dataType;
	}

	/**
	 * Copy a SqlExpression to another. Note that it just copies references.
	 *
	 *
	 * @param orignal
	 *
	 * @param overwrite
	 *
	 * @return
	 *
	 */

	public static SqlExpression copy(SqlExpression orignal,
			SqlExpression overwrite) {

		overwrite.exprType = orignal.exprType;
		overwrite.exprString = orignal.exprString;
		overwrite.unaryOperator = orignal.unaryOperator;
		overwrite.operator = orignal.operator;
		overwrite.operandSign = orignal.operandSign;
		overwrite.constantValue = orignal.constantValue;
		overwrite.column = orignal.column;
		overwrite.functionId = orignal.functionId;
		overwrite.functionName = orignal.functionName;
		overwrite.functionParams = orignal.functionParams;
		overwrite.expTypeOfCast = orignal.expTypeOfCast;
		overwrite.needParenthesisInFunction = orignal.needParenthesisInFunction;
		overwrite.argSeparator = orignal.argSeparator;
		overwrite.alias = orignal.alias;
		overwrite.outerAlias = orignal.outerAlias;
		overwrite.leftExpr = orignal.leftExpr;
		overwrite.rightExpr = orignal.rightExpr;
		overwrite.exprDataType = orignal.exprDataType;
		overwrite.aQueryCondition = orignal.aQueryCondition;
		overwrite.isDistinctGroupFunction = orignal.isDistinctGroupFunction;
		overwrite.isDeferredGroup = orignal.isDeferredGroup;
		overwrite.isAllCountGroupFunction = orignal.isAllCountGroupFunction;
		overwrite.caseConstruct = orignal.caseConstruct;
		overwrite.subqueryTree = orignal.subqueryTree;
		overwrite.projectionLabel = orignal.projectionLabel;
		overwrite.paramNumber = orignal.paramNumber;
		overwrite.paramValue = orignal.paramValue;
		return overwrite;
	}

	/**
	 * Returns a copy of this SqlExpression
	 *
	 * @return
	 *
	 */

	public SqlExpression copy() {
		return copy(this, new SqlExpression());
	}

	// Rebuild exprString
	public void rebuildExpression() {
		rebuildExpression(null);
	}

	// Rebuild exprString
	private void rebuildExpression(XDBSessionContext client) {
		//
		if (unaryOperator.equals("+")) {
			unaryOperator = "";
			this.exprString = rebuildExpression(this, client);
		} else if (unaryOperator.equals("")) {
			this.exprString = rebuildExpression(this, client);
		} else if (this.getExprType() == SQLEX_PARAMETER) {
			rebuildExpression(this, client);
		} else if (unaryOperator.equals("|/")) {
			this.exprString = "|/ " + operandSign
					+ rebuildExpression(this, client);
		} else if (unaryOperator.equals("||/")) {
			this.exprString = "||/ " + operandSign
					+ rebuildExpression(this, client);
		} else if (unaryOperator.equals("!")) {
			this.exprString = operandSign + rebuildExpression(this, client)
					+ " !";
		} else if (unaryOperator.equals("!!")) {
			this.exprString = "!! " + operandSign
					+ rebuildExpression(this, client);
		} else if (unaryOperator.equals("@")) {
			this.exprString = "@ " + operandSign
					+ rebuildExpression(this, client);
		} else if (unaryOperator.equals("~")) {
			this.exprString = "~ " + rebuildExpression(this, client);
		} else {
			this.exprString = "-" + rebuildExpression(this, client);
		}
	}

	/**
	 * Use recursion to get all of the elements
	 *
	 *
	 * @param aSqlExpression
	 *
	 * @return
	 *
	 */

	private static String rebuildExpression(SqlExpression aSqlExpression,
			XDBSessionContext client) {
		String rightExprString = "";
		String leftExprString = "";
		String newExprString = "";
		AttributeColumn anAC;

		if (aSqlExpression.isTemporaryExpression == false) {

			if (aSqlExpression.getExprType() != SQLEX_OPERATOR_EXPRESSION) {
				if (aSqlExpression.getExprType() == SQLEX_COLUMN) {
					anAC = aSqlExpression.getColumn();

					if (anAC.relationNode != null) {
						if (anAC.relationNode.getCurrentTempTableName()
								.length() > 0) {

							newExprString = IdentifierHandler
									.quote(anAC.relationNode
											.getCurrentTempTableName());
						} else if (!anAC.relationNode.getAlias().equals("")) {
							newExprString = IdentifierHandler
									.quote(anAC.relationNode.getAlias());
						} else {
							if (anAC.getTableAlias().length() > 0) {
								newExprString = IdentifierHandler.quote(anAC
										.getTableAlias());
							} else {
								newExprString = IdentifierHandler.quote(anAC
										.getTableName());
							}
						}
					} else {
						if (anAC.getTableAlias() != null
								&& anAC.getTableAlias().length() > 0) {
							newExprString = IdentifierHandler.quote(anAC
									.getTableAlias());
						}
					}

					// Use tempColumnAlias, if assigned
					if (aSqlExpression.getColumn().tempColumnAlias.length() > 0) {
						newExprString += "."
								+ IdentifierHandler.quote(aSqlExpression
										.getColumn().tempColumnAlias);
					} else if (aSqlExpression.getColumn().columnAlias != null
							&& aSqlExpression.getColumn().columnAlias.length() > 0) {

						if (aSqlExpression.getColumn().getTableName().length() > 0) {
							newExprString += "."
									+ IdentifierHandler.quote(aSqlExpression
											.getColumn().columnAlias);
						} else {
                                                        if (newExprString.length() > 0) {
                                                            newExprString += ".";
                                                        }
							newExprString += IdentifierHandler
									.quote(aSqlExpression.getColumn().columnAlias);
						}
					} else {
						// If we are using a temp table and the expression has
						// been aliased, use the alias
						/*
						 * if (aSqlExpression.column.relationNode != null &&
						 * aSqlExpression
						 * .column.relationNode.currentTempTableName.length() >
						 * 0 && aSqlExpression.alias != null &&
						 * aSqlExpression.alias.length() > 0) { newExprString +=
						 * "." + aSqlExpression.alias; } else
						 */
						if (aSqlExpression.getColumn().getTableName().length() > 0) {
							newExprString += "."
									+ IdentifierHandler.quote(aSqlExpression
											.getColumn().columnName);
						} else {
							if (newExprString == null
									|| newExprString.length() == 0) {
								newExprString += IdentifierHandler
										.quote(aSqlExpression.getColumn().columnName);
							} else {
								newExprString += "."
										+ IdentifierHandler
												.quote(aSqlExpression
														.getColumn().columnName);
							}
						}
					}
				} else if (aSqlExpression.getExprType() == SQLEX_FUNCTION
						&& aSqlExpression.isTemporaryExpression == false) {
					newExprString = Property.get("xdb.sqlfunction."
							+ aSqlExpression.functionName + ".template");
					if (newExprString == null) {
						newExprString = aSqlExpression.functionName;
						if (aSqlExpression.functionId == IFunctionID.CAST_ID) {
							int theFromType;

							SqlExpression theFuncParam = aSqlExpression.functionParams
									.get(0);
							theFuncParam.rebuildExpression();
							if (theFuncParam.getExprDataType() == null) {
                                theFromType = Types.NULL;
                            } else if (theFuncParam.getExprType() == SqlExpression.SQLEX_COLUMN) {
								theFromType = theFuncParam.getColumn().columnType.type;
							} else {
								theFromType = theFuncParam.getExprDataType().type;
							}

							newExprString = CastTemplates.getTemplate(
									theFromType,
									aSqlExpression.expTypeOfCast.getSqlType());
							if (newExprString == null
									|| newExprString.trim().equals("")) {
								throw new XDBServerException(
										"Can not cast those datatypes.");
							}
							HashMap<String, String> arguments = new HashMap<String, String>();
							for (SqlExpression sqlexpr : aSqlExpression.functionParams) {
								sqlexpr.rebuildExpression();
								arguments.put("arg", sqlexpr.exprString);
								arguments.put("type",
										aSqlExpression.expTypeOfCast
												.getTypeString());
							}
							newExprString = ParseCmdLine.substitute(
									newExprString, arguments);

						} else if (aSqlExpression.functionId != IFunctionID.COUNT_STAR_ID) {
							if (aSqlExpression.needParenthesisInFunction) {
								newExprString = newExprString + "( ";
							} else {
								newExprString = newExprString + " ";
							}

							if (FunctionAnalysis
									.isGroupFunction(aSqlExpression.functionId)
									&& aSqlExpression.isDistinctGroupFunction) {
								newExprString = newExprString + "DISTINCT ";
							}

							if (FunctionAnalysis
									.isGroupFunction(aSqlExpression.functionId)
									&& aSqlExpression.isAllCountGroupFunction) {
								newExprString = newExprString + "ALL ";
							}

							int count = 0;
							for (SqlExpression sqlexpr : aSqlExpression.functionParams) {
								if (count != 0) {
									newExprString = newExprString
											+ aSqlExpression.getArgSeparator();
								}
								count = 1;
								sqlexpr.rebuildExpression();
								newExprString = newExprString
										+ sqlexpr.exprString;
							}

							if (aSqlExpression.needParenthesisInFunction) {
								newExprString = newExprString + ") ";
							} else {
								newExprString = newExprString + " ";
							}
						} else {
							if (newExprString == null) {
								newExprString = aSqlExpression.exprString;
							}
						}
					} else {
						HashMap<String, String> arguments = new HashMap<String, String>();
						int count = 0;
						for (SqlExpression sqlexpr : aSqlExpression.functionParams) {
							count++;
							sqlexpr.rebuildExpression();
							arguments.put("arg" + count, sqlexpr.exprString);
						}
						newExprString = ParseCmdLine.substitute(newExprString,
								arguments);
					}

				} else if (aSqlExpression.getExprType() == SqlExpression.SQLEX_CASE) {
					newExprString = aSqlExpression.getCaseConstruct()
							.rebuildString();
				} else if (aSqlExpression.getExprType() == SqlExpression.SQLEX_CONSTANT) {
					if (Props.XDB_STRIP_INTERVAL_QUOTES
							&& aSqlExpression.getConstantValue() != null
							&& aSqlExpression.getConstantValue().toLowerCase()
									.startsWith("interval")) {
						newExprString = aSqlExpression.getConstantValue()
								.replaceAll("'", "");
					} else {
						newExprString = aSqlExpression.getConstantValue();
					}
				} else if (aSqlExpression.getExprType() == SqlExpression.SQLEX_SUBQUERY) {
					newExprString = "("
							+ aSqlExpression.subqueryTree.rebuildString() + ")";
				} else if (aSqlExpression.getExprType() == SqlExpression.SQLEX_LIST) {
					boolean firstTime = true;
					for (SqlExpression aSqlExpressionItem : aSqlExpression.expressionList) {
						if (firstTime) {
							newExprString += "( "
									+ aSqlExpressionItem.rebuildString();
							firstTime = false;
						} else {
							newExprString += " , "
									+ aSqlExpressionItem.rebuildString();
						}

					}
					newExprString += " )";
				} else {
					newExprString = aSqlExpression.exprString;
				}
			} else {
				// Always do left side first
				if (aSqlExpression.leftExpr != null) {
					aSqlExpression.leftExpr.rebuildExpression();
					leftExprString = aSqlExpression.leftExpr.exprString;
					aSqlExpression.rightExpr.rebuildExpression();
					rightExprString = aSqlExpression.rightExpr.exprString;

					if (aSqlExpression.operator.compareTo("\\") == 0) {
						aSqlExpression.operator = "/";
					}

					newExprString = "(" + leftExprString + " "
							+ aSqlExpression.operator + " " + rightExprString
							+ ")";
				}
			}
		} else {
			// if a temporary column, quote it if not quoted
			if (aSqlExpression.exprType == SqlExpression.SQLEX_COLUMN
					&& aSqlExpression.exprString != null
					&& aSqlExpression.exprString.length() > 0
                    && !aSqlExpression.exprString
                            .startsWith(Props.XDB_IDENTIFIER_QUOTE_OPEN)) {
				newExprString = IdentifierHandler
						.quote(aSqlExpression.exprString);
			} else {
				newExprString = aSqlExpression.exprString;
			}
		}

		return newExprString;
	}

	/**
	 * Checks to see if the expression is an aggregate expression. Note that it
	 * does not check child expressions for aggregates.
	 *
	 * @return whether or not expression is an aggregate function
	 */
	public boolean isAggregateExpression() {
		if (getExprType() == SQLEX_FUNCTION) {
			switch (functionId) {
			case IFunctionID.SUM_ID:
			case IFunctionID.AVG_ID:
			case IFunctionID.COUNT_STAR_ID:
			case IFunctionID.COUNT_ID:
			case IFunctionID.MAX_ID:
			case IFunctionID.MIN_ID:
			case IFunctionID.BITOR_ID:
			case IFunctionID.BITAND_ID:
			case IFunctionID.BOOLOR_ID:
			case IFunctionID.BOOLAND_ID:
			case IFunctionID.EVERY_ID:
			case IFunctionID.STDEV_ID:
			case IFunctionID.STDEVPOP_ID:
			case IFunctionID.STDEVSAMP_ID:
			case IFunctionID.VARIANCE_ID:
			case IFunctionID.VARIANCEPOP_ID:
			case IFunctionID.VARIANCESAMP_ID:
			case IFunctionID.REGRCOUNT_ID:
			case IFunctionID.REGRSXX_ID:
			case IFunctionID.REGRSYY_ID:
			case IFunctionID.REGRSXY_ID:
			case IFunctionID.COVARPOP_ID:
			case IFunctionID.CORR_ID:
			case IFunctionID.COVARSAMP_ID:
			case IFunctionID.REGRSLOPE_ID:
			case IFunctionID.REGRINTERCEPT_ID:
			case IFunctionID.REGRR2_ID:
			case IFunctionID.REGRAVX_ID:
			case IFunctionID.REGRAVY_ID:
			case IFunctionID.ST_EXTENT_ID:
			case IFunctionID.ST_EXTENT3D_ID:
			case IFunctionID.ST_COLLECT_AGG_ID:
				return true;
			default:
				return false;
			}
		}
		return false;
	}

	/**
	 * Checks to see if aggregates are used in this expression The
	 * Optimizer/Planner uses this to handle the final SELECTs
	 *
	 * @return
	 *
	 */

	public boolean containsAggregates() {
		for (SqlExpression expr : SqlExpression.getNodes(this, SQLEX_FUNCTION)) {
			if (expr.isAggregateExpression()) {
				return true;
			}
		}
		return false;
	}

	/**
	 *
	 *
	 *
	 * @param prepend
	 *
	 * @return
	 *
	 */
	public String toString(String prepend) {
		return prepend + " " + exprString;
	}

	/**
	 *
	 *
	 *
	 * @param colname
	 *
	 * @param tableName
	 *
	 * @param tableAlias
	 *
	 * @param columnAlias
	 *
	 * @param Operator
	 *
	 * @return
	 *
	 */

	public static SqlExpression getSqlColumnExpression(String colname,
			String tableName, String tableAlias, String columnAlias,
			String Operator)

	{ // Create a new Expression
		SqlExpression sqlexpr = new SqlExpression();
		// Assign Column Name to the exprString --
		sqlexpr.exprString = colname;
		// ExprType SQLEX_COLUMN
		sqlexpr.setExprType(SqlExpression.SQLEX_COLUMN);
		// Create a new Column --
		sqlexpr.setColumn(new AttributeColumn());
		// Assign Values to the four fields
		sqlexpr.getColumn().columnName = colname;
		sqlexpr.setAlias(sqlexpr.getColumn().columnName);
		sqlexpr.getColumn().setTableName(tableName);
		if (Operator == null || Operator.equals("")) {
			sqlexpr.operator = "+";
		} else {
			sqlexpr.operator = Operator;
		}
		if (columnAlias == null || columnAlias.equals("")) {
			sqlexpr.getColumn().columnAlias = colname;
		} else {
			sqlexpr.getColumn().columnAlias = columnAlias;
		}
		if (tableAlias == null || tableAlias.equals("")) {
			sqlexpr.getColumn().setTableAlias(tableName);
		} else {
			sqlexpr.getColumn().setTableAlias(tableAlias);
		}
		sqlexpr.setAlias(sqlexpr.getColumn().columnAlias);
		sqlexpr.outerAlias = sqlexpr.getAlias();

		return sqlexpr;
	}

	/**
	 *
	 * This function is responsible for finding the type of column contained in
	 * a particular expression.
	 *
	 *
	 *
	 * @return ExpressionType
	 *
	 * @param expr
	 *
	 * @param column
	 *
	 * @throws XDBServerException
	 *
	 * @throws ColumnNotFoundException
	 *
	 */

	public ExpressionType setExpressionResultType(SqlExpression expr,
			SysColumn column) {

		expr.setExprDataType(new ExpressionType());
		expr.getExprDataType().setExpressionType(column.getColType(),
				column.getColLength(), column.getColPrecision(),
				column.getColScale());
		return expr.getExprDataType();
	}

	/**
	 *
	 * This function is responsible for finding the type of expression a
	 *
	 * particular expression is.
	 *
	 *
	 *
	 * @return
	 *
	 * @param database
	 *
	 * @param expr
	 *
	 * @throws XDBServerException
	 *
	 * @throws ColumnNotFoundException
	 *
	 */

	public static ExpressionType setExpressionResultType(SqlExpression expr,
	                Command commandToExecute) throws XDBServerException,
			ColumnNotFoundException {
		if (expr.getExprDataType() != null && expr.getExprDataType().type != 0) {
			return expr.getExprDataType();
		}

		SysDatabase database = commandToExecute.getClientContext().getSysDatabase();

		if (expr.getExprType() == SqlExpression.SQLEX_PARAMETER) {
			// expr.exprDataType.type may be 0 in this case
			return expr.getExprDataType();
		} else if (expr.getExprType() == SqlExpression.SQLEX_SUBQUERY) {
		        SqlExpression projExpr = expr.subqueryTree.getProjectionList().get(0);

		        if (projExpr.getExprDataType() == null) {
		            // Analysis of the subquery is incomplete
		                SQLExpressionHandler aSQLExpressionHandler =
		                        new SQLExpressionHandler(commandToExecute);
		                aSQLExpressionHandler.finishSubQueryAnalysis(expr);
		        }

			// In this case we will need to find out the expression type that
			// this SQL query will return and that will be the expression type
			return projExpr.getExprDataType();
		}
                
		if (expr.getExprType() == SqlExpression.SQLEX_CONDITION)
		{
			// Here we are sure that the SQLEX_CONDITION will give us a boolean
			// value
			ExpressionType exprT = new ExpressionType();
			exprT.setExpressionType(ExpressionType.BOOLEAN_TYPE, 0, 0, 0);
			return exprT;
		}

		if (expr.getExprType() == SqlExpression.SQLEX_COLUMNLIST) {
			// In case the Sql Expression is a columnlist
		}

		if (expr.getExprType() == SqlExpression.SQLEX_CONSTANT) {
			// In this case we will have to check the actual constant value
			// whether it is a string , a numeric, or decimal or a varchar -
			int dataType = expr.getConstantDataType();
			ExpressionType exprT = new ExpressionType();
			exprT.setExpressionType(dataType,
					expr.getConstantValue() == null ? -1 : expr
							.getConstantValue().length(),
					CONSTANT_PRECISION_DOUBLE_VALUE,
					CONSTANT_DOUBLE_SCALE_VALUE);
			expr.setExprDataType(exprT);
			return exprT;
		}

		if (expr.getExprType() == SqlExpression.SQLEX_FUNCTION) {
			if (expr.functionId == IFunctionID.CAST_ID) {
				expr.functionParams.get(0).rebuildExpression();
				SqlExpression.setExpressionResultType(
						expr.functionParams.get(0), commandToExecute);
				expr.setExprDataType(new ExpressionType());
				expr.exprDataType.type = expr.expTypeOfCast.getSqlType();
				expr.exprDataType.length = expr.expTypeOfCast.getLength();
				expr.exprDataType.precision = expr.expTypeOfCast.getPrecision();
				expr.exprDataType.scale = expr.expTypeOfCast.getScale();
				if ((expr.exprDataType.type == Types.CHAR || expr.exprDataType.type == Types.VARCHAR)
						&& expr.exprDataType.length == -1) {
					expr.exprDataType.length = 1024;
				}
				return expr.exprDataType;
			} else {
				// Each function will give a specific value, - CAST operation
				// will also be treated as a function
				for (SqlExpression afunctionParameter : expr.functionParams) {
					afunctionParameter.setExprDataType(SqlExpression
							.setExpressionResultType(afunctionParameter,
									commandToExecute));
				}
				ExpressionType exprT = expr.getFunctionOutputValue(commandToExecute);
				expr.setExprDataType(exprT);
				return exprT;
			}
		}

		if (expr.getExprType() == SqlExpression.SQLEX_UNARY_EXPRESSION) {
			// assumes that the left one will actually be filled.
			ExpressionType exprDatatypeL = setExpressionResultType(
					expr.leftExpr, commandToExecute);

			expr.setExprDataType(exprDatatypeL);
			return expr.getExprDataType();
		}

		if (expr.getExprType() == SqlExpression.SQLEX_COLUMN) {
			// If it is orphan
			if ((expr.getColumn().columnGenre & AttributeColumn.ORPHAN) == AttributeColumn.ORPHAN) {
				if (expr.getColumn().relationNode == null) {
					return null;
				}
			}

			AttributeColumn colAttrib = expr.getColumn();

			// This will find the column Type and Set it for this particular
			// column
			if (expr.mapped == SqlExpression.INTERNALMAPPING) {
				expr.setExprDataType(SqlExpression.setExpressionResultType(
						expr.mappedExpression, commandToExecute));
			} else {
				expr.setExprDataType(colAttrib.getColumnType(database));
			}

			return expr.getExprDataType();
		}

		else if (expr.getExprType() == SqlExpression.SQLEX_OPERATOR_EXPRESSION) {
			try {
				ExpressionType exprDataTypeL = setExpressionResultType(
						expr.leftExpr, commandToExecute);

				ExpressionType exprDataTypeR = setExpressionResultType(
						expr.rightExpr, commandToExecute);

				ExprTypeHelper aExprTypeHelper = new ExprTypeHelper();

				aExprTypeHelper.leftExprType = exprDataTypeL;

				aExprTypeHelper.righExprType = exprDataTypeR;

				aExprTypeHelper.Operator = expr.operator;

				expr.setExprDataType(exprDataTypeL
						.GetExpressionType(aExprTypeHelper));
			} catch (XDBServerException ex) {
				String errorMessage = ErrorMessageRepository.EXPRESSION_TYPE_UNDETERMINED;
				throw new XDBServerException(
						errorMessage + expr.rebuildString(),
						ex,
						ErrorMessageRepository.EXPRESSION_TYPE_UNDETERMINED_CODE);
			}
			return expr.getExprDataType();
		} else if (expr.getExprType() == SqlExpression.SQLEX_CASE) {
			// 1. For each SQL expression in case construct call the -
			// setExpressionDataType
			// function. All the expression in the result data type should be
			// same
			expr.setExprDataType(expr.getCaseConstruct().setDataType(commandToExecute));
			return expr.getExprDataType();
		} else if (expr.getExprType() == SqlExpression.SQLEX_LIST) {
			SqlExpression aSqlExpression = expr.expressionList.get(0);
			ExpressionType exprType = SqlExpression.setExpressionResultType(
					aSqlExpression, commandToExecute);
			// Check to make sure all are of the same type
			for (SqlExpression aSqlExpressionItem : expr.expressionList) {
				ExpressionType exprItemType = SqlExpression
						.setExpressionResultType(aSqlExpressionItem, commandToExecute);
				if (exprItemType.type != exprType.type) {
					throw new XDBServerException("The Expression "
							+ aSqlExpression.rebuildString() + " and "
							+ aSqlExpressionItem.rebuildString()
							+ " are not of the same type");
				}
			}
			return exprType;
		} else {
			throw new XDBServerException(
					ErrorMessageRepository.INVALID_DATATYPE + "( "
							+ expr.getExprType() + " , " + expr.exprString
							+ " ) ", 0,
					ErrorMessageRepository.INVALID_DATATYPE_CODE);
		}
	}

	/**
	 *
	 *
	 *
	 * @return
	 *
	 */

	int getConstantDataType() {
		// Determine if this is a constant data Type
		if (getExprType() == SqlExpression.SQLEX_CONSTANT) {
			if (constantValue == null) {
				return ExpressionType.NULL_TYPE;
			}

			// First we differentiate between - Numeric and Non _ Numeric
			// If the peice of data is non- numeric we automatically assign it
			// the value string
			// else we need to check if it is DOUBLE OR If It is Numeric
			// boolean isNumeric = false;
			try {
				Double.parseDouble(constantValue);
				return ExpressionType.DOUBLEPRECISION_TYPE;
			} catch (NumberFormatException ex) {
				// check to make sure that we are not dealing with
				// null
				// isNumeric = false;
				// if (constantValue.equalsIgnoreCase("null"))
				// if (constantValue == null)
				// {
				// return ExpressionType.NULL_TYPE;
				// }
				return ExpressionType.VARCHAR_TYPE;
			}
		}

		// Some illegal value
		return -1;
	}

	/**
	 *
	 * Is this expression NULL?
	 *
	 * @return
	 *
	 */
	public boolean isNullConstant() {
		if (exprType == SqlExpression.SQLEX_CONSTANT) {
			if (constantValue == null) {
				return true;
			}
		}
		return false;
	}

	/**
	 *
	 *
	 *
	 * @param database
	 *
	 * @throws org.postgresql.stado.exception.ColumnNotFoundException
	 *
	 * @return
	 *
	 */

	ExpressionType getFunctionOutputValue(Command commandToExecute)
			throws ColumnNotFoundException, XDBServerException {
                SysDatabase database = commandToExecute.getClientContext().getSysDatabase();
		// Let us be positive and create an expression type which will be set
		// later
		ExpressionType exprType = new ExpressionType();
		// Check if there is a valid Function ID - use a switch case statement
		// here
		switch (this.functionId) {
	    case IFunctionID.SUBSTRING_ID:
            exprType = FunctionAnalysis.analyzeSubstring(this);
            break;
		case IFunctionID.EXTRACT_ID:
			exprType = FunctionAnalysis.analyzeExtract(this);
			break;
		case IFunctionID.TRIM_ID:
			exprType = FunctionAnalysis.analyzeTrim(this);
			break;
        case IFunctionID.CONVERT_ID:
            exprType = FunctionAnalysis.analyzeConvert(this);
            break;
        case IFunctionID.OVERLAY_ID:
            exprType = FunctionAnalysis.analyzeOverlay(this);
            break;
        case IFunctionID.POSITION_ID:
            exprType = FunctionAnalysis.analyzePosition(this);
            break;
		/* Aggregate Function - */
		case IFunctionID.AVG_ID:
			exprType = FunctionAnalysis.analyzeAverageParameter(this, commandToExecute);
			break;
		case IFunctionID.COUNT_ID:
			exprType = FunctionAnalysis.analyzeCountParameter(this);
			break;
		case IFunctionID.MAX_ID:
		case IFunctionID.MIN_ID:
			exprType = FunctionAnalysis.analyzeMax_MinParameter(this, commandToExecute);
			break;
		case IFunctionID.SUM_ID:
			exprType = FunctionAnalysis.analyzeSumParameter(this, commandToExecute);
			break;
		case IFunctionID.COUNT_STAR_ID: // no semantic analysis required
			exprType.setExpressionType(ExpressionType.INT_TYPE, 10, 10, 0);
			for (SqlExpression sqlexpr : functionParams) {
				sqlexpr.setExprDataType(setExpressionResultType(sqlexpr,
						commandToExecute));
			}
			break;
		// Misc Functions
        case IFunctionID.NULLIF_ID:
            /* Function Signature : (value1,value2) Output : type of value1 */
            exprType = FunctionAnalysis.analyzeNullIf(this);
            break;
		case IFunctionID.COALESCE_ID:
			exprType = FunctionAnalysis.analyzeCoalesce(this, commandToExecute);
			break;
		case IFunctionID.VARIANCE_ID:
		case IFunctionID.STDEV_ID:
		case IFunctionID.STDEVPOP_ID:
		case IFunctionID.STDEVSAMP_ID:
		case IFunctionID.VARIANCEPOP_ID:
		case IFunctionID.VARIANCESAMP_ID:
			exprType = FunctionAnalysis.analyzeVarianceOrStddev(this);
			break;
		case IFunctionID.CORR_ID:
		case IFunctionID.COVARPOP_ID:
		case IFunctionID.COVARSAMP_ID:
		case IFunctionID.REGRAVX_ID:
		case IFunctionID.REGRAVY_ID:
		case IFunctionID.REGRINTERCEPT_ID:
		case IFunctionID.REGRSLOPE_ID:
		case IFunctionID.REGRR2_ID:
		case IFunctionID.REGRSXX_ID:
		case IFunctionID.REGRSXY_ID:
		case IFunctionID.REGRSYY_ID:
			exprType = FunctionAnalysis.analyzeCoRegFunc(this);
			break;
		case IFunctionID.REGRCOUNT_ID:
			exprType = FunctionAnalysis.analyzeRegrCount(this);
			break;
		case IFunctionID.BITAND_ID:
		case IFunctionID.BITOR_ID:
			/* Input: sqlExpr Output: type of sqlExpr */
			exprType = FunctionAnalysis.analyzeBitAnd(this);
			break;
		case IFunctionID.BOOLAND_ID:
		case IFunctionID.BOOLOR_ID:
		case IFunctionID.EVERY_ID:
			/* Input: BOOLEAN Output: BOOLEAN */
			exprType = FunctionAnalysis.analyzeBoolAnd(this);
			break;

		default:
			try {
                exprType = FunctionAnalysis.analyzeFunction(this,
                            commandToExecute);
			} catch (Exception ex) {
				throw new XDBServerException(this.functionName, ex);
			}
		}

		return exprType;
	}

	public String rebuildString(XDBSessionContext client) {
		rebuildExpression(client);
		return exprString;
	}

	/**
	 *
	 *
	 *
	 * @return
	 *
	 */
	public String rebuildString() {
		rebuildExpression();

		return exprString;
	}

	/**
	 * Checks to see if the expression only contains constants It or all of its
	 * children must have type SQLEX_CONSTANT
	 *
	 *
	 * @return
	 *
	 */

	public boolean isConstantExpr() {
		if (this.getExprType() == SQLEX_CONSTANT) {
			return true;
		} else if (this.getExprType() == SQLEX_OPERATOR_EXPRESSION) {
			if (this.leftExpr != null && !this.leftExpr.isConstantExpr()) {
				return false;
			}
			if (this.rightExpr != null && !this.rightExpr.isConstantExpr()) {
				return false;
			}
			return true;
		} else if (this.getExprType() == SQLEX_FUNCTION) {
			// Make sure not an aggregate function
			if (this.containsAggregates()) {
				return false;
			}

			for (int i = 0; i < this.functionParams.size(); i++) {
				SqlExpression aSE = this.functionParams.get(i);
				if (!aSE.isConstantExpr()) {
					return false;
				}
			}
			return true;
		} else if (this.getExprType() == SQLEX_UNARY_EXPRESSION) {
			if (!this.leftExpr.isConstantExpr()) {
				return false;
			}
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Functions for get and set parent containerNode
	 *
	 *
	 * @return
	 *
	 */

	public RelationNode getParentContainerNode() {
		return parentContainerNode;
	}

	/**
	 *
	 *
	 *
	 * @param parentContainerNode
	 *
	 */

	public void setParentContainerNode(RelationNode parentContainerNode) {
		this.parentContainerNode = parentContainerNode;
	}

	/**
	 *
	 * Checks to see if the SqlExpression is equal to the one passed in, or if
	 * it is contained within it.
	 *
	 * @param aSqlExpression
	 *
	 * @return
	 *
	 */
	public boolean contains(SqlExpression aSqlExpression) {
		boolean check = false;

		if (aSqlExpression == this) {
			return true;
		}

		if (this.leftExpr != null) {
			check = this.leftExpr.contains(aSqlExpression);
		}

		if (!check && this.rightExpr != null) {
			return this.rightExpr.contains(aSqlExpression);
		}

		return check;
	}

	/**
	 *
	 * Check to see if the SqlExpression contains any expressions that are only
	 * derived from the specified RelationNode. <br>
	 *
	 * @param checkTableName
	 *
	 * @return
	 *
	 */
	public boolean containsColumnsExclusiveFromTable(String checkTableName) {
		boolean check = false;

		if (this.getExprType() == SQLEX_COLUMN) {
			// if (this.column.relationNode == aRelationNode)
			if (this.getColumn().relationNode.getCurrentTempTableName()
					.equalsIgnoreCase(checkTableName)) {
				return true;
			} else {
				return false;
			}
		}

		/*
		 * We don't want to check recursively. This is used just to detect
		 * special, hashable correlated subqueries
		 *
		 * if (this.leftExpr != null) { check =
		 * this.leftExpr.containsColumnsExclusiveFromTable (checkTableName);
		 *
		 * if (!check) { return false; } }
		 *
		 * if (this.rightExpr != null) { return
		 * this.rightExpr.containsColumnsExclusiveFromTable (checkTableName); }
		 */

		return check;
	}

	/**
	 *
	 * Check to see if the SqlExpression contains the specified AttributeColumn
	 *
	 * @param anAttributeColumn
	 *
	 * @return
	 *
	 */
	public boolean containsColumn(AttributeColumn anAttributeColumn) {
		boolean check = false;

		if (this.getExprType() == SQLEX_COLUMN) {
			// if (this.column.relationNode == aRelationNode)
			if (this.getColumn() == anAttributeColumn) {
				return true;
			} else {
				return false;
			}
		} else if ((this.getExprType() & SQLEX_FUNCTION) > 0) {
			for (SqlExpression aSqlExpression : functionParams) {
				if (aSqlExpression.containsColumn(anAttributeColumn)) {
					return true;
				}
			}
		} else if ((this.getExprType() & SQLEX_CASE) > 0) {
			for (SqlExpression aSqlExpression : caseConstruct
					.getSQLExpressions()) {
				if (aSqlExpression.containsColumn(anAttributeColumn)) {
					return true;
				}
			}
		} else if ((this.getExprType() & SQLEX_SUBQUERY) > 0) {
			if ((this.subqueryTree.getQueryType() & QueryTree.SCALAR) == 0) {
				for (SqlExpression aSqlExpression : parentContainerNode
						.getProjectionList()) {
					if (aSqlExpression.containsColumn(anAttributeColumn)) {
						return true;
					}
				}
			}
		}

		if (leftExpr != null) {
			check = leftExpr.containsColumn(anAttributeColumn);

			if (check) {
				return true;
			}
		}

		if (rightExpr != null) {
			return rightExpr.containsColumn(anAttributeColumn);
		}

		return check;
	}

	/**
	 *
	 * Check to see if the SqlExpression contains the specified AttributeColumn
	 *
	 * @param aRelationNode
	 *
	 * @return
	 *
	 */
	public boolean contains(RelationNode aRelationNode) {
		boolean check = false;

		if (this.getExprType() == SQLEX_COLUMN) {
			// if (this.column.relationNode == aRelationNode)
			if (this.getColumn().relationNode == aRelationNode) {
				return true;
			} else {
				return false;
			}
		} else if ((this.getExprType() & SQLEX_FUNCTION) > 0) {
			for (SqlExpression aSqlExpression : functionParams) {
				if (aSqlExpression.contains(aRelationNode)) {
					return true;
				}
			}
		} else if ((this.getExprType() & SQLEX_CASE) > 0) {
			for (SqlExpression aSqlExpression : caseConstruct
					.getSQLExpressions()) {
				if (aSqlExpression.contains(aRelationNode)) {
					return true;
				}
			}
		} else if ((this.getExprType() & SQLEX_SUBQUERY) > 0) {
			if ((this.subqueryTree.getQueryType() & QueryTree.SCALAR) == 0) {
				for (SqlExpression aSqlExpression : parentContainerNode
						.getProjectionList()) {
					if (aSqlExpression.contains(aRelationNode)) {
						return true;
					}
				}
			}
		}

		if (this.leftExpr != null) {
			check = this.leftExpr.contains(aRelationNode);

			if (check) {
				return true;
			}
		}

		if (this.rightExpr != null) {
			return this.rightExpr.contains(aRelationNode);
		}

		return check;
	}

	/**
	 * Normalize numeric value to prevent ambiguous hash codes
	 *
	 * @param anInputString
	 * @return
	 */
	private static String normalizeNumber(String anInputString, int scale) {
		// Detect scientific format
		int pos = Math.max(anInputString.indexOf("E"),
				anInputString.indexOf("e"));
		if (pos > 0) {
			return normalizeNumber(anInputString.substring(0, pos), scale)
					+ "E"
					+ normalizeNumber(anInputString.substring(pos + 1), 0);
		}
		// detect sign
		boolean negative = anInputString.charAt(0) == '-';
		pos = anInputString.indexOf(".");
		int end = anInputString.length() - 1;
		if (pos > -1) {
			while (anInputString.charAt(end) == '0') {
				end--;
			}
			if (end == pos) {
				end--;
			}
		} else {
			pos = end + 1;
		}
		int start = 0;
		while (start < pos
				&& (anInputString.charAt(start) == '-'
						|| anInputString.charAt(start) == '+' || anInputString
						.charAt(start) == '0')) {
			start++;
		}
		if (end - pos > scale) {
			end = pos + scale;
		}
		return (negative ? "-" : "")
				+ (start == pos ? "0" : anInputString.substring(start, pos))
				+ (end > pos ? anInputString.substring(pos, end + 1) : "");
	}

	/**
	 *
	 *
	 *
	 * @return
	 *
	 * @param anInputString
	 *
	 */

	public static String normalizeDate(String anInputString) {
		String key_word = "";
		if (anInputString.toLowerCase().startsWith("date")) {
			anInputString = anInputString.substring(4, anInputString.length())
					.trim();
			key_word = "date";
		}
		// Strip quotes if present
		while (anInputString.charAt(0) == '\''
				&& anInputString.charAt(anInputString.length() - 1) == '\'') {
			anInputString = anInputString.substring(1,
					anInputString.length() - 1);
		}
		String theResult;
		if (anInputString.matches("[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]")) {
			// YYYYMMDD
			theResult = anInputString.substring(0, 4) + "-"
					+ anInputString.substring(4, 6) + "-"
					+ anInputString.substring(6, 8);
		} else if (anInputString
				.matches("[0-9][0-9][0-9][0-9]\\-[0-9][0-9]\\-[0-9][0-9]")) {
			// YYYY-MM-DD
			theResult = anInputString;
		} else if (anInputString
				.matches("[0-9][0-9][0-9][0-9]\\-[0-9]\\-[0-9]")) {
			// YYYY-M-D
			theResult = anInputString.substring(0, 5) + "0"
					+ anInputString.substring(5, 7) + "0"
					+ anInputString.substring(7, 8);
		} else if (anInputString
				.matches("[0-9]{1,4}\\-[a-zA-Z]*[[0-9]{1,2}]?\\-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}[:[0-9]{0,2}]?[\\.[0-9]*]?")) {
			// YYYY-MM-DD HH:mim:sec
			theResult = anInputString;
		} else if (anInputString
				.matches("[0-9]{1,2}\\-[a-zA-Z]*[0-9]{0,2}\\-[0-9]{2,4}[ [0-9]{1,2}:[0-9]{1,2}:[0-9]{0,2}[\\.[0-9]*]?]?")) {
			// DD-MM-YYYY HH:mim:sec
			theResult = anInputString;
		} else if (anInputString
				.matches("[0-9]{1,2}/[0-9]{0,2}/[0-9]{2,4}[ [0-9]{1,2}:[0-9]{1,2}:[0-9]{0,2}[\\.[0-9]*]?]?")) {
			// MM/DD/YYYY HH:mim:sec
			theResult = anInputString;
		} /*
         */
		// else if (anInputString
		// .matches("\'[0-9]{1,2}\\-[0-9]{1,2}\\-[0-9][0-9][0-9][0-9]\'"))
		// {
		// //MM-DD-YYYY
		// String str2 = theResult.substring(theResult.length() - 4) + "-" +
		// theResult.substring(0,theResult.length() - 5);
		// theResult = "'" + str2 + "'";
		// theResult = normalizeDate(theResult);
		// theResult = theResult.replaceAll("'", "");
		// }
		else if (anInputString
				.matches("[0-9][0-9][0-9][0-9]\\-[0-9]\\-[0-9][0-9]")) {
			// YYYY-M-DD
			theResult = anInputString.substring(0, 5) + "0"
					+ anInputString.substring(5, 9);
		} else if (anInputString
				.matches("[0-9][0-9][0-9][0-9]\\-[0-9][0-9]\\-[0-9]")) {
			// YYYY-MM-D
			theResult = anInputString.substring(0, 8) + "0"
					+ anInputString.substring(8, 9);
		} else {
			throw new XDBServerException("Invalid date/time format "
					+ anInputString);
		}

		theResult = "'" + theResult + "'";

		if (key_word.length() != 0) {
			theResult = key_word + theResult;
		}
		return theResult;
	}

	/**
	 * @param anInputString
	 *            000102 00:01:02 0:1:2 00:1:2 01:02 1:02 1:2 02 2
	 *
	 * @return
	 */
	public static String normalizeTime(String anInputString) {
		String key_word = "";
		if (anInputString.toLowerCase().startsWith("time")) {
			anInputString = anInputString.substring(4, anInputString.length())
					.trim();
			key_word = "time";
		}

		// Strip quotes if present
		while (anInputString.charAt(0) == '\''
				&& anInputString.charAt(anInputString.length() - 1) == '\'') {
			anInputString = anInputString.substring(1,
					anInputString.length() - 1);
		}

		String theResult;
		if (anInputString.matches("[0-9][0-9][0-9][0-9][0-9][0-9]")) {
			// HHMMSS
			theResult = anInputString.substring(0, 2) + ":"
					+ anInputString.substring(2, 4) + ":"
					+ anInputString.substring(4, 6);
		} else if (anInputString
				.matches("[0-9]{1,2}\\:[0-9]{1,2}\\:[0-9]{1,2}[.\\d+]*")) {
			// HH:MM:SS[.bla.bla...]
			StringTokenizer st = new StringTokenizer(anInputString, ":");
			String[] str1 = new String[st.countTokens()];
			for (int i = 0; st.hasMoreTokens(); i++) {
				str1[i] = st.nextToken();
				if (str1[i].length() == 1) {
					str1[i] = "0" + str1[i];
				}
			}

			theResult = str1[0] + ":" + str1[1] + ":" + str1[2];
		} else if (anInputString.matches("[0-9]{1,2}\\:[0-9]{1,2}")) {
			// HH:MM
			StringTokenizer st = new StringTokenizer(anInputString, ":");
			String[] str1 = new String[st.countTokens()];
			for (int i = 0; st.hasMoreTokens(); i++) {
				str1[i] = st.nextToken();
				if (str1[i].length() == 1) {
					str1[i] = "0" + str1[i];
				}
			}
			theResult = str1[0] + ":" + str1[1] + ":00";
		} else if (anInputString.matches("[0-9]{1,2}")) {
			// SS
			if (anInputString.length() == 1) {
				theResult = "00:00:0" + anInputString;
			} else {
				theResult = "00:00:" + anInputString;
			}
		} else {
			throw new XDBServerException("Invalid date/time format "
					+ anInputString);
		}

		theResult = "'" + theResult + "'";
		if (key_word.length() != 0) {
			theResult = key_word + theResult;
		}

		return theResult;
	}

	/**
	 *
	 *
	 *
	 * @param anInputString
	 *
	 * @return
	 *
	 */

	public static String normalizeTimeStamp(String anInputString) {
		String theResult;
		String theDate = null;
		String theLongTime = null;
		String subseconds = "";
		String key_word = "";

		if (anInputString.toLowerCase().startsWith("timestamp")) {
			anInputString = anInputString.substring(9, anInputString.length())
					.trim();
			key_word = "timestamp";
		}
		// Strip quotes if present
		while (anInputString.charAt(0) == '\''
				&& anInputString.charAt(anInputString.length() - 1) == '\'') {
			anInputString = anInputString.substring(1,
					anInputString.length() - 1);
		}

		if (Props.XDB_SUBSECOND_PRECISION > 0) {
			subseconds = subsecondBaseString.substring(0,
					Props.XDB_SUBSECOND_PRECISION);
		}

		int spacePos = anInputString.indexOf(" ");
		theDate = normalizeDate(spacePos < 0 ? anInputString : anInputString
				.substring(0, spacePos));
		theDate = theDate.replaceAll("'", "");
		if (spacePos < 0) {
			if (Props.XDB_SUBSECOND_PRECISION > 0) {
				theLongTime = "00:00:00." + subseconds;
			} else {
				theLongTime = "00:00:00";
			}
		} else {
			String timePart = anInputString.substring(spacePos + 1).trim();
			String timeInfo;
			String zoneInfo = "";

			// See if we have "-" or "+" for time zone
			int dashPos = timePart.indexOf('-');
			if (dashPos < 0) {
				dashPos = timePart.indexOf('+');
			}
			if (dashPos >= 0) {
				timeInfo = timePart.substring(0, dashPos).trim();
				zoneInfo = timePart.substring(dashPos).trim();
			} else {
				int pos;
				// See if we have text for zone info like PST.
				for (pos = timePart.length() - 1; pos >= 0; pos--) {
					char c = timePart.charAt(pos);
					if (!(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z')) {
						break;
					}
				}
				timeInfo = timePart.substring(0, pos + 1);
				zoneInfo = timePart.substring(timeInfo.length()).trim();
				timeInfo = timeInfo.trim();
			}

			int periodPos = timeInfo.indexOf(".");
			String theMls;
			if (periodPos < 0) {
				theMls = subseconds;
			} else {
				theMls = timeInfo.substring(periodPos + 1);
			}
			if (theMls.length() > Props.XDB_SUBSECOND_PRECISION) {
				throw new XDBServerException(
						"Invalid timestamp format, beyond max subsecond precision "
								+ anInputString);
			} else if (theMls.length() < Props.XDB_SUBSECOND_PRECISION) {
				theMls = theMls
						+ subseconds.substring(0, Props.XDB_SUBSECOND_PRECISION
								- theMls.length());
			}

			theLongTime = normalizeTime(periodPos < 0 ? timeInfo.trim()
					: timeInfo.substring(0, periodPos).trim());
			theLongTime = theLongTime.replaceAll("'", "");
			if (Props.XDB_SUBSECOND_PRECISION > 0) {
				theLongTime = theLongTime + '.' + theMls;
			}
			if (zoneInfo.length() > 0) {
				theLongTime += " " + zoneInfo;
			}
		}
		theResult = "'" + theDate + " " + theLongTime + "'";
		if (key_word.length() != 0) {
			theResult = key_word + theResult;
		}

		return theResult;
	}

	/**
	 * Normalize a Macaddr constant. Macaddr supports various customary formats,
	 * including '08002b:010203' '08002b-010203' '0800.2b01.0203'
	 * '08-00-2b-01-02-03' '08:00:2b:01:02:03' All of the above specify the same
	 * address. A good case for normalization is to strip the characters ':',
	 * '-', '.' from the input string
	 *
	 * @param anInputString
	 * @return normalized macaddr
	 */
	public static String normalizeMacaddr(String anInputString) {
		String theResult;
		String theMacaddr = "";

		if (anInputString.toLowerCase().startsWith("macaddr")) {
			anInputString = anInputString.substring(7, anInputString.length())
					.trim();
		}
		theMacaddr = anInputString;
		theMacaddr = theMacaddr.replaceAll("-", "");
		theMacaddr = theMacaddr.replaceAll(":", "");
		theMacaddr = theMacaddr.replaceAll("\\.", "");

		theResult = "'" + theMacaddr + "'";

		return theResult;
	}

	private static String normalizeIPv4(String value)
			throws NumberFormatException {
		int a = 0;
		int b = 0;
		int c = 0;
		int d = 0;
		int pos = value.indexOf(".");
		if (pos > 0) {
			a = Integer.parseInt(value.substring(0, pos));
			value = value.substring(pos + 1);
			pos = value.indexOf(".");
			if (pos > 0) {
				b = Integer.parseInt(value.substring(0, pos));
				value = value.substring(pos + 1);
				pos = value.indexOf(".");
				if (pos > 0) {
					c = Integer.parseInt(value.substring(0, pos));
					value = value.substring(pos + 1);
					d = Integer.parseInt(value);
				} else {
					c = Integer.parseInt(value);
				}
			} else {
				b = Integer.parseInt(value);
			}
		} else {
			a = Integer.parseInt(value);
		}
		return a + "." + b + "." + c + "." + d;
	}

	private static String normalizeIPv6(String value)
			throws NumberFormatException {
		int[] address = new int[8];
		int curPos = 0;
		int doubleColonPos = -1;
		int pos = value.indexOf(":");
		while (pos >= 0) {
			if (pos == 0) {
				if (doubleColonPos != -1) {
					throw new NumberFormatException("Malformed IPv6 address");
				}
				doubleColonPos = curPos;
				// if address starts from :: move pos to second colon
				if (curPos == 0 && value.length() > 1 && value.charAt(1) == ':') {
					pos++;
				}
			} else {
				address[curPos++] = Integer.parseInt(value.substring(0, pos),
						16);
			}
			value = value.substring(pos + 1);
			pos = value.indexOf(":");
		}
		int prefix = 8;
		String ip4part = null;
		pos = value.indexOf(".");
		if (pos > 0) {
			// IPv4-IPv6 transition addresses normalize remaining as IPv4
			ip4part = normalizeIPv4(value);
			prefix = 6;
		} else if (value.length() > 0) {
			address[curPos++] = Integer.parseInt(value, 16);
		}
		// expand double colon
		if (doubleColonPos != -1) {
			int i = prefix - 1;
			for (curPos--; curPos >= doubleColonPos; curPos--, i--) {
				address[i] = address[curPos];
			}
			for (; i >= doubleColonPos; i--) {
				address[i] = 0;
			}
		}
		// Determine longest sequence of 0's
		int longestSeqStart = -1;
		int longestSeqLength = 0;
		int curSeqStart = -1;
		int curSeqLength = 0;
		for (int i = 0; i < prefix; i++) {
			if (address[i] == 0) {
				if (curSeqStart == -1) {
					curSeqStart = i;
				}
				curSeqLength++;
			} else {
				if (curSeqLength > 1 && curSeqLength > longestSeqLength) {
					longestSeqLength = curSeqLength;
					longestSeqStart = curSeqStart;
				}
				curSeqStart = -1;
				curSeqLength = 0;
			}
		}
		if (curSeqLength > 1 && curSeqLength > longestSeqLength) {
			longestSeqLength = curSeqLength;
			longestSeqStart = curSeqStart;
		}
		// Compose result
		String result = "";
		for (int i = 0; i < prefix; i++) {
			if (i == longestSeqStart) {
				if (result.length() == 0) {
					result = "::";
				} else {
					// result has colon at the end already
					result += ":";
				}
				i += longestSeqLength - 1;
			} else {
				result += Integer.toHexString(address[i]) + ":";
			}
		}
		if (ip4part == null) {
			if (result.endsWith("::")) {
				return result;
			} else {
				return result.substring(0, result.length() - 1);
			}
		} else {
			return result + ip4part;
		}
	}

	/**
	 * Normalize values of data type inet, normalized format is 'a.b.c.d/y'
	 * where a-d are integers from range 0..255, y is between 1..32 TODO support
	 * IPv6 addresses
	 *
	 * @param anInputString
	 * @return
	 */
	public static String normalizeInet(String anInputString) {
		String value = anInputString.trim();
		if (value.startsWith("inet")) {
			value = value.substring(4).trim();
		}
		if (value.charAt(0) == '\'' && value.charAt(value.length() - 1) == '\'') {
			// Remove quotes
			value = value.substring(1, value.length() - 1);
		}
		int netmask = 0;
		String result;
		try {
			int pos = value.indexOf("/");
			if (pos > 0) {
				netmask = Integer.parseInt(value.substring(pos + 1));
				value = value.substring(0, pos);
			}
			if (value.indexOf(":") >= 0) {
				result = normalizeIPv6(value);
				if (netmask == 0 || netmask == 128) {
					return "'" + result + "'";
				} else {
					return "'" + result + "/" + netmask + "'";
				}
			} else {
				result = normalizeIPv4(value);
				if (netmask == 0 || netmask == 32) {
					return "'" + result + "'";
				} else {
					return "'" + result + "/" + netmask + "'";
				}
			}
		} catch (NumberFormatException nfe) {
			throw new XDBServerException("Invalid inet format: "
					+ anInputString);
		}
	}

	/**
	 * Normalize values of data type cidr, normalized format is 'a.b.c.d/y'
	 * where a-d are integers from range 0..255, y is between 1..32 TODO support
	 * IPv6 addresses
	 *
	 * @param anInputString
	 * @return
	 */
	public static String normalizeCidr(String anInputString) {
		String value = anInputString.trim();
		if (value.startsWith("cidr")) {
			value = value.substring(4).trim();
		}
		if (value.charAt(0) == '\'' && value.charAt(value.length() - 1) == '\'') {
			// Remove quotes
			value = value.substring(1, value.length() - 1);
		}
		int netmask = 0;
		String result;
		try {
			int pos = value.indexOf("/");
			if (pos > 0) {
				netmask = Integer.parseInt(value.substring(pos + 1));
				value = value.substring(0, pos);
			}
			if (value.indexOf(":") >= 0) {
				result = normalizeIPv6(value);
				if (netmask == 0) {
					return "'" + result + "/128" + "'";
				} else {
					return "'" + result + "/" + netmask + "'";
				}
			} else {
				result = normalizeIPv4(value);
				if (netmask == 0) {
					return "'" + result + "/32" + "'";
				} else {
					return "'" + result + "/" + netmask + "'";
				}
			}
		} catch (NumberFormatException nfe) {
			throw new XDBServerException("Invalid cidr format: "
					+ anInputString);
		}
	}

	/**
	 *
	 *
	 *
	 * @param anExp1
	 *
	 * @param anExp2
	 *
	 * @return
	 *
	 */

	public static boolean checkCompatibilityForUnion(SqlExpression anExp1,
			SqlExpression anExp2) {
		if (anExp1.getExprDataType() == null
				|| anExp2.getExprDataType() == null) {
			return true;
		}
		int expType1 = anExp1.getExprDataType().type;
		int expType2 = anExp2.getExprDataType().type;
		switch (expType1) {
		case ExpressionType.CHAR_TYPE:
		case ExpressionType.VARCHAR_TYPE:
			switch (expType2) {
			case ExpressionType.VARCHAR_TYPE:
			case ExpressionType.CHAR_TYPE:
				return true;
			default:
				return false;
			}
		case ExpressionType.INTERVAL_TYPE:
			switch (expType2) {
			case ExpressionType.INTERVAL_TYPE:
				return true;
			default:
				return false;
			}

		case ExpressionType.TIME_TYPE:
			switch (expType2) {
			case ExpressionType.TIME_TYPE:
				return true;
			default:
				return false;
			}
		case ExpressionType.TIMESTAMP_TYPE:
			switch (expType2) {
			case ExpressionType.TIMESTAMP_TYPE:
				return true;
			default:
				return false;
			}
		case ExpressionType.DATE_TYPE:
			switch (expType2) {
			case ExpressionType.DATE_TYPE:
				return true;
			default:
				return false;
			}
		case ExpressionType.DECIMAL_TYPE:
		case ExpressionType.NUMERIC_TYPE:
		case ExpressionType.REAL_TYPE:
		case ExpressionType.DOUBLEPRECISION_TYPE:
		case ExpressionType.INT_TYPE:
		case ExpressionType.SMALLINT_TYPE:
			switch (expType2) {
			case ExpressionType.DECIMAL_TYPE:
			case ExpressionType.NUMERIC_TYPE:
			case ExpressionType.REAL_TYPE:
			case ExpressionType.DOUBLEPRECISION_TYPE:
			case ExpressionType.INT_TYPE:
			case ExpressionType.SMALLINT_TYPE:
				return true;
			default:
				return false;
			}
		case ExpressionType.BOOLEAN_TYPE:
			switch (expType2) {
			case ExpressionType.BOOLEAN_TYPE:
				return true;
			default:
				return false;
			}

		default:
			return true;
		}
	}

	/**
	 * Replaces the AttributeColumn in a SqlExpression with an equivalent one
	 * found in columnList. This helps with duplicated column names.
	 *
	 * @param columnList
	 *            The query's projection column list
	 */
	protected void replaceColumnInExpression(List<AttributeColumn> columnList) {
		final String method = "replaceColumnInExpression";
		logger.entering(method);

		try {
			for (SqlExpression aSqlExpr : getNodes(this,
					SqlExpression.SQLEX_COLUMN)) {
				AttributeColumn column = aSqlExpr.getColumn();

				// skip if we already have equivalency
				if (columnList.contains(column)) {
					return;
				}

				// Now, see if we have something equivalent in list
				for (AttributeColumn projAC : columnList) {
					if (column.isEquivalent(projAC)
							&& column.relationNode == projAC.relationNode) {
						// these appear to be interchangable, just use one
						// instance
						aSqlExpr.setColumn(projAC);
					}
				}
			}
		} finally {
			logger.exiting(method);
		}
	}

	/**
	 * @return whether or not this expression contains a subquery.
	 */
	public boolean hasSubQuery() {
		switch (getExprType()) {
		case SQLEX_SUBQUERY:
			return true;
		case SQLEX_FUNCTION:
			for (SqlExpression param : functionParams) {
				if (param.hasSubQuery()) {
					return true;
				}
			}
			return false;
		case SQLEX_CASE:
			if (getCaseConstruct().getDefaultexpr().hasSubQuery()) {
				return true;
			}
			for (Map.Entry<QueryCondition, SqlExpression> caseEntry : getCaseConstruct().aHashtable
					.entrySet()) {
				if (!caseEntry.getKey().isSimple()
						|| caseEntry.getValue().hasSubQuery()) {
					return true;
				}
			}
			return false;
		case SQLEX_CONDITION:
			return getQueryCondition().isSimple();
		default:
			return false;
		}
	}

	/**
	 * @return a converted constant value, so we can evaluate consistently for
	 *         partitioning.
	 */
	public String getNormalizedValue() {
		if (exprType == SQLEX_CONSTANT) {
			if (constantValue != null) {
				String normalized = constantValue.trim();
				// constants may come in as 'constant'- strip single quotes
				while (normalized.length() > 1 && normalized.startsWith("'")
						&& normalized.endsWith("'")) {
					normalized = normalized.substring(1,
							normalized.length() - 1);
				}
				// constant may be a negative number, prepend the minus sign
				if ("-".equals(unaryOperator)) {
					normalized = unaryOperator + normalized;
				}
				Number aNumber = null;
				switch (exprDataType.type) {
				// Integer data types
				case Types.BIGINT:
					// fall through
				case Types.INTEGER:
					// fall through
				case Types.SMALLINT:
					// fall through
				case Types.TINYINT:
					// fall through
					// Decimal types
				case Types.DECIMAL:
					// fall through
				case Types.NUMERIC:
					// fall through
					// Float point types
				case Types.DOUBLE:
					// fall through
				case Types.FLOAT:
					// fall through
				case Types.REAL:
					aNumber = new BigDecimal(normalized);
					return normalizeNumber(aNumber.toString(),
							exprDataType.scale);
					// Date/time data types
				case Types.DATE:
					return normalizeDate(constantValue);
				case Types.TIME:
					return normalizeTime(constantValue);
				case Types.TIMESTAMP:
					return normalizeTimeStamp(constantValue);
					// Text data types
				case Types.CHAR:
					// fall through
				case Types.VARCHAR:
					if (getExprDataType().length > 0
							&& normalized.length() > getExprDataType().length) {
						normalized = normalized.substring(0,
								getExprDataType().length);
					}
					return normalized;
					// not supported, or undecided leave "as is"
				case ExpressionType.MACADDR_TYPE:
					return normalizeMacaddr(constantValue);
				case ExpressionType.INET_TYPE:
					return normalizeInet(constantValue);
				case ExpressionType.CIDR_TYPE:
					return normalizeCidr(constantValue);
				case Types.ARRAY:
					// fall through
				case Types.BINARY:
					// fall through
				case Types.BIT:
					// fall through
				case Types.BLOB:
					// fall through
				case Types.BOOLEAN:
					// fall through
				case Types.CLOB:
					// fall through
				case Types.DATALINK:
					// fall through
				case Types.DISTINCT:
					// fall through
				case Types.JAVA_OBJECT:
					// fall through
				case Types.LONGVARBINARY:
					// fall through
				case Types.LONGVARCHAR:
					// fall through
				case Types.NULL:
					// fall through
				case Types.OTHER:
					// fall through
				case Types.REF:
					// fall through
				case Types.STRUCT:
					// fall through
				case Types.VARBINARY:
					// fall through
				default:
				}
			}
		}
		return constantValue;
	}

	/**
	 *
	 * Creates an SqlExpression of type SQLEX_FUNCTION
	 *
	 * @param functionName
	 *            Name of the function
	 * @param functionId
	 *            IFunction.FunctionId of the function
	 *
	 * @return SqlExpression of type SQLEX_FUNCTION for the input parameters
	 *
	 */
	public static SqlExpression createNewTempFunction(String functionName,
			int functionId) {
		SqlExpression aNewSE = new SqlExpression();
		aNewSE.setExprType(SqlExpression.SQLEX_FUNCTION);
		aNewSE.functionName = functionName;
		aNewSE.functionId = functionId;
		aNewSE.setTempExpr(true);

		return aNewSE;
	}

	/**
	 *
	 * Creates an SqlExpression of type SQLEX_OPERATOR_EXPRESSION
	 *
	 * @param op
	 *            Operator
	 * @param leftExpr
	 *            Left operand
	 * @param rightExpr
	 *            Right operand
	 *
	 * @return SqlExpression of type SQLEX_OPERATOR_EXPRESSION for the input
	 *         parameters
	 *
	 */
	public static SqlExpression createNewTempOpExpression(String op,
			SqlExpression leftExpr, SqlExpression rightExpr) {
		SqlExpression aNewSE = new SqlExpression();
		aNewSE.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
		aNewSE.operator = op;
		aNewSE.leftExpr = leftExpr;
		aNewSE.rightExpr = rightExpr;
		aNewSE.setTempExpr(true);

		return aNewSE;
	}

	/**
	 *
	 * Creates an SqlExpression of type SQLEX_CONSTANT
	 *
	 * @param value
	 *            Constant value
	 * @param exprType
	 *            Desired expression type
	 *
	 * @return SqlExpression of type SQLEX_CONSTANT for the input parameters
	 *
	 */
	public static SqlExpression createConstantExpression(String value,
			ExpressionType exprType) {
		SqlExpression expr = new SqlExpression();
		expr.setExprType(SQLEX_CONSTANT);
		expr.setExprDataType(exprType);
		expr.setConstantValue(value);
		return expr;
	}

	/**
	 *
	 * Checks expression to see if it contains any marked as deferred, for
	 * distinct aggregates
	 *
	 * @return Collection of deferred expressions
	 *
	 */
	public Collection<SqlExpression> getDeferredExpressions() {

		ArrayList<SqlExpression> list = new ArrayList<SqlExpression>();

		if (this.isDeferredGroup()) {
			list.add(this);
		} else if ((this.getExprType() & SQLEX_FUNCTION) > 0) {
			for (SqlExpression aSqlExpression : this.functionParams) {
				list.addAll(aSqlExpression.getDeferredExpressions());
			}
		} else if ((this.getExprType() & SQLEX_CASE) > 0) {
			for (SqlExpression aSqlExpression : caseConstruct
					.getSQLExpressions()) {
				list.addAll(aSqlExpression.getDeferredExpressions());
			}
		} else if ((this.getExprType() & SQLEX_SUBQUERY) > 0) {
			if ((this.subqueryTree.getQueryType() & QueryTree.SCALAR) == 0) {
				for (SqlExpression aSqlExpression : this.parentContainerNode
						.getProjectionList()) {
					list.addAll(aSqlExpression.getDeferredExpressions());
				}
			}
		}

		if (this.leftExpr != null) {
			list.addAll(leftExpr.getDeferredExpressions());
		}
		if (this.rightExpr != null) {
			list.addAll(rightExpr.getDeferredExpressions());
		}

		return list;
	}

	/**
	 * @param aggAlias
	 *            the aggAlias to set
	 */
	public void setAggAlias(String aggAlias) {
		this.aggAlias = aggAlias;
	}

	/**
	 * @return the aggAlias
	 */
	public String getAggAlias() {
		return aggAlias;
	}

	/**
	 * @param alias
	 *            the alias to set
	 */
	public void setAlias(String alias) {
		this.alias = alias;
	}

	/**
	 * @return the alias
	 */
	public String getAlias() {
		return alias;
	}

	/**
	 * @param aQueryCondition
	 *            the aQueryCondition to set
	 */
	public void setQueryCondition(QueryCondition aQueryCondition) {
		this.aQueryCondition = aQueryCondition;
	}

	/**
	 * @return the aQueryCondition
	 */
	public QueryCondition getQueryCondition() {
		return aQueryCondition;
	}

	/**
	 * @param argSeparator
	 *            the argSeparator to set
	 */
	public void setArgSeparator(String argSeparator) {
		this.argSeparator = argSeparator;
	}

	/**
	 * @return the argSeparator
	 */
	public String getArgSeparator() {
		return argSeparator;
	}

	/**
	 * @return the caseConstruct
	 */
	public SCase getCaseConstruct() {
		return caseConstruct;
	}

	/**
	 * @param caseConstruct
	 *            the caseConstruct to set
	 */
	public void setCaseConstruct(SCase caseConstruct) {
		this.caseConstruct = caseConstruct;
	}

	/**
	 * @param column
	 *            the column to set
	 */
	public void setColumn(AttributeColumn column) {
		this.column = column;
	}

	/**
	 * @return the column
	 */
	public AttributeColumn getColumn() {
		return column;
	}

	/**
	 * @param constantValue
	 *            the constantValue to set
	 */
	public void setConstantValue(String constantValue) {
	    this.exprType = SQLEX_CONSTANT;
		this.constantValue = constantValue;
	}

	/**
	 * @return the constantValue
	 */
	public String getConstantValue() {
		return constantValue;
	}

	/**
	 * @param exprDataType
	 *            the exprDataType to set
	 */
	public void setExprDataType(ExpressionType exprDataType) {
		this.exprDataType = exprDataType;
	}

	/**
	 * @return the exprDataType
	 */
	public ExpressionType getExprDataType() {
		return exprDataType;
	}

	/**
	 * @param expressionList
	 *            the expressionList to set
	 */
	public void setExpressionList(List<SqlExpression> expressionList) {
		this.expressionList = expressionList;
	}

	/**
	 * @return the expressionList
	 */
	public List<SqlExpression> getExpressionList() {
		return expressionList;
	}

	/**
	 * @param exprString
	 *            the exprString to set
	 */
	public void setExprString(String exprString) {
		this.exprString = exprString;
	}

	/**
	 * @return the exprString
	 */
	public String getExprString() {
		return exprString;
	}

	/**
	 * @param exprType
	 *            the exprType to set
	 */
	public void setExprType(int exprType) {
		this.exprType = exprType;
	}

	/**
	 * @return the exprType
	 */
	public int getExprType() {
		return exprType;
	}

	/**
	 * @param expTypeOfCast
	 *            the expTypeOfCast to set
	 */
	public void setExpTypeOfCast(DataTypeHandler expTypeOfCast) {
		this.expTypeOfCast = expTypeOfCast;
	}

	/**
	 * @param functionId
	 *            the functionId to set
	 */
	public void setFunctionId(int functionId) {
		this.functionId = functionId;
	}

	/**
	 * @return the functionId
	 */
	public int getFunctionId() {
		return functionId;
	}

	/**
	 * @param functionName
	 *            the functionName to set
	 */
	public void setFunctionName(String functionName) {
		this.functionName = functionName;
	}

	/**
	 * @return the functionName
	 */
	public String getFunctionName() {
		return functionName;
	}

	/**
	 * @param functionParams
	 *            the functionParams to set
	 */
	public void setFunctionParams(List<SqlExpression> functionParams) {
		this.functionParams = functionParams;
	}

	/**
	 * @return the functionParams
	 */
	public List<SqlExpression> getFunctionParams() {
		return functionParams;
	}

	/**
	 * @param isAdded
	 *            the isAdded to set
	 */
	public void setAdded(boolean isAdded) {
		this.isAdded = isAdded;
	}

	/**
	 * @return the isAdded
	 */
	public boolean isAdded() {
		return isAdded;
	}

	/**
	 * @param isAllCountGroupFunction
	 *            the isAllCountGroupFunction to set
	 */
	public void setAllCountGroupFunction(boolean isAllCountGroupFunction) {
		this.isAllCountGroupFunction = isAllCountGroupFunction;
	}

	/**
	 * @return the isAllCountGroupFunction
	 */
	public boolean isAllCountGroupFunction() {
		return isAllCountGroupFunction;
	}

	/**
	 * @param isDeferredGroup
	 *            the isDeferredGroup to set
	 */
	public void setDeferredGroup(boolean isDeferredGroup) {
		this.isDeferredGroup = isDeferredGroup;
	}

	/**
	 * @return the isDeferredGroup
	 */
	public boolean isDeferredGroup() {
		return isDeferredGroup;
	}

	/**
	 * @param isDistinctExtraGroup
	 *            the isDistinctExtraGroup to set
	 */
	public void setDistinctExtraGroup(boolean isDistinctExtraGroup) {
		this.isDistinctExtraGroup = isDistinctExtraGroup;
	}

	/**
	 * @return the isDistinctExtraGroup
	 */
	public boolean isDistinctExtraGroup() {
		return isDistinctExtraGroup;
	}

	/**
	 * @param isDistinctGroupFunction
	 *            the isDistinctGroupFunction to set
	 */
	public void setDistinctGroupFunction(boolean isDistinctGroupFunction) {
		this.isDistinctGroupFunction = isDistinctGroupFunction;
	}

	/**
	 * @return the isDistinctGroupFunction
	 */
	public boolean isDistinctGroupFunction() {
		return isDistinctGroupFunction;
	}

	/**
	 * @return true if DISTINCT clause is on the partitioned column
	 */
	public boolean isDistinctGroupFunctionOnPartitionedCol(SysDatabase database) {
                if (isDistinctGroupFunction) {
			AttributeColumn anAC = getFunctionParams().get(0).getColumn();
			if (anAC != null) {
                            return anAC.isPartitionColumn();
			}
		}
		return false;
	}

	/**
	 * @param leftExpr
	 *            the leftExpr to set
	 */
	public void setLeftExpr(SqlExpression leftExpr) {
		this.leftExpr = leftExpr;
	}

	/**
	 * @return the leftExpr
	 */
	public SqlExpression getLeftExpr() {
		return leftExpr;
	}

	/**
	 * @param mapped
	 *            the mapped to set
	 */
	public void setMapped(int mapped) {
		this.mapped = mapped;
	}

	/**
	 * @return the mapped
	 */
	public int getMapped() {
		return mapped;
	}

	/**
	 * @param mappedExpression
	 *            the mappedExpression to set
	 */
	public void setMappedExpression(SqlExpression mappedExpression) {
		this.mappedExpression = mappedExpression;
	}

	/**
	 * @return the mappedExpression
	 */
	public SqlExpression getMappedExpression() {
		return mappedExpression;
	}

	/**
	 * @param needParenthesisInFunction
	 *            the needParenthesisInFunction to set
	 */
	public void setNeedParenthesisInFunction(boolean needParenthesisInFunction) {
		this.needParenthesisInFunction = needParenthesisInFunction;
	}

	/**
	 * @return the needParenthesisInFunction
	 */
	public boolean needParenthesisInFunction() {
		return needParenthesisInFunction;
	}

	/**
	 * @param operandSign
	 *            the operandSign to set
	 */
	public void setOperandSign(String operandSign) {
		this.operandSign = operandSign;
	}

	/**
	 * @return the operandSign
	 */
	public String getOperandSign() {
		return operandSign;
	}

	/**
	 * @param operator
	 *            the operator to set
	 */
	public void setOperator(String operator) {
		this.operator = operator;
	}

	/**
	 * @return the operator
	 */
	public String getOperator() {
		return operator;
	}

	/**
	 * @param outerAlias
	 *            the outerAlias to set
	 */
	public String setOuterAlias(String outerAlias) {
		return this.outerAlias = outerAlias;
	}

	/**
	 * @return the outerAlias
	 */
	public String getOuterAlias() {
		return outerAlias;
	}

	/**
	 * @param paramNumber
	 *            the paramNumber to set
	 */
	public void setParamNumber(int paramNumber) {
		this.paramNumber = paramNumber;
		exprString = "&xp" + paramNumber + "xp&";
	}

	/**
	 * @return the paramNumber
	 */
	public int getParamNumber() {
		return paramNumber;
	}

	/**
	 * @param paramValue
	 *            the paramValue to set
	 */
	public void setParamValue(String paramValue) {
		this.paramValue = paramValue;
		if (paramValue == null) {
			exprString = "null";
		} else if (exprDataType != null
				&& (exprDataType.type == ExpressionType.VARCHAR_TYPE
						|| exprDataType.type == ExpressionType.CHAR_TYPE
						|| exprDataType.type == ExpressionType.TIMESTAMP_TYPE
						|| exprDataType.type == ExpressionType.DATE_TYPE
						|| exprDataType.type == ExpressionType.TIME_TYPE || exprDataType.type == Types.OTHER)) {
			exprString = "'" + ParseCmdLine.escape(paramValue) + "'";
		} else {
			exprString = paramValue;
		}
	}

	/**
	 * @return the paramValue
	 */
	public String getParamValue() {
		return paramValue;
	}

	/**
	 * @param projectionLabel
	 *            the projectionLabel to set
	 */
	public void setProjectionLabel(String projectionLabel) {
		this.projectionLabel = projectionLabel;
	}

	/**
	 * @return the projectionLabel
	 */
	public String getProjectionLabel() {
		return projectionLabel;
	}

	/**
	 * @param rightExpr
	 *            the rightExpr to set
	 */
	public void setRightExpr(SqlExpression rightExpr) {
		this.rightExpr = rightExpr;
	}

	/**
	 * @return the rightExpr
	 */
	public SqlExpression getRightExpr() {
		return rightExpr;
	}

	/**
	 * @param subqueryTree
	 *            the subqueryTree to set
	 */
	public void setSubqueryTree(QueryTree subqueryTree) {
		this.subqueryTree = subqueryTree;
	}

	/**
	 * @return the subqueryTree
	 */
	public QueryTree getSubqueryTree() {
		return subqueryTree;
	}

	/**
	 * @param unaryOperator
	 *            the unaryOperator to set
	 */
	public void setUnaryOperator(String unaryOperator) {
		this.unaryOperator = unaryOperator;
	}

	/**
	 * @return the unaryOperator
	 */
	public String getUnaryOperator() {
		return unaryOperator;
	}

	// Semantic information for CASE
	public class SCase implements IRebuildString {
		private SqlExpression defaultexpr = null;

		// Contains mapping of QueryConditions
		private Hashtable<QueryCondition, SqlExpression> aHashtable;

		/**
		 *
		 * This function does a type check and then sets the datatype of the
		 * case expression to the sql expressions that we are going to get from
		 * the case expression
		 *
		 *
		 * @param database
		 *
		 * @throws org.postgresql.stado.exception.ColumnNotFoundException
		 *
		 * @return
		 *
		 */

		public ExpressionType setDataType(Command commandToExecute)
				throws ColumnNotFoundException {
			boolean isNumPresent = true;
			boolean isNumPrevious = true;
			ExpressionType toReturn = null;
			ExpressionType aExpressionType = null;
			String isPresentExpr = "";
			String isPreviousExpr = "";
			for (QueryCondition qc : aHashtable.keySet()) {
				for (QueryCondition aSqlExprCond : QueryCondition.getNodes(qc,
						QueryCondition.QC_SQLEXPR)) {
					SqlExpression aSqlExpression = aSqlExprCond.getExpr();
					SqlExpression.setExpressionResultType(aSqlExpression,
					        commandToExecute);
				}
				SqlExpression aSqlExpression = aHashtable.get(qc);
				aExpressionType = SqlExpression.setExpressionResultType(
						aSqlExpression, commandToExecute);

				// Set the expression type to the first one
				if (toReturn == null) {
					if (aExpressionType.type == ExpressionType.NULL_TYPE) {
						continue;
					}
					toReturn = aExpressionType;
					isNumPresent = aExpressionType.isNumeric();
					isPresentExpr = aSqlExpression.rebuildString();
				} else {
					isNumPrevious = isNumPresent;
					isPreviousExpr = isPresentExpr;
					isNumPresent = aExpressionType.isNumeric();
					isPresentExpr = aSqlExpression.rebuildString();
					if (isNumPrevious != isNumPresent) {
						String errorMessage = ErrorMessageRepository.CASE_STATEMENT_TYPE_MISMATCH
								+ "("
								+ isPreviousExpr
								+ " <--> "
								+ isPresentExpr + ")";

						throw new XDBServerException(
								errorMessage,
								0,
								ErrorMessageRepository.CASE_STATEMENT_TYPE_MISMATCH_CODE);
					}
				}
			}

			if (defaultexpr != null) {
				aExpressionType = SqlExpression.setExpressionResultType(
						defaultexpr, commandToExecute);
				if (toReturn != null
						&& aExpressionType.type != ExpressionType.NULL_TYPE) {
					boolean defExprType = aExpressionType.isNumeric();
					if (defExprType == isNumPresent) {
						return aExpressionType;
					} else {
						String errorMessage = ErrorMessageRepository.CASE_STATEMENT_TYPE_MISMATCH
								+ "("
								+ isPresentExpr
								+ " <--> "
								+ defaultexpr.rebuildString() + ")";
						throw new XDBServerException(
								errorMessage,
								0,
								ErrorMessageRepository.CASE_STATEMENT_TYPE_MISMATCH_CODE);

					}
				}
			}
			return toReturn == null ? aExpressionType /* NULL_TYPE */
			: toReturn;
		}

		/**
		 *
		 *
		 *
		 * @return
		 *
		 */

		public Collection<SqlExpression> getSQLExpressions() {
			Collection<SqlExpression> exprList = new LinkedList<SqlExpression>();
			if (aHashtable != null) {
				for (QueryCondition qc : aHashtable.keySet()) {
					for (QueryCondition qcSqlExpr : QueryCondition.getNodes(qc,
							QueryCondition.QC_SQLEXPR)) {
						exprList.add(qcSqlExpr.getExpr());
					}
				}
				exprList.addAll(aHashtable.values());
			}
			if (defaultexpr != null) {
				exprList.add(defaultexpr);
			}
			return exprList;
		}

		public SCase() {
			aHashtable = new Hashtable<QueryCondition, SqlExpression>();

		}

		public Map<QueryCondition, SqlExpression> getCases() {
			return aHashtable;
		}

		/**
		 *
		 *
		 *
		 * @param qc
		 *
		 * @param aSqlExpression
		 *
		 */

		public void addCase(QueryCondition qc, SqlExpression aSqlExpression) {
			aHashtable.put(qc, aSqlExpression);
		}

		/**
		 *
		 *
		 *
		 * @param qc
		 *
		 * @return
		 *
		 */

		public SqlExpression getCaseSqlExpr(QueryCondition qc) {
			return aHashtable.get(qc);
		}

		/**
		 *
		 *
		 *
		 * @return
		 *
		 */

		public String rebuildString() {
			if (aHashtable == null) {
				return "";
			}

			String caseExprString = "";
			caseExprString = "( CASE ";
			for (QueryCondition qc : aHashtable.keySet()) {
				qc.rebuildCondString();
				SqlExpression aSqlExpression = getCaseSqlExpr(qc);
				aSqlExpression.rebuildExpression();
				caseExprString = caseExprString + "  WHEN ( "
						+ qc.getCondString() + " )  THEN ( "
						+ aSqlExpression.getExprString() + " )  ";
			}
			if (defaultexpr != null) {
				defaultexpr.rebuildExpression();
				caseExprString = caseExprString + " else "
						+ defaultexpr.getExprString();
			}
			caseExprString += " end)";

			return caseExprString;
		}

		/**
		 * @param defaultexpr
		 *            the defaultexpr to set
		 */
		public void setDefaultexpr(SqlExpression defaultexpr) {
			this.defaultexpr = defaultexpr;
		}

		/**
		 * @return the defaultexpr
		 */
		public SqlExpression getDefaultexpr() {
			return defaultexpr;
		}

	}

	/**
	 * @param AttributeColumn
	 *            the column to search for
	 *
	 * @return true if the column is present in this expression
	 */
	public boolean hasColumnInExpression(AttributeColumn col) {
		switch (this.exprType) {
		case SQLEX_FUNCTION: {
			if (functionParams != null) {
				int len = functionParams.size();
				for (int index = 0; index < len; index++) {
					SqlExpression aSqlExpr = functionParams.get(index);
					if (aSqlExpr.hasColumnInExpression(col)) {
						return true;
					}
				}
			}
			break;
		}
		case SQLEX_COLUMN: {
			if (column.columnName == col.columnName
					&& (col.getTableAlias().equals("") || column.getTableName() == col
							.getTableName())) {
				return true;
			}
		}
		}
		return false;
	}

	/**
	 * @param AttributeColumn
	 *            the column to compare
	 *
	 * @return
	 */
	public boolean isAlliasSameAsColumnNameInFunction(AttributeColumn col) {
		if (exprType == SQLEX_FUNCTION) {
			if (functionParams != null) {
				int len = functionParams.size();
				for (int index = 0; index < len; index++) {
					SqlExpression aSqlExpr = functionParams.get(index);
					if (aSqlExpr.hasColumnInExpression(col)) {
						return true;
					}
				}
			}
		}
		return false;
	}

	/**
	 * @param SqlExpression
	 *            to compare
	 *
	 * @return
	 */
	public boolean isSameFunction(SqlExpression aSqlExpression) {
		if (this.exprType == SqlExpression.SQLEX_FUNCTION
				&& aSqlExpression.exprType == SqlExpression.SQLEX_FUNCTION) {
			if (this.functionId == aSqlExpression.functionId) {
				if (this.outerAlias.equalsIgnoreCase(aSqlExpression.outerAlias)) {
					if (this.exprString
							.equalsIgnoreCase(aSqlExpression.exprString)) {
						return true;
					}
				}
			}
		}
		return false;
	}

	/**
	 *
	 * @return true if this is a column
	 */
	public boolean isColumn() {
		return exprType == SqlExpression.SQLEX_COLUMN;
	}

	/**
     *
     */

	public void moveDown(RelationNode node) {
		for (SqlExpression expr : getNodes(this, SQLEX_COLUMN)) {
			if (expr.mappedExpression != null && expr.column != null
					&& node.equals(expr.column.relationNode)) {
				copy(expr.mappedExpression, expr);
			}
		}
	}
        
        /*
         * 
         */
        public void setIsProjection(boolean isProjection) {
            this.isProjection = isProjection;
        }

        /*
         * 
         */
        public boolean isProjection() {
            return isProjection;
        }
}
