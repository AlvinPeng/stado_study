/* Generated by JTB 1.4.4 */
package org.postgresql.stado.parser.core.visitor;

import org.postgresql.stado.parser.core.syntaxtree.*;
import java.util.*;

public class DepthFirstVoidVisitor implements IVoidVisitor {


  public void visit(final NodeChoice n) {
    n.choice.accept(this);
    return;
  }

  public void visit(final NodeList n) {
    for (final Iterator<INode> e = n.elements(); e.hasNext();) {
      e.next().accept(this);
    }
    return;
  }

  public void visit(final NodeListOptional n) {
    if (n.present()) {
      for (final Iterator<INode> e = n.elements(); e.hasNext();) {
        e.next().accept(this);
        }
      return;
    } else
      return;
  }

  public void visit(final NodeOptional n) {
    if (n.present()) {
      n.node.accept(this);
      return;
    } else
    return;
  }

  public void visit(final NodeSequence n) {
    for (final Iterator<INode> e = n.elements(); e.hasNext();) {
      e.next().accept(this);
    }
    return;
  }

  public void visit(final NodeToken n) {
    @SuppressWarnings("unused")
    final String tkIm = n.tokenImage;
    return;
  }

  public void visit(final numberValue n) {
    n.f0.accept(this);
  }

  public void visit(final stringLiteral n) {
    n.f0.accept(this);
  }

  public void visit(final UnreservedWords n) {
    n.f0.accept(this);
  }

  public void visit(final Identifier n) {
    n.f0.accept(this);
  }

  public void visit(final process n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final CopyData n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
  }

  public void visit(final FormatDefOIDS n) {
    n.f0.accept(this);
  }

  public void visit(final FormatDefDelimiter n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final FormatDefNull n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final FormatDefCSV n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final AddNodeToDB n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
  }

  public void visit(final DropNodeFromDB n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
  }

  public void visit(final StartDatabase n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
  }

  public void visit(final StopDatabase n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final ShutdownXDB n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final CreateDatabase n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
    n.f5.accept(this);
    n.f6.accept(this);
    n.f7.accept(this);
  }

  public void visit(final DropDatabase n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final CreateNode n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
  }

  public void visit(final FormatDefPort n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final FormatDefUser n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final FormatDefPassword n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final ExecDirect n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
  }

  public void visit(final Explain n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final VacuumDatabase n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final AnalyzeDatabase n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final CreateTablespace n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
  }

  public void visit(final DropTablespace n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final TablespaceLocation n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
  }

  public void visit(final UpdateStats n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final RenameTable n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
  }

  public void visit(final DropIndex n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
  }

  public void visit(final Alter n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final AlterTableSpace n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
  }

  public void visit(final AlterTable n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
  }

  public void visit(final AlterTableActon n) {
    n.f0.accept(this);
  }

  public void visit(final Inherit n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final SetTablespace n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final SetProperty n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final IsolationLevel n) {
    n.f0.accept(this);
  }

  public void visit(final ShowProperty n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final OwnerDef n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final Constraint n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final AddDef n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final DropDef n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final RenameDef n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
  }

  public void visit(final AlterDef n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final AlterDefOperation n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final AlterDefOperationType n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final AlterDefOperationSet n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final DropDefaultNotNull n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final Delete n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
  }

  public void visit(final UpdateTable n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
    n.f5.accept(this);
  }

  public void visit(final SetUpdateClause n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
  }

  public void visit(final createIndex n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
    n.f5.accept(this);
    n.f6.accept(this);
    n.f7.accept(this);
    n.f8.accept(this);
    n.f9.accept(this);
    n.f10.accept(this);
    n.f11.accept(this);
  }

  public void visit(final columnListIndexSpec n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final createTable n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
  }

  public void visit(final OnCommitClause n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final WithXRowID n) {
    n.f0.accept(this);
  }

  public void visit(final tablespaceDef n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final inheritsDef n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
  }

  public void visit(final createView n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
    n.f5.accept(this);
  }

  public void visit(final ColumnNameListWithParenthesis n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final DropView n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final dropTable n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
  }

  public void visit(final InsertTable n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
  }

  public void visit(final PrimaryKeyDef n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
  }

  public void visit(final CheckDef n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
  }

  public void visit(final CreateDefinition n) {
    n.f0.accept(this);
  }

  public void visit(final ColumnDeclare n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
    n.f5.accept(this);
  }

  public void visit(final ForeignKeyDef n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
    n.f5.accept(this);
    n.f6.accept(this);
    n.f7.accept(this);
    n.f8.accept(this);
  }

  public void visit(final DefaultSpec n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final PartitionDeclare n) {
    n.f0.accept(this);
  }

  public void visit(final PartitionChoice n) {
    n.f0.accept(this);
  }

  public void visit(final NodePartitionList n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final types n) {
    n.f0.accept(this);
  }

  public void visit(final DatetimeField n) {
    n.f0.accept(this);
  }

  public void visit(final IntervalQualifier n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final IntervalDataType n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final TextDataType n) {
    n.f0.accept(this);
  }

  public void visit(final BLOBDataType n) {
    n.f0.accept(this);
  }

  public void visit(final BitDataType n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final VarBitDataType n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final MacAddrDataType n) {
    n.f0.accept(this);
  }

  public void visit(final CidrDataType n) {
    n.f0.accept(this);
  }

  public void visit(final InetDataType n) {
    n.f0.accept(this);
  }

  public void visit(final GeometryDataType n) {
    n.f0.accept(this);
  }

  public void visit(final Box2DDataType n) {
    n.f0.accept(this);
  }

  public void visit(final Box3DDataType n) {
    n.f0.accept(this);
  }

  public void visit(final Box3DExtentDataType n) {
    n.f0.accept(this);
  }

  public void visit(final RegClassDataType n) {
    n.f0.accept(this);
  }

  public void visit(final BooleanDataType n) {
    n.f0.accept(this);
  }

  public void visit(final SmallIntDataType n) {
    n.f0.accept(this);
  }

  public void visit(final BigIntDataType n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final SerialDataType n) {
    n.f0.accept(this);
  }

  public void visit(final BigSerialDataType n) {
    n.f0.accept(this);
  }

  public void visit(final RealDataType n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final IntegerDataType n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final FloatDataType n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final TimeStampDataType n) {
    n.f0.accept(this);
  }

  public void visit(final TimeDataType n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final DateDataType n) {
    n.f0.accept(this);
  }

  public void visit(final VarCharDataType n) {
    n.f0.accept(this);
  }

  public void visit(final NumericDataType n) {
    n.f0.accept(this);
  }

  public void visit(final DecimalDataType n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final FixedDataType n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final NationalCharDataType n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final DoublePrecision n) {
    n.f0.accept(this);
  }

  public void visit(final CharachterDataType n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final LengthSpec n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final UnsignedZeroFillSpecs n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final PrecisionSpec n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
  }

  public void visit(final SelectAddGeometryColumn n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
    n.f5.accept(this);
    n.f6.accept(this);
    n.f7.accept(this);
    n.f8.accept(this);
    n.f9.accept(this);
    n.f10.accept(this);
    n.f11.accept(this);
    n.f12.accept(this);
    n.f13.accept(this);
    n.f14.accept(this);
  }

  public void visit(final SelectWithParenthesis n) {
    n.f0.accept(this);
  }

  public void visit(final Select n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
  }

  public void visit(final SelectWithoutOrderWithParenthesis n) {
    n.f0.accept(this);
  }

  public void visit(final SelectWithoutOrder n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final SelectWithoutOrderAndSetWithParenthesis n) {
    n.f0.accept(this);
  }

  public void visit(final UnionSpec n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final WithList n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final WithDef n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
    n.f5.accept(this);
  }

  public void visit(final WithSelect n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final SelectWithoutOrderAndSet n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
    n.f5.accept(this);
    n.f6.accept(this);
    n.f7.accept(this);
  }

  public void visit(final SelectList n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final SelectTupleSpec n) {
    n.f0.accept(this);
  }

  public void visit(final SQLSimpleExpression n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final SQLPrecedenceLevel1Expression n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final SQLPrecedenceLevel1Operand n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final SQLPrecedenceLevel2Expression n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final SQLPrecedenceLevel2Operand n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final SQLPrecedenceLevel3Expression n) {
    n.f0.accept(this);
  }

  public void visit(final SQLPrecedenceLevel3Operand n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final SQLPrimaryExpression n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
  }

  public void visit(final IsNullExpression n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final IsBooleanExpression n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final PreparedStmtParameter n) {
    n.f0.accept(this);
  }

  public void visit(final TimeStampLiteral n) {
    n.f0.accept(this);
  }

  public void visit(final TimeLiteral n) {
    n.f0.accept(this);
  }

  public void visit(final DateLiteral n) {
    n.f0.accept(this);
  }

  public void visit(final IntervalLiterals n) {
    n.f0.accept(this);
  }

  public void visit(final TextLiterals n) {
    n.f0.accept(this);
  }

  public void visit(final NullLiterals n) {
    n.f0.accept(this);
  }

  public void visit(final booleanLiteral n) {
    n.f0.accept(this);
  }

  public void visit(final binaryLiteral n) {
    n.f0.accept(this);
  }

  public void visit(final hex_decimalLiteral n) {
    n.f0.accept(this);
  }

  public void visit(final IntegerLiteral n) {
    n.f0.accept(this);
  }

  public void visit(final MacaddrLiteral n) {
    n.f0.accept(this);
  }

  public void visit(final CidrLiteral n) {
    n.f0.accept(this);
  }

  public void visit(final InetLiteral n) {
    n.f0.accept(this);
  }

  public void visit(final GeometryLiteral n) {
    n.f0.accept(this);
  }

  public void visit(final PseudoColumn n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
    n.f5.accept(this);
    n.f6.accept(this);
    n.f7.accept(this);
    n.f8.accept(this);
  }

  public void visit(final SQLArgumentList n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final SQLArgument n) {
    n.f0.accept(this);
  }

  public void visit(final CharString n) {
    n.f0.accept(this);
  }

  public void visit(final FunctionCall n) {
    n.f0.accept(this);
  }

  public void visit(final Func_Custom n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
  }

  public void visit(final Func_NullIf n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
    n.f5.accept(this);
  }

  public void visit(final Func_Coalesce n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
  }

  public void visit(final Func_ClockTimeStamp n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final Func_StatementTimeStamp n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final Func_TransactionTimeStamp n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final Func_CurrentDatabase n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final Func_CurrentSchema n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final Func_Version n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final Func_BitAnd n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
  }

  public void visit(final Func_BitOr n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
  }

  public void visit(final Func_BoolAnd n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
  }

  public void visit(final Func_BoolOr n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
  }

  public void visit(final Func_CorrCov n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
    n.f5.accept(this);
  }

  public void visit(final Func_Regr n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
    n.f5.accept(this);
  }

  public void visit(final Func_Substring n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
    n.f5.accept(this);
  }

  public void visit(final Func_Position n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
    n.f5.accept(this);
  }

  public void visit(final Func_Overlay n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
    n.f5.accept(this);
    n.f6.accept(this);
    n.f7.accept(this);
    n.f8.accept(this);
  }

  public void visit(final Func_Convert n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
    n.f5.accept(this);
    n.f6.accept(this);
  }

  public void visit(final Func_Extract n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
  }

  public void visit(final Func_User n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final Func_Case n) {
    n.f0.accept(this);
  }

  public void visit(final Func_Trim n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
    n.f5.accept(this);
    n.f6.accept(this);
  }

  public void visit(final Func_Avg n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
  }

  public void visit(final Func_Count n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
  }

  public void visit(final Func_Max n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
  }

  public void visit(final Func_Min n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
  }

  public void visit(final Func_Stdev n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
  }

  public void visit(final Func_Variance n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
  }

  public void visit(final Func_Sum n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
  }

  public void visit(final length n) {
    n.f0.accept(this);
  }

  public void visit(final position n) {
    n.f0.accept(this);
  }

  public void visit(final TableColumn n) {
    n.f0.accept(this);
  }

  public void visit(final extendbObject n) {
    n.f0.accept(this);
  }

  public void visit(final SelectAliasSpec n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final AliasName n) {
    n.f0.accept(this);
  }

  public void visit(final SQLComplexExpression n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final SQLAndExpression n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final SQLORExpression n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final SQLAndExp n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final SQLUnaryLogicalExpression n) {
    n.f0.accept(this);
  }

  public void visit(final SQLCondResult n) {
    n.f0.accept(this);
  }

  public void visit(final ExistsClause n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
  }

  public void visit(final SQLRelationalExpression n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final SQLRelationalOperatorExpression n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final Relop n) {
    n.f0.accept(this);
  }

  public void visit(final SQLInClause n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
  }

  public void visit(final SQLBetweenClause n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
  }

  public void visit(final SQLLikeClause n) {
    n.f0.accept(this);
  }

  public void visit(final SubQuery n) {
    n.f0.accept(this);
  }

  public void visit(final IsNullClause n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final IsBooleanClause n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final IntoClause n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
  }

  public void visit(final FromClause n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final TableSpec n) {
    n.f0.accept(this);
  }

  public void visit(final TableList n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final FromTableSpec n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final JoinSpec n) {
    n.f0.accept(this);
  }

  public void visit(final WhereClause n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final GroupByClause n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final SQLExpressionList n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final SQLExpressionListItem n) {
    n.f0.accept(this);
  }

  public void visit(final HavingClause n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final OrderByClause n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final LimitClause n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final OffsetClause n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final OrderByItem n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final Func_PgCurrentDate n) {
    n.f0.accept(this);
  }

  public void visit(final Func_PgCurrentTime n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final Func_PgCurrentTimeStamp n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final Func_Cast n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
    n.f5.accept(this);
  }

  public void visit(final ColumnNameList n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final TableName n) {
    n.f0.accept(this);
  }

  public void visit(final FloatingPointNumber n) {
    n.f0.accept(this);
  }

  public void visit(final ShowAgents n) {
    n.f0.accept(this);
  }

  public void visit(final ShowCluster n) {
    n.f0.accept(this);
  }

  public void visit(final ShowDatabases n) {
    n.f0.accept(this);
  }

  public void visit(final ShowStatements n) {
    n.f0.accept(this);
  }

  public void visit(final ShowTables n) {
    n.f0.accept(this);
  }

  public void visit(final ShowTranIsolation n) {
    n.f0.accept(this);
  }

  public void visit(final BeginTransaction n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final CommitTransaction n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final RollbackTransaction n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final DescribeTable n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final ShowConstraints n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final ShowIndexes n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final ShowUsers n) {
    n.f0.accept(this);
  }

  public void visit(final ShowViews n) {
    n.f0.accept(this);
  }

  public void visit(final Deallocate n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final CreateUser n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
    n.f5.accept(this);
  }

  public void visit(final DropUser n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final AlterUser n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
  }

  public void visit(final Grantee n) {
    n.f0.accept(this);
  }

  public void visit(final GranteeList n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final TableListForGrant n) {
    n.f0.accept(this);
  }

  public void visit(final Privilege n) {
    n.f0.accept(this);
  }

  public void visit(final PrivilegeList n) {
    n.f0.accept(this);
  }

  public void visit(final Grant n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
    n.f5.accept(this);
    n.f6.accept(this);
  }

  public void visit(final Revoke n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
    n.f5.accept(this);
    n.f6.accept(this);
  }

  public void visit(final Cluster n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final Truncate n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
  }

  public void visit(final Kill n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final Unlisten n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final DeclareCursor n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
    n.f4.accept(this);
  }

  public void visit(final CloseCursor n) {
    n.f0.accept(this);
    n.f1.accept(this);
  }

  public void visit(final FetchCursor n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
  }

  public void visit(final AlterCluster n) {
    n.f0.accept(this);
    n.f1.accept(this);
    n.f2.accept(this);
    n.f3.accept(this);
  }

}
