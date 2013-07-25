/* Generated by JTB 1.4.4 */
package org.postgresql.stado.parser.core.syntaxtree;

import org.postgresql.stado.parser.core.visitor.*;

public class SQLBetweenClause implements INode {

  public NodeOptional f0;

  public NodeToken f1;

  public SQLSimpleExpression f2;

  public NodeToken f3;

  public SQLSimpleExpression f4;

  private static final long serialVersionUID = 144L;

  public SQLBetweenClause(final NodeOptional n0, final NodeToken n1, final SQLSimpleExpression n2, final NodeToken n3, final SQLSimpleExpression n4) {
    f0 = n0;
    f1 = n1;
    f2 = n2;
    f3 = n3;
    f4 = n4;
  }

  public SQLBetweenClause(final NodeOptional n0, final SQLSimpleExpression n1, final SQLSimpleExpression n2) {
    f0 = n0;
    f1 = new NodeToken("BETWEEN");
    f2 = n1;
    f3 = new NodeToken("AND");
    f4 = n2;
  }

  public <R, A> R accept(final IRetArguVisitor<R, A> vis, final A argu) {
    return vis.visit(this, argu);
  }

  public <R> R accept(final IRetVisitor<R> vis) {
    return vis.visit(this);
  }

  public <A> void accept(final IVoidArguVisitor<A> vis, final A argu) {
    vis.visit(this, argu);
  }

  public void accept(final IVoidVisitor vis) {
    vis.visit(this);
  }

}
