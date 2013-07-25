/* Generated by JTB 1.4.4 */
package org.postgresql.stado.parser.core.syntaxtree;

import org.postgresql.stado.parser.core.visitor.*;

public class ExistsClause implements INode {

  public NodeOptional f0;

  public NodeToken f1;

  public NodeToken f2;

  public SubQuery f3;

  public NodeToken f4;

  private static final long serialVersionUID = 144L;

  public ExistsClause(final NodeOptional n0, final NodeToken n1, final NodeToken n2, final SubQuery n3, final NodeToken n4) {
    f0 = n0;
    f1 = n1;
    f2 = n2;
    f3 = n3;
    f4 = n4;
  }

  public ExistsClause(final NodeOptional n0, final SubQuery n1) {
    f0 = n0;
    f1 = new NodeToken("EXISTS");
    f2 = new NodeToken("(");
    f3 = n1;
    f4 = new NodeToken(")");
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
