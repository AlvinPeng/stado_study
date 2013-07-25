/* Generated by JTB 1.4.4 */
package org.postgresql.stado.parser.core.syntaxtree;

import org.postgresql.stado.parser.core.visitor.*;

public class CheckDef implements INode {

  public NodeToken f0;

  public NodeToken f1;

  public SQLComplexExpression f2;

  public NodeToken f3;

  private static final long serialVersionUID = 144L;

  public CheckDef(final NodeToken n0, final NodeToken n1, final SQLComplexExpression n2, final NodeToken n3) {
    f0 = n0;
    f1 = n1;
    f2 = n2;
    f3 = n3;
  }

  public CheckDef(final SQLComplexExpression n0) {
    f0 = new NodeToken("CHECK");
    f1 = new NodeToken("(");
    f2 = n0;
    f3 = new NodeToken(")");
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
