/* Generated by JTB 1.4.4 */
package org.postgresql.stado.parser.core.syntaxtree;

import org.postgresql.stado.parser.core.visitor.*;

public class Func_Variance implements INode {

  public NodeChoice f0;

  public NodeToken f1;

  public NodeOptional f2;

  public SQLArgument f3;

  public NodeToken f4;

  private static final long serialVersionUID = 144L;

  public Func_Variance(final NodeChoice n0, final NodeToken n1, final NodeOptional n2, final SQLArgument n3, final NodeToken n4) {
    f0 = n0;
    f1 = n1;
    f2 = n2;
    f3 = n3;
    f4 = n4;
  }

  public Func_Variance(final NodeChoice n0, final NodeOptional n1, final SQLArgument n2) {
    f0 = n0;
    f1 = new NodeToken("(");
    f2 = n1;
    f3 = n2;
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
