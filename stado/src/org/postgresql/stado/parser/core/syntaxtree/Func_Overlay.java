/* Generated by JTB 1.4.4 */
package org.postgresql.stado.parser.core.syntaxtree;

import org.postgresql.stado.parser.core.visitor.*;

public class Func_Overlay implements INode {

  public NodeToken f0;

  public NodeToken f1;

  public SQLArgument f2;

  public NodeToken f3;

  public SQLArgument f4;

  public NodeToken f5;

  public SQLArgument f6;

  public NodeOptional f7;

  public NodeToken f8;

  private static final long serialVersionUID = 144L;

  public Func_Overlay(final NodeToken n0, final NodeToken n1, final SQLArgument n2, final NodeToken n3, final SQLArgument n4, final NodeToken n5, final SQLArgument n6, final NodeOptional n7, final NodeToken n8) {
    f0 = n0;
    f1 = n1;
    f2 = n2;
    f3 = n3;
    f4 = n4;
    f5 = n5;
    f6 = n6;
    f7 = n7;
    f8 = n8;
  }

  public Func_Overlay(final SQLArgument n0, final SQLArgument n1, final SQLArgument n2, final NodeOptional n3) {
    f0 = new NodeToken("OVERLAY");
    f1 = new NodeToken("(");
    f2 = n0;
    f3 = new NodeToken("PLACING");
    f4 = n1;
    f5 = new NodeToken("FROM");
    f6 = n2;
    f7 = n3;
    f8 = new NodeToken(")");
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
