/* Generated by JTB 1.4.4 */
package org.postgresql.stado.parser.core.syntaxtree;

import org.postgresql.stado.parser.core.visitor.*;

public class SelectWithoutOrderAndSet implements INode {

  public NodeToken f0;

  public NodeOptional f1;

  public SelectList f2;

  public NodeOptional f3;

  public NodeOptional f4;

  public NodeOptional f5;

  public NodeOptional f6;

  public NodeOptional f7;

  private static final long serialVersionUID = 144L;

  public SelectWithoutOrderAndSet(final NodeToken n0, final NodeOptional n1, final SelectList n2, final NodeOptional n3, final NodeOptional n4, final NodeOptional n5, final NodeOptional n6, final NodeOptional n7) {
    f0 = n0;
    f1 = n1;
    f2 = n2;
    f3 = n3;
    f4 = n4;
    f5 = n5;
    f6 = n6;
    f7 = n7;
  }

  public SelectWithoutOrderAndSet(final NodeOptional n0, final SelectList n1, final NodeOptional n2, final NodeOptional n3, final NodeOptional n4, final NodeOptional n5, final NodeOptional n6) {
    f0 = new NodeToken("SELECT");
    f1 = n0;
    f2 = n1;
    f3 = n2;
    f4 = n3;
    f5 = n4;
    f6 = n5;
    f7 = n6;
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
