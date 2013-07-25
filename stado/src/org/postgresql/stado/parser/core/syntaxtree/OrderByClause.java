/* Generated by JTB 1.4.4 */
package org.postgresql.stado.parser.core.syntaxtree;

import org.postgresql.stado.parser.core.visitor.*;

public class OrderByClause implements INode {

  public NodeToken f0;

  public OrderByItem f1;

  public NodeListOptional f2;

  private static final long serialVersionUID = 144L;

  public OrderByClause(final NodeToken n0, final OrderByItem n1, final NodeListOptional n2) {
    f0 = n0;
    f1 = n1;
    f2 = n2;
  }

  public OrderByClause(final OrderByItem n0, final NodeListOptional n1) {
    f0 = new NodeToken("BY");
    f1 = n0;
    f2 = n1;
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
