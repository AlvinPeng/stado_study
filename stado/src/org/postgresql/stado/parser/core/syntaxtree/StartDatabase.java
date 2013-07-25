/* Generated by JTB 1.4.4 */
package org.postgresql.stado.parser.core.syntaxtree;

import org.postgresql.stado.parser.core.visitor.*;

public class StartDatabase implements INode {

  public NodeToken f0;

  public Identifier f1;

  public NodeListOptional f2;

  public NodeOptional f3;

  private static final long serialVersionUID = 144L;

  public StartDatabase(final NodeToken n0, final Identifier n1, final NodeListOptional n2, final NodeOptional n3) {
    f0 = n0;
    f1 = n1;
    f2 = n2;
    f3 = n3;
  }

  public StartDatabase(final Identifier n0, final NodeListOptional n1, final NodeOptional n2) {
    f0 = new NodeToken("DATABASE");
    f1 = n0;
    f2 = n1;
    f3 = n2;
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
