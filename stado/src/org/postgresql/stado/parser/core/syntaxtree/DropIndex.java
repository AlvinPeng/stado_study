/* Generated by JTB 1.4.4 */
package org.postgresql.stado.parser.core.syntaxtree;

import org.postgresql.stado.parser.core.visitor.*;

public class DropIndex implements INode {

  public NodeToken f0;

  public NodeToken f1;

  public Identifier f2;

  public NodeOptional f3;

  private static final long serialVersionUID = 144L;

  public DropIndex(final NodeToken n0, final NodeToken n1, final Identifier n2, final NodeOptional n3) {
    f0 = n0;
    f1 = n1;
    f2 = n2;
    f3 = n3;
  }

  public DropIndex(final Identifier n0, final NodeOptional n1) {
    f0 = new NodeToken("DROP");
    f1 = new NodeToken("INDEX");
    f2 = n0;
    f3 = n1;
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
