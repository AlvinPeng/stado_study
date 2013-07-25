/* Generated by JTB 1.4.4 */
package org.postgresql.stado.parser.core.syntaxtree;

import org.postgresql.stado.parser.core.visitor.*;

public class FetchCursor implements INode {

  public NodeToken f0;

  public NodeToken f1;

  public NodeToken f2;

  public Identifier f3;

  private static final long serialVersionUID = 144L;

  public FetchCursor(final NodeToken n0, final NodeToken n1, final NodeToken n2, final Identifier n3) {
    f0 = n0;
    f1 = n1;
    f2 = n2;
    f3 = n3;
  }

  public FetchCursor(final NodeToken n0, final Identifier n1) {
    f0 = new NodeToken("FETCH");
    f1 = n0;
    f2 = new NodeToken("FROM");
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
