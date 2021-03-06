/* Generated by JTB 1.4.4 */
package org.postgresql.stado.parser.core.syntaxtree;

import org.postgresql.stado.parser.core.visitor.*;

public class DeclareCursor implements INode {

  public NodeToken f0;

  public Identifier f1;

  public NodeToken f2;

  public NodeToken f3;

  public Select f4;

  private static final long serialVersionUID = 144L;

  public DeclareCursor(final NodeToken n0, final Identifier n1, final NodeToken n2, final NodeToken n3, final Select n4) {
    f0 = n0;
    f1 = n1;
    f2 = n2;
    f3 = n3;
    f4 = n4;
  }

  public DeclareCursor(final Identifier n0, final Select n1) {
    f0 = new NodeToken("DECLARE");
    f1 = n0;
    f2 = new NodeToken("CURSOR");
    f3 = new NodeToken("FOR");
    f4 = n1;
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
