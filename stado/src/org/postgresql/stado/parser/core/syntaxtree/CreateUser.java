/* Generated by JTB 1.4.4 */
package org.postgresql.stado.parser.core.syntaxtree;

import org.postgresql.stado.parser.core.visitor.*;

public class CreateUser implements INode {

  public NodeToken f0;

  public NodeToken f1;

  public Identifier f2;

  public NodeToken f3;

  public Identifier f4;

  public NodeOptional f5;

  private static final long serialVersionUID = 144L;

  public CreateUser(final NodeToken n0, final NodeToken n1, final Identifier n2, final NodeToken n3, final Identifier n4, final NodeOptional n5) {
    f0 = n0;
    f1 = n1;
    f2 = n2;
    f3 = n3;
    f4 = n4;
    f5 = n5;
  }

  public CreateUser(final Identifier n0, final Identifier n1, final NodeOptional n2) {
    f0 = new NodeToken("CREATE");
    f1 = new NodeToken("USER");
    f2 = n0;
    f3 = new NodeToken("PASSWORD");
    f4 = n1;
    f5 = n2;
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
