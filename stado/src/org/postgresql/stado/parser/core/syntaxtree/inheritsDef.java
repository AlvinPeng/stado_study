/* Generated by JTB 1.4.4 */
package org.postgresql.stado.parser.core.syntaxtree;

import org.postgresql.stado.parser.core.visitor.*;

public class inheritsDef implements INode {

  public NodeToken f0;

  public NodeToken f1;

  public TableName f2;

  public NodeListOptional f3;

  public NodeToken f4;

  private static final long serialVersionUID = 144L;

  public inheritsDef(final NodeToken n0, final NodeToken n1, final TableName n2, final NodeListOptional n3, final NodeToken n4) {
    f0 = n0;
    f1 = n1;
    f2 = n2;
    f3 = n3;
    f4 = n4;
  }

  public inheritsDef(final TableName n0, final NodeListOptional n1) {
    f0 = new NodeToken("INHERITS");
    f1 = new NodeToken("(");
    f2 = n0;
    f3 = n1;
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