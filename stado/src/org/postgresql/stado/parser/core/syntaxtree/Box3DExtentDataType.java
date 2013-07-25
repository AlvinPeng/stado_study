/* Generated by JTB 1.4.4 */
package org.postgresql.stado.parser.core.syntaxtree;

import org.postgresql.stado.parser.core.visitor.*;

public class Box3DExtentDataType implements INode {

  public NodeToken f0;

  private static final long serialVersionUID = 144L;

  public Box3DExtentDataType(final NodeToken n0) {
    f0 = n0;
  }

  public Box3DExtentDataType() {
    f0 = new NodeToken("BOX3DEXTENT");
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
