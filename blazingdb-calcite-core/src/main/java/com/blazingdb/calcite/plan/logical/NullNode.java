package com.blazingdb.calcite.plan.logical;

import java.util.Collection;

final class NullNode implements Node {

  private static final long serialVersionUID = -6547955683169355149L;

  public Node addChild(final Node node) {
    throw new UnsupportedOperationException();
  }

  public Collection<Node> getChildren() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    return "NullNode";
  }
}
