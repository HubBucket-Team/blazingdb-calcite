package com.blazingdb.calcite.plan.logical;

final class UnionNode extends NodeBase {

  private static final long serialVersionUID = 6727925829569575851L;

  private final boolean all;

  public UnionNode(final boolean all) { this.all = all; }

  @Override
  public String toString() {
    return "UnionNode : all = " + all;
  }
}
