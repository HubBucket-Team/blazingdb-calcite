package com.blazingdb.calcite.plan.logical;

final class FilterNodeBuilder implements NodeBuilder {

  private static final long serialVersionUID = -1694318605271393011L;

  private final FilterNode filterNode;

  public FilterNodeBuilder(final FilterNode filterNode) {
    this.filterNode = filterNode;
  }

  @Override
  public Node build() {
    return null;
  }
}
