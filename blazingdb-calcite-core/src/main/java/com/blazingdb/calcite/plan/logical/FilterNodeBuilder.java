package com.blazingdb.calcite.plan.logical;

import com.blazingdb.calcite.plan.logical.expressions.ExpressionRexShuttle;

import org.apache.calcite.rel.logical.LogicalFilter;

final class FilterNodeBuilder implements NodeBuilder {

  private static final long serialVersionUID = -1694318605271393011L;

  private final LogicalFilter logicalFilter;

  public FilterNodeBuilder(final LogicalFilter logicalFilter) {
    this.logicalFilter = logicalFilter;
  }

  @Override
  public FilterNode build() {
    ExpressionRexShuttle expressionRexShuttle = new ExpressionRexShuttle();
    logicalFilter.accept(expressionRexShuttle);

    FilterNode filterNode =
        new FilterNode(expressionRexShuttle.getExpressionRootNode());

    return filterNode;
  }
}
