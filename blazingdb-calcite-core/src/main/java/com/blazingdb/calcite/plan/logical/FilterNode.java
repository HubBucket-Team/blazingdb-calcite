package com.blazingdb.calcite.plan.logical;

import com.blazingdb.calcite.plan.logical.expressions.Expression;
import com.blazingdb.calcite.plan.logical.expressions.ExpressionStringSerializer;

final class FilterNode extends NodeBase {

  private static final long serialVersionUID = -3015864347938693253L;

  private final Expression expression;

  public FilterNode(final Expression expression) {
    this.expression = expression;
  }

  public Expression getExpression() { return expression; }

  @Override
  public String toString() {
    return "Filter : " + new ExpressionStringSerializer(expression);
  }
}
