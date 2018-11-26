package com.blazingdb.calcite.plan.logical.expressions;

abstract class NumberExpression extends ExpressionBase {

  private static final long serialVersionUID = -3709793341211183406L;

  public abstract Number getValue();

  @Override
  public String toString() {
    return "Number*";
  }
}
