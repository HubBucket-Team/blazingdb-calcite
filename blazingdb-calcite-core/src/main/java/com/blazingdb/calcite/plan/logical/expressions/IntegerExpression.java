package com.blazingdb.calcite.plan.logical.expressions;

final class IntegerExpression extends NumberExpression {

  private static final long serialVersionUID = -5097332124880404925L;

  private Integer value;

  public IntegerExpression(final Integer value) { this.value = value; }

  @Override
  public Integer getValue() { return value; }

  @Override
  public String toString() {
    return "Integer: " + value;
  }
}
