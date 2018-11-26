package com.blazingdb.calcite.plan.logical.expressions;

final class ReferenceExpression extends ExpressionBase {

  private static final long serialVersionUID = -2989808876252667649L;

  private Integer index;

  public ReferenceExpression(final Integer index) { this.index = index; }

  public Integer getIndex() { return index; }

  @Override
  public String toString() {
    return "Reference: INDEX=$" + index;
  }
}
