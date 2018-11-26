package com.blazingdb.calcite.plan.logical.expressions;

import org.apache.commons.lang3.Validate;

final class CastExpression extends ExpressionBase {

  private static final long serialVersionUID = -7099715056954440929L;

  private Class<? extends Number> targetCast;

  public CastExpression() {}

  public CastExpression(final Class<? extends Number> targetCast) {
    this.targetCast = targetCast;
  }

  public Expression getValue() { return getInput(0); }

  @Override
  public String toString() {
    Validate.isTrue((1 == getInputs().size()), "a input is required");
    return "Cast";
  }
}
