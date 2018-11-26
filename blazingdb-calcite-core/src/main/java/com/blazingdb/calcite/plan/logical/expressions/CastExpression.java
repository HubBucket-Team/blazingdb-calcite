package com.blazingdb.calcite.plan.logical.expressions;

import org.apache.commons.lang3.Validate;

final class CastExpression extends ExpressionBase {

  private static final long serialVersionUID = -7099715056954440929L;

  private final Class<? extends Number> target;

  public CastExpression(final Class<? extends Number> target) {
    this.target = target;
  }

  public Expression getValue() { return getInput(0); }

  public Class<? extends Number> getTarget() { return target; }

  @Override
  public String toString() {
    Validate.isTrue((1 == getInputs().size()), "a input is required");
    return "Cast";
  }
}
