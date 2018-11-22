package com.blazingdb.calcite.plan.logical.expressions;

import org.apache.commons.lang3.Validate;

final class CastExpression extends BinaryOperationExpression {

  private static final long serialVersionUID = -7099715056954440929L;

  public Expression getValue() { return getInput(0); }

  @Override
  public String toString() {
    Validate.isTrue((1 == getInputs().size()), "a input is required");
    return "Cast";
  }
}
