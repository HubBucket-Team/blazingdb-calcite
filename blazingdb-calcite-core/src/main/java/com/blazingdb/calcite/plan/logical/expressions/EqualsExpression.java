package com.blazingdb.calcite.plan.logical.expressions;

import org.apache.commons.lang3.Validate;

final class EqualsExpression extends BinaryOperationExpression {

  private static final long serialVersionUID = 2456887263227506464L;

  public Expression leftElementNode() { return getInput(0); }

  public Expression rightElementNode() { return getInput(1); }

  @Override
  public String toString() {
    Validate.isTrue((2 == getInputs().size()), "two inputs are required");
    return "Equals";
  }
}
