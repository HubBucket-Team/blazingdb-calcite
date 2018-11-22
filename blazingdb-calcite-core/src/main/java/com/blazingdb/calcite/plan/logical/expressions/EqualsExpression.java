package com.blazingdb.calcite.plan.logical.expressions;

import org.apache.commons.lang3.Validate;

import java.util.List;

final class EqualsExpression extends BinaryOperationExpression {

  private static final long serialVersionUID = 2456887263227506464L;

  private List<Expression> indexedChildren;

  public EqualsExpression() {
    indexedChildren = (List<Expression>) getInputs();
  }

  public Expression leftElementNode() { return indexedChildren.get(0); }

  public Expression rightElementNode() { return indexedChildren.get(1); }

  @Override
  public String toString() {
    Validate.isTrue((2 == getInputs().size()), "two inputs are required");
    return "Equals";
  }
}
