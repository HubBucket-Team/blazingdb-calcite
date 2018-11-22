package com.blazingdb.calcite.plan.logical.expressions;

import java.util.ArrayList;
import java.util.Collection;

abstract class ExpressionBase implements Expression {

  private static final long serialVersionUID = -7559406120476755398L;

  private Collection<Expression> inputs;

  public ExpressionBase() { inputs = new ArrayList<Expression>(); }

  @Override
  public Expression addInput(final Expression expressionNode) {
    inputs.add(expressionNode);
    return this;
  }

  @Override
  public Collection<Expression> getInputs() {
    return inputs;
  }

  public abstract String toString();
}
