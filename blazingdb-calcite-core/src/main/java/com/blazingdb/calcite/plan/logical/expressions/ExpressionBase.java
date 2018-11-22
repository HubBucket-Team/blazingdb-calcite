package com.blazingdb.calcite.plan.logical.expressions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

abstract class ExpressionBase implements Expression {

  private static final long serialVersionUID = -7559406120476755398L;

  private List<Expression> inputs;

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

  public Expression getInput(final int index) throws IndexOutOfBoundsException {
    return inputs.get(index);
  }

  public abstract String toString();
}
