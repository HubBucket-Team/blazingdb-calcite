package com.blazingdb.calcite.plan.logical.expressions;

import org.apache.commons.lang3.NotImplementedException;

import java.util.Collection;

final class NullExpression implements Expression {

  private static final long serialVersionUID = -1940648132242053121L;

  @Override
  public Expression addInput(final Expression expression) {
    throw new NotImplementedException("NullExpression#addInput");
  }

  @Override
  public Collection<Expression> getInputs() {
    throw new NotImplementedException("NullExpression#getInputs");
  }

  public String toString() { return "NullExpression"; }
}
