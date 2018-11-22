package com.blazingdb.calcite.plan.logical.expressions;

import java.util.ArrayList;
import java.util.Collection;

abstract class ExpressionBase implements Expression {

  private static final long serialVersionUID = -7559406120476755398L;

  private Collection<Expression> children;

  public ExpressionBase() { children = new ArrayList<Expression>(); }

  @Override
  public Expression addChild(final Expression expressionNode) {
    children.add(expressionNode);
    return this;
  }

  @Override
  public Collection<Expression> getChildren() {
    return children;
  }

  public abstract String toString();
}
