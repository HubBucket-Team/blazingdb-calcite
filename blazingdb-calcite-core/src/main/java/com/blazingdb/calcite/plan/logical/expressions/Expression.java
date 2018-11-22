package com.blazingdb.calcite.plan.logical.expressions;

import java.util.Collection;

import java.io.Serializable;

public interface Expression extends Serializable {
  Expression             addChild(final Expression expression);
  Collection<Expression> getChildren();
}
