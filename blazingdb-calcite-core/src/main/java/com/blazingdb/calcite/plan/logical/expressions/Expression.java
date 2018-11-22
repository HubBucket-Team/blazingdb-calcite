package com.blazingdb.calcite.plan.logical.expressions;

import java.util.Collection;

import java.io.Serializable;

public interface Expression extends Serializable {
  Expression             addInput(final Expression expression);
  Collection<Expression> getInputs();
}
