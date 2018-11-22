package com.blazingdb.calcite.plan.logical.expressions;

import java.util.Optional;

public interface ExpressionBuilder {
  Optional<Expression> build() throws ExpressionBuildingException;

  public static class ExpressionBuildingException extends Exception {
    private static final long serialVersionUID = 3759278121982525348L;
    public ExpressionBuildingException(final String message) { super(message); }
  }
}
