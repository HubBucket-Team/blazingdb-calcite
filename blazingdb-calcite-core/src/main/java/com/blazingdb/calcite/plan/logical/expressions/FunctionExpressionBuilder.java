package com.blazingdb.calcite.plan.logical.expressions;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlKind;

import java.util.Optional;
import java.util.function.Supplier;

final class FunctionExpressionBuilder implements ExpressionBuilder {

  private final RexCall rexCall;

  public FunctionExpressionBuilder(final RexCall rexCall) {
    this.rexCall = rexCall;
  }

  public Optional<Expression> build()
      throws ExpressionBuilder.ExpressionBuildingException {
    return Optional.of(
        (Expression) suppliers[rexCall.getKind().ordinal()].get());
  }

  final class SqlKindFunctionBuildingException
      extends ExpressionBuilder.ExpressionBuildingException {
    private static final long serialVersionUID = -8076779235185096770L;
    public SqlKindFunctionBuildingException(final String message) {
      super(message);
    }
  }

  final class AssigningFunctionExpressionException
      extends ExpressionBuilder.ExpressionBuildingException {
    private static final long serialVersionUID = -1015905101636608945L;
    public AssigningFunctionExpressionException(final String message) {
      super(message);
    }
  }

  private static final Supplier<?>[] suppliers =
      new Supplier<?>[ SqlKind.values().length ];

  static {
    suppliers[SqlKind.EQUALS.ordinal()] = () -> new EqualsExpression();
    suppliers[SqlKind.CAST.ordinal()]   = () -> new CastExpression();
  }
}
