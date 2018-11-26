package com.blazingdb.calcite.plan.logical.expressions;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Optional;
import java.util.function.Function;

final class FunctionExpressionBuilder implements ExpressionBuilder {

  private final RexCall rexCall;

  public FunctionExpressionBuilder(final RexCall rexCall) {
    this.rexCall = rexCall;
  }

  public Optional<Expression> build()
      throws ExpressionBuilder.ExpressionBuildingException {
    return Optional.of(
        rexCallToExpressionMaps[rexCall.getKind().ordinal()].apply(rexCall));
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

  // TODO(gcca): Arrays.asList instead for mapping all kinds
  @FunctionalInterface
  private interface RexCallToExpressionMap
      extends Function<RexCall, Expression> {}

  private static final RexCallToExpressionMap[] rexCallToExpressionMaps =
      new RexCallToExpressionMap[SqlKind.values().length];

  static {
    rexCallToExpressionMaps[SqlKind.EQUALS.ordinal()] =
        FunctionExpressionBuilder::makeEqualsExpression;
    rexCallToExpressionMaps[SqlKind.CAST.ordinal()] =
        FunctionExpressionBuilder::makeCastExpression;
    rexCallToExpressionMaps[SqlKind.OR.ordinal()] =
        FunctionExpressionBuilder::makeOrExpression;
    rexCallToExpressionMaps[SqlKind.AND.ordinal()] =
        FunctionExpressionBuilder::makeAndExpression;
  }

  private static EqualsExpression makeEqualsExpression(final RexCall rexCall) {
    return new EqualsExpression();
  }

  private static CastExpression makeCastExpression(final RexCall rexCall) {
    BasicSqlType basicSqlType = (BasicSqlType) rexCall.getType();
    SqlTypeName  sqlTypeName  = basicSqlType.getSqlTypeName();

    if (sqlTypeName.equals(SqlTypeName.INTEGER)) {
      return new CastExpression(Integer.class);
    }

    if (sqlTypeName.equals(SqlTypeName.DOUBLE)) {
      return new CastExpression(Double.class);
    }

    throw new AssertionError("Unexpected sql type name");
  }

  private static OrExpression makeOrExpression(final RexCall rexCall) {
    return new OrExpression();
  }

  private static AndExpression makeAndExpression(final RexCall rexCall) {
    return new AndExpression();
  }
}
