package com.blazingdb.calcite.plan.logical.expressions;

final class LiteralExpression extends ExpressionBase {

  private static final long serialVersionUID = 8593615451734572217L;

  private final String digest;
  private final Comparable<?> value;

  public <T extends Comparable<T>>
  LiteralExpression(final String digest, final Comparable<T> value) {
    this.digest = digest;
    this.value  = value;
  }

  public String getDigest() { return digest; }

  public <T extends Comparable<T>> T getValueAs(Class<T> clazz) {
    return clazz.cast(value);
  }

  @Override
  public String toString() {
    return "Literal: DIGEST=" + digest;
  }
}
