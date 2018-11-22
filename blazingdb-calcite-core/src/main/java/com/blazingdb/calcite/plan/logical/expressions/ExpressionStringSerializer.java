package com.blazingdb.calcite.plan.logical.expressions;

public final class ExpressionStringSerializer {

  private static final Integer INDENTATION_LEVEL     = 2;
  private static final char    INDENTATION_CHARACTER = ' ';

  final StringBuilder stringBuilder           = new StringBuilder();
  Integer             currentIndentationLevel = 0;

  public ExpressionStringSerializer(final Expression node) {
    appendStringRepresentation(node);
  }

  public String toString() { return stringBuilder.toString(); }

  private void appendStringRepresentation(final Expression node) {
    stringBuilderAppend(node);
    currentIndentationLevel += INDENTATION_LEVEL;
    node.getInputs().stream().forEach(this::appendStringRepresentation);
    currentIndentationLevel -= INDENTATION_LEVEL;
  }

  private void stringBuilderAppend(final Expression node) {
    int counter = currentIndentationLevel + 1;
    while (0 != --counter) { stringBuilder.append(INDENTATION_CHARACTER); }
    stringBuilder.append(node);
    stringBuilder.append('\n');
  }
}
