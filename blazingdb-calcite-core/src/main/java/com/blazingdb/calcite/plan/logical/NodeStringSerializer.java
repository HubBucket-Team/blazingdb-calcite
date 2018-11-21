package com.blazingdb.calcite.plan.logical;

final class NodeStringSerializer {

  private static final Integer INDENTATION_LEVEL     = 2;
  private static final char    INDENTATION_CHARACTER = ' ';

  final StringBuilder stringBuilder           = new StringBuilder();
  Integer             currentIndentationLevel = 0;

  public NodeStringSerializer(final Node node) {
    appendStringRepresentation(node);
  }

  public String toString() { return stringBuilder.toString(); }

  private void appendStringRepresentation(final Node node) {
    stringBuilderAppend(node);
    currentIndentationLevel += INDENTATION_LEVEL;
    node.getChildren().stream().forEach(this::appendStringRepresentation);
    currentIndentationLevel -= INDENTATION_LEVEL;
  }

  private void stringBuilderAppend(final Node node) {
    int counter = currentIndentationLevel + 1;
    while (0 != --counter) { stringBuilder.append(INDENTATION_CHARACTER); }
    stringBuilder.append(node);
    stringBuilder.append('\n');
  }
}
