package com.blazingdb.calcite.application;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Arrays;
import java.util.List;

public class SqlSyntaxException extends Exception {
  private static final long serialVersionUID = -1689099602920569510L;

  private final String queryString;
  private final SqlParseException sqlParseException;

  public SqlSyntaxException(final String queryString,
                            final SqlParseException sqlParseException) {
    super();
    this.queryString = queryString;
    this.sqlParseException = sqlParseException;
  }

  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("SqlSyntaxException:\n\n");

    List<String> queryLines = Arrays.asList(queryString.split("\n"));

    builder.append(queryLines.get(0));

    builder.append('\n');
    SqlParserPos pos = sqlParseException.getPos();
    for (int i = 1; i < pos.getColumnNum(); i++) {
      builder.append(" ");
    }

    for (int i = pos.getColumnNum(); i <= pos.getEndColumnNum(); i++) {
      builder.append("^");
    }

    builder.append("\n\n");
    builder.append(sqlParseException.getMessage());

    return builder.toString();
  }
}
