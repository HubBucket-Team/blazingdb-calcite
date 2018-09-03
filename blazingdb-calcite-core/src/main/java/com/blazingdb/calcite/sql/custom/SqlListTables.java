package com.blazingdb.calcite.sql.custom;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Parse tree for {@code LIST TABLES} statement.
 */
public class SqlListTables extends AbstractSqlSingleCall {

	private static final String KEYWORD = "LIST TABLES";
	private static final SqlOperator OPERATOR = new SqlSpecialOperator(KEYWORD, SqlKind.OTHER);

	/** Creates a SqlListTables. */
	SqlListTables(SqlParserPos pos) {
		super(pos);
	}

	@Override
	public String getKeyword() {
		return KEYWORD;
	}

	@Override
	public SqlOperator getOperator() {
		return OPERATOR;
	}

}

// End SqlListTables.java
