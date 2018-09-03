package com.blazingdb.calcite.sql.custom;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Parse tree for {@code GET DATA FOLDERS} statement.
 */
public class SqlGetDataFolders extends AbstractSqlSingleCall {

	private static final String KEYWORD = "GET DATA FOLDERS";
	private static final SqlOperator OPERATOR = new SqlSpecialOperator(KEYWORD, SqlKind.OTHER);

	/** Creates a SqlListTables. */
	SqlGetDataFolders(SqlParserPos pos) {
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

// End SqlGetDataFolders.java
