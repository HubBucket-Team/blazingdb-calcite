/*
 * Copyright 2018 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

package com.blazingdb.calcite.sql.custom;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Parse tree for {@code LIST DATABASES} statement.
 */
public class SqlListDatabases extends AbstractSqlSingleCall {

	private static final String KEYWORD = "LIST DATABASES";
	private static final SqlOperator OPERATOR = new SqlSpecialOperator(KEYWORD, SqlKind.OTHER);

	/** Creates a SqlListDatabases. */
	SqlListDatabases(SqlParserPos pos) {
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
