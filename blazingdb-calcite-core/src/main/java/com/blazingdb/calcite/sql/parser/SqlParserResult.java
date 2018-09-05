/*
 * Copyright 2018 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

package com.blazingdb.calcite.sql.parser;

import org.apache.calcite.sql.SqlNode;

public class SqlParserResult {
	private final boolean valid;
	private final String error;
	private final SqlNode sqlNode;

	// Creates a valid SqlParserResult object
	public SqlParserResult(SqlNode sqlNode) {
		if (sqlNode == null) {
			throw new NullPointerException("sqlNode must not be null");
		}

		this.valid = true;
		this.error = "";
		this.sqlNode = sqlNode;
	}

	// Creates an invalid SqlParserResult object
	public SqlParserResult(String error) {
		this.valid = false;
		this.error = error;
		this.sqlNode = null;
	}

	public boolean isValid() {
		return this.valid;
	}

	public String getError() {
		return this.error;
	}

	public SqlNode getSqlNode() {
		return this.sqlNode;
	}
}
