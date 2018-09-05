/*
 * Copyright 2018 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

package com.blazingdb.calcite.sql.custom;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Parse tree for any statement that represents a single call without arguments. The call representation can be a
 * literal or be composed by many tokens.
 */
public abstract class AbstractSqlSingleCall extends SqlCall {

	/** AbstractSqlSingleCall constructor. */
	AbstractSqlSingleCall(SqlParserPos pos) {
		super(pos);
	}

	public abstract String getKeyword();

	@Override
	public List<SqlNode> getOperandList() {
		final Collection<SqlNode> elements = new ArrayList<SqlNode>(); // empty (e.g. list tables doesn't use any args)
		return ImmutableNullableList.<SqlNode>copyOf(elements);
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword(this.getKeyword());
	}

}

// End AbstractSqlSingleCall.java
