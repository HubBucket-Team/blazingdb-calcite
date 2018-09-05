/*
 * Copyright 2018 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

package com.blazingdb.calcite.sql.custom;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import com.google.common.base.Preconditions;

import java.util.List;

/**
 * Parse tree for {@code USE DATABASE} statement.
 */
public class SqlUseDatabase extends SqlCall {

	private static final String KEYWORD = "USE DATABASE";
	private static final SqlOperator OPERATOR = new SqlSpecialOperator(KEYWORD, SqlKind.OTHER);

	private final SqlIdentifier name;

	/** Creates a SqlUseDatabase. */
	SqlUseDatabase(SqlParserPos pos, SqlIdentifier name) {
		super(pos);
		this.name = Preconditions.checkNotNull(name);
	}

	@Override
	public SqlOperator getOperator() {
		return OPERATOR;
	}

	@Override
	public List<SqlNode> getOperandList() {
		return ImmutableNullableList.<SqlNode>of(name);
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword(KEYWORD);
		name.unparse(writer, leftPrec, rightPrec);
	}
}

// End SqlUseDatabase.java
