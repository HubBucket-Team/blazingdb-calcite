/*
 * Copyright 2018 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

package com.blazingdb.calcite.sql.parser;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;

import com.blazingdb.calcite.sql.parser.SqlParserImpl;
import com.blazingdb.calcite.sql.validate.BlazingSqlConformance;

public class BlazingSqlParser {
	private final Quoting quoting = Quoting.DOUBLE_QUOTE;
	private final Casing unquotedCasing = Casing.UNCHANGED;
	private final Casing quotedCasing = Casing.UNCHANGED;
	private final SqlConformance conformance = new BlazingSqlConformance();
	private final SqlParser parser;

	public BlazingSqlParser() {
		this.parser = this.buildSqlParser();
	}

	public synchronized SqlParserResult parseSql(String sql) {
		try {
			final SqlNode sqlNode = this.parser.parseQuery(sql);

			return new SqlParserResult(sqlNode);
		} catch (SqlParseException e) {
			return new SqlParserResult(e.getMessage());
		}
	}

	// Private API

	private SqlParser buildSqlParser() {
		final String sql = "select * from x";
		return SqlParser.create(sql,
				SqlParser.configBuilder().setParserFactory(SqlParserImpl.FACTORY).setQuoting(quoting)
						.setUnquotedCasing(unquotedCasing).setQuotedCasing(quotedCasing).setConformance(conformance)
						.build());
	}

}
