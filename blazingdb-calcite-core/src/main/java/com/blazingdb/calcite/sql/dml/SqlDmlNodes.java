package com.blazingdb.calcite.sql.dml;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Utilities concerning {@link SqlNode} for DDL.
 */
public class SqlDmlNodes {
	private SqlDmlNodes() {
	}

	/** Creates a LOAD DATA INFILE. */
	public static SqlLoadDataInfile loadDataInfile(SqlParserPos pos, SqlNode file, SqlIdentifier table,
			SqlNode delimiter, SqlNode quoteCharacter, SqlNode termination) {
		return new SqlLoadDataInfile(pos, file, table, delimiter, quoteCharacter, termination);
	}

}

// End SqlDmlNodes.java
