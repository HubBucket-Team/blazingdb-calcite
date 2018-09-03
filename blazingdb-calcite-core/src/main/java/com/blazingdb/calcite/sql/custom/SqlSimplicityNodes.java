package com.blazingdb.calcite.sql.custom;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Utilities concerning {@link SqlNode} for Simplicity commands.
 */
public class SqlSimplicityNodes {

	private SqlSimplicityNodes() {
	}

	/** Creates a USE DATABASE. */
	public static SqlUseDatabase useDatabase(SqlParserPos pos, SqlIdentifier name) {
		return new SqlUseDatabase(pos, name);
	}

	/** Creates a LIST DATABASES. */
	public static SqlListDatabases listDatabases(SqlParserPos pos) {
		return new SqlListDatabases(pos);
	}

	/** Creates a LIST TABLES. */
	public static SqlListTables listTables(SqlParserPos pos) {
		return new SqlListTables(pos);
	}

	/** Creates a LIST VIEWS. */
	public static SqlListViews listViews(SqlParserPos pos) {
		return new SqlListViews(pos);
	}

	/** Creates a GET DATA FOLDERS. */
	public static SqlGetDataFolders getDataFolders(SqlParserPos pos) {
		return new SqlGetDataFolders(pos);
	}

	/** Creates a GET UPLOAD FOLDERS. */
	public static SqlGetUploadFolders getUploadFolders(SqlParserPos pos) {
		return new SqlGetUploadFolders(pos);
	}

	/** Creates a GET PERF FOLDERS. */
	public static SqlGetPerfFolders getPerfFolders(SqlParserPos pos) {
		return new SqlGetPerfFolders(pos);
	}

	/** Creates a EXIT-BLAZING. */
	public static SqlExitBlazing exitBlazing(SqlParserPos pos) {
		return new SqlExitBlazing(pos);
	}
}

// End SqlSimplicityNodes.java
