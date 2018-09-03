package com.blazingdb.calcite.sql.custom;

import static org.testng.Assert.*;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.blazingdb.calcite.sql.parser.BlazingSqlParser;
import com.blazingdb.calcite.sql.parser.SqlParserResult;

public class SqlListDatabasesTest {

	private BlazingSqlParser blazingCalcite;

	@BeforeMethod
	public void setUp() throws Exception {
		blazingCalcite = new BlazingSqlParser();
	}

	@Test()
	public void testSimpleListDatabases() {
		final String sqlStatement = "list databases";
		final SqlParserResult sqlVerification = blazingCalcite.parseSql(sqlStatement);
		final boolean isValid = sqlVerification.isValid();
		assertTrue(isValid, sqlVerification.getError());
	}

}
