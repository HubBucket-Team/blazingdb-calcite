package com.blazingdb.calcite.sql.ddl;

import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.SqlParserTest;
import org.junit.Test;

import com.blazingdb.calcite.sql.parser.SqlParserImpl;

/**
 * Tests SQL parser extensions for DDL.
 *
 * <p>
 * Remaining tasks:
 * <ul>
 *
 * <li>"create table x (a int) as values 1, 2" should fail validation; data type not allowed in "create table ... as".
 *
 * <li>"create table x (a int, b int as (a + 1)) stored" should not allow b to be specified in insert; should generate
 * check constraint on b; should populate b in insert as if it had a default
 *
 * <li>"create table as select" should store constraints deduced by planner
 *
 * <li>during CREATE VIEW, check for a table and a materialized view with the same name (they have the same namespace)
 *
 * </ul>
 */
public class ServerParserTest extends SqlParserTest {

	@Override
	protected SqlParserImplFactory parserImplFactory() {
		return SqlParserImpl.FACTORY;
	}

	@Override
	public void testGenerateKeyWords() {
		// by design, method only works in base class; no-ops in this sub-class
	}



}

// End ServerParserTest.java
