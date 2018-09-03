package com.blazingdb.calcite.sql.parser;

import static org.testng.Assert.*;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.blazingdb.calcite.sql.parser.BlazingSqlParser;
import com.blazingdb.calcite.sql.parser.SqlParserResult;

public class BlazingCalciteTest {

	private BlazingSqlParser blazingCalcite;

	@BeforeMethod
	public void setUp() throws Exception {
		blazingCalcite = new BlazingSqlParser();
	}

	@Test
	public void testSimpleCreateDatabase() {
		final String sqlStatement = "create database foodb";

		final SqlParserResult sqlVerification = blazingCalcite.parseSql(sqlStatement);

		final boolean isValid = sqlVerification.isValid();
		assertTrue(isValid, "The SQL statement is invalid.");
	}

	@Test
	public void testSimpleDropDatabase() {
		final String sqlStatement = "drop database foodb";

		final SqlParserResult sqlVerification = blazingCalcite.parseSql(sqlStatement);

		final boolean isValid = sqlVerification.isValid();
		assertTrue(isValid, "The SQL statement is invalid.");
	}

	@Test
	public void testSimpleDropDatabaseIfExists() {
		final String sqlStatement = "drop database if exists foodb";

		final SqlParserResult sqlVerification = blazingCalcite.parseSql(sqlStatement);

		final boolean isValid = sqlVerification.isValid();
		assertTrue(isValid, "The SQL statement is invalid.");
	}

	@Test(threadPoolSize = 9, invocationCount = 6, timeOut = 1000)
	public void testSimpleCreateTable() {
		final String sqlStatement = "create table x (i int not null, j varchar(5) null)";

		long last_time = System.nanoTime();

		final SqlParserResult sqlVerification = blazingCalcite.parseSql(sqlStatement);

		long time = System.nanoTime();

		int delta_time = (int) ((time - last_time) / 1000000);

		Long id = Thread.currentThread().getId();

		System.out.println(
				String.valueOf(delta_time) + " milliseconds (testSimpleCreateTable - Thread " + id.toString() + ")");

		final boolean isValid = sqlVerification.isValid();
		assertTrue(isValid, "The SQL statement is invalid.");
	}

	@Test(threadPoolSize = 9, invocationCount = 6, timeOut = 1000)
	public void testSimpleSelect() {
		final String sqlStatement = "select orders.o_orderstatus, customer.c_custkey from orders inner join customer on orders.o_custkey = customer.c_custkey where orders.o_clerk = 'Clerk#000000419' and customer.c_nationkey > 3 and customer.c_nationkey < 10 order by orders.o_custkey";

		long last_time = System.nanoTime();

		final SqlParserResult sqlVerification = blazingCalcite.parseSql(sqlStatement);

		long time = System.nanoTime();

		int delta_time = (int) ((time - last_time) / 1000000);

		Long id = Thread.currentThread().getId();

		System.out.println(
				String.valueOf(delta_time) + " milliseconds (testSimpleSelect - Thread " + id.toString() + ")");

		final boolean isValid = sqlVerification.isValid();
		assertTrue(isValid, "The SQL statement is invalid.");
	}

	@Test(threadPoolSize = 12, invocationCount = 6, timeOut = 1000)
	public void testComplexSelect() {
		final String sqlStatement = "select customer.c_custkey, orders.o_orderkey, orders.o_orderkey + 6, orders.o_orderkey + 6, orders.o_orderkey - 6, orders.o_orderkey * 6, orders.o_orderkey / 6, orders.o_totalprice, orders.o_totalprice + 6.6, orders.o_totalprice + 6.6, orders.o_totalprice - 6.6, orders.o_totalprice * 6.6, orders.o_totalprice / 6.6 from customer left outer join orders on customer.c_custkey = orders.o_custkey where  customer.c_nationkey = 3 and customer.c_custkey between 20 and 100";

		long last_time = System.nanoTime();

		final SqlParserResult sqlVerification = blazingCalcite.parseSql(sqlStatement);

		long time = System.nanoTime();

		int delta_time = (int) ((time - last_time) / 1000000);

		Long id = Thread.currentThread().getId();

		System.out.println(
				String.valueOf(delta_time) + " milliseconds (testComplexSelect - Thread " + id.toString() + ")");

		final boolean isValid = sqlVerification.isValid();
		assertTrue(isValid, "The SQL statement is invalid.");
	}

	@Test()
	public void testLoadDataInFile() {
		final String sqlStatement = "load data infile 'supplier.tbl' into table supplier fields terminated by '|' enclosed by '\"' lines terminated by '\\n'";

		long last_time = System.nanoTime();

		final SqlParserResult sqlVerification = blazingCalcite.parseSql(sqlStatement);

		long time = System.nanoTime();

		int delta_time = (int) ((time - last_time) / 1000000);

		Long id = Thread.currentThread().getId();

		System.out.println(
				String.valueOf(delta_time) + " milliseconds (testComplexSelect - Thread " + id.toString() + ")");

		final boolean isValid = sqlVerification.isValid();
		assertTrue(isValid, "The SQL statement is invalid.");
	}

}
