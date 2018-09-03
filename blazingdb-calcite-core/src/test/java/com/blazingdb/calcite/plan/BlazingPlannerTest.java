package com.blazingdb.calcite.plan;

import static org.testng.Assert.*;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BlazingPlannerTest {

	private BlazingPlanner blazingCalcite;

	@BeforeMethod
	public void setUp() throws Exception {
		blazingCalcite = new BlazingPlanner();
	}

	@Test()
	public void testLoadDataInFile()
			throws RelConversionException, ValidationException, SqlParseException, IOException, SQLException {
		// blazingCalcite.foo("select orders.o_totalprice, customer.c_name from orders inner join customer on
		// orders.o_custkey = customer.c_custkey order by orders.o_orderkey limit 10");
		blazingCalcite.foo("select N_NATIONKEY from nation");
	}

}
