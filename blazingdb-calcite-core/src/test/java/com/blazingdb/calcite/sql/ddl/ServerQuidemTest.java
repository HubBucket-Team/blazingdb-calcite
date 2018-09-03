package com.blazingdb.calcite.sql.ddl;

import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.test.QuidemTest;

import net.hydromatic.quidem.Quidem;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.util.Collection;

/**
 * Unit tests for server and DDL.
 */
@RunWith(value = Parameterized.class)
public class ServerQuidemTest extends QuidemTest {
	/** Creates a ServerQuidemTest. Public per {@link Parameterized}. */
	@SuppressWarnings("WeakerAccess")
	public ServerQuidemTest(String path) {
		super(path);
	}

	/**
	 * Runs a test from the command line.
	 *
	 * <p>
	 * For example:
	 *
	 * <blockquote> <code>java ServerQuidemTest sql/table.iq</code> </blockquote>
	 */
	public static void main(String[] args) throws Exception {
		for (String arg : args) {
			new ServerQuidemTest(arg).test();
		}
	}

	@Override
	@Test
	public void test() throws Exception {
		MaterializationService.setThreadLocal();
		super.test();
	}

	/** For {@link Parameterized} runner. */
	@Parameterized.Parameters(name = "{index}: quidem({0})")
	public static Collection<Object[]> data() {
		// Start with a test file we know exists, then find the directory and list
		// its files.
		final String first = "sql/table.iq";
		return data(first);
	}

	@Override
	protected Quidem.ConnectionFactory createConnectionFactory() {
		return new QuidemConnectionFactory() {
			@Override
			public Connection connect(String name, boolean reference) throws Exception {
				switch (name) {
					case "server":
						return ServerTest.connect();
				}
				return super.connect(name, reference);
			}
		};
	}
}

// End ServerQuidemTest.java
