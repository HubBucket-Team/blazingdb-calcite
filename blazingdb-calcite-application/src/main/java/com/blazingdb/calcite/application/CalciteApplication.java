/*
 * This file is part of the JNR project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.blazingdb.calcite.application;

/**
 * Class which holds main function. Listens in on a TCP socket
 * for protocol buffer requests and then processes these requests.
 * @author felipe
 *
 */
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.blazingdb.calcite.application.Chrono.Chronometer;
import com.blazingdb.protocol.IService;
import com.blazingdb.protocol.message.RequestMessage;
import com.blazingdb.protocol.message.ResponseErrorMessage;
import com.blazingdb.protocol.message.ResponseMessage;
import com.blazingdb.protocol.message.calcite.DDLCreateTableRequestMessage;
import com.blazingdb.protocol.message.calcite.DDLDropTableRequestMessage;
import com.blazingdb.protocol.message.calcite.DDLResponseMessage;
import com.blazingdb.protocol.message.calcite.DMLRequestMessage;
import com.blazingdb.protocol.message.calcite.DMLResponseMessage;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.dbcp.BasicDataSource;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;

import java.nio.ByteBuffer;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

import javax.naming.NamingException;

import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;

import jnr.unixsocket.UnixServerSocket;
import jnr.unixsocket.UnixServerSocketChannel;
import jnr.unixsocket.UnixSocketAddress;
import jnr.unixsocket.UnixSocketChannel;

import blazingdb.protocol.Status;
import blazingdb.protocol.calcite.MessageType;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.CompositeResourceAccessor;
import liquibase.resource.FileSystemResourceAccessor;
import liquibase.resource.ResourceAccessor;

import java.nio.charset.Charset;

public class CalciteApplication {

	private static void executeUpdate(final String dataDirectory) throws NamingException, SQLException,
			LiquibaseException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		// setDataSource((String) servletValueContainer.getValue(LIQUIBASE_DATASOURCE));

		final String LIQUIBASE_CHANGELOG = "liquibase.changelog";
		final String LIQUIBASE_DATASOURCE = "liquibase.datasource";

		String dataSourceName;
		String changeLogFile;
		String contexts;
		String labels;

		// setChangeLogFile((String) servletValueContainer.getValue(LIQUIBASE_CHANGELOG));
		changeLogFile = "liquibase-bz-master.xml";

		// setContexts((String) servletValueContainer.getValue(LIQUIBASE_CONTEXTS));
		contexts = "";
		// setLabels((String) servletValueContainer.getValue(LIQUIBASE_LABELS));

		labels = "";

		// defaultSchema = StringUtil.trimToNull((String)
		// servletValueContainer.getValue(LIQUIBASE_SCHEMA_DEFAULT));
		// defaultSchema =

		Connection connection = null;
		Database database = null;
		try {
			// DriverManager.registerDriver((Driver) Class.forName("com.mysql.jdbc.Driver").newInstance());
			// String url = "jdbc:mysql://localhost:3306/bz3";
			// connection = DriverManager.getConnection(url);

			BasicDataSource dataSource = new BasicDataSource();
			dataSource.setDriverClassName("org.h2.Driver");
			dataSource.setUsername("blazing");
			dataSource.setPassword("blazing");
			dataSource.setUrl("jdbc:h2:" + dataDirectory + "/bz3");
			dataSource.setMaxActive(10);
			dataSource.setMaxIdle(5);
			dataSource.setInitialSize(5);
			dataSource.setValidationQuery("SELECT 1");

			// MySQLData dataSource = new JdbcDataSource(); // (DataSource) ic.lookup(dataSourceName);
			// dataSource.setURL("jdbc:mysql://localhost:3306/bz3");
			// dataSource.setUser("blazing");
			// dataSource.setPassword("blazing");
			connection = dataSource.getConnection();

			Thread currentThread = Thread.currentThread();
			ClassLoader contextClassLoader = currentThread.getContextClassLoader();
			ResourceAccessor threadClFO = new ClassLoaderResourceAccessor(contextClassLoader);

			ResourceAccessor clFO = new ClassLoaderResourceAccessor();
			ResourceAccessor fsFO = new FileSystemResourceAccessor();

			database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));

			Liquibase liquibase = new Liquibase(changeLogFile, new CompositeResourceAccessor(clFO, fsFO, threadClFO),
					database);

			// @SuppressWarnings("unchecked")
			// StringTokenizer initParameters = new StringTokenizer(""); // servletContext.getInitParameterNames();
			// while (initParameters.hasMoreElements()) {
			// String name = initParameters.nextElement().trim();
			// if (name.startsWith(LIQUIBASE_PARAMETER + ".")) {
			// // liquibase.setChangeLogParameter(name.substring(LIQUIBASE_PARAMETER.length() + 1),
			// // servletValueContainer.getValue(name));
			// }
			// }

			liquibase.update(new Contexts(contexts), new LabelExpression(labels));
		} finally {
			if (database != null) {
				database.close();
			} else if (connection != null) {
				connection.close();
			}
		}
	}

	public static int bytesToInt(byte[] bytes) {
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		buffer.order(LITTLE_ENDIAN);
		return buffer.getInt();
	}

	public static byte[] intToBytes(int value) {
		return ByteBuffer.allocate(4).putInt(value).array();
	}

	public static void main(String[] args) throws IOException {
		final CalciteApplicationOptions calciteApplicationOptions = parseArguments(args);

		System.out.println("Calcite is running");
		if (calciteApplicationOptions.isVerboseMode() == false) {
			System.setOut(new PrintStream(new OutputStream() {
				public void write(int b) {
					//DO NOTHING
				}
			}));

			System.setErr(new PrintStream(new OutputStream() {
				public void write(int b) {
					//DO NOTHING
				}
			}));
		}

		final Integer port = calciteApplicationOptions.port();
		final String dataDirectory = calciteApplicationOptions.dataDirectory();

		try {
			executeUpdate(dataDirectory);
		} catch (Exception e) {
			e.printStackTrace();
		}

		//ApplicationContext.init(); // any api call initializes it actually
		File unixSocketFile = new File("/tmp/calcite.socket");
		unixSocketFile.deleteOnExit();

		UnixService service = new UnixService(dataDirectory);
		service.bind(unixSocketFile);
		new Thread(service).start();
	}

	private static CalciteApplicationOptions parseArguments(String[] arguments) {
		final Options options = new Options();

		final String verboseDefaultValue = "false";
		final String portDefaultValue = "8891";
		final String dataDirectoryDefaultValue = "/blazingsql";


		final Option portOption = Option.builder("p").required(false).longOpt("port").hasArg().argName("INTEGER")
				.desc("TCP port for this service").type(Integer.class).build();
		options.addOption(portOption);

		final Option dataDirectoryOption = Option.builder("d").required(false).longOpt("data_directory").hasArg()
				.argName("PATH").desc("Path to data directory where calcite put" + " the metastore files").build();
		options.addOption(dataDirectoryOption);

		final Option verboseOption = Option.builder("p").required(false).longOpt("verbose").hasArg().argName("BOOLEAN")
				.desc("verbose mode").type(Boolean.class).build();
		options.addOption(verboseOption);
		try {
			final CommandLineParser commandLineParser = new DefaultParser();
			final CommandLine commandLine = commandLineParser.parse(options, arguments);

			final Integer port = Integer.valueOf(commandLine.getOptionValue(portOption.getLongOpt(), portDefaultValue));
			final String dataDirectory = commandLine.getOptionValue(dataDirectoryOption.getLongOpt(),
					dataDirectoryDefaultValue);
			final Boolean verbose = Boolean.valueOf(commandLine.getOptionValue(verboseOption.getLongOpt(), verboseDefaultValue));

			CalciteApplicationOptions calciteApplicationOptions = new CalciteApplicationOptions(port, dataDirectory, verbose);

			return calciteApplicationOptions;
		} catch (ParseException e) {
			System.out.println(e);
			final HelpFormatter helpFormatter = new HelpFormatter();
			helpFormatter.printHelp("CalciteApplication", options);
			System.exit(1);
			return null;
		}
	}

	private static class CalciteApplicationOptions {

		private final Integer port;
		private final String dataDirectory;
		private final boolean verbose;


		public CalciteApplicationOptions(final Integer port, final String dataDirectory, final boolean verbose) {
			this.port = port;
			this.dataDirectory = dataDirectory;
			this.verbose = verbose;
		}

		public Integer port() {
			return this.port;
		}

		public String dataDirectory() {
			return dataDirectory;
		}

		public boolean isVerboseMode() {return verbose; }
	}
}
