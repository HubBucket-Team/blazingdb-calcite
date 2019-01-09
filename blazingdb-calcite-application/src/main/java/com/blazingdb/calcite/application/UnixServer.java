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
 * Class which holds main function. Listens in on a unix domain socket
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

import java.sql.Connection;
import java.sql.SQLException;

import java.nio.ByteBuffer;

import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import javax.naming.NamingException;

import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;

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

public class UnixServer {

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

	public static ByteBuffer calciteService(ByteBuffer buffer, final String dataDirectory) {
		Chronometer chronometer = Chronometer.makeStarted();

		RequestMessage requestMessage = new RequestMessage(buffer);
		if (requestMessage.getHeaderType() == MessageType.DML) {
			DMLRequestMessage requestPayload = new DMLRequestMessage(requestMessage.getPayloadBuffer());
			ResponseMessage response = null;
			System.out.println("DML: " + requestPayload.getQuery());

			try {
				String logicalPlan = RelOptUtil.toString(ApplicationContext.getRelationalAlgebraGenerator(dataDirectory)
						.getRelationalAlgebra(requestPayload.getQuery()));
				DMLResponseMessage responsePayload = new DMLResponseMessage(logicalPlan,
						chronometer.elapsed(MILLISECONDS));
				response = new ResponseMessage(Status.Success, responsePayload.getBufferData());
			} catch (SqlSyntaxException e) {
				ResponseErrorMessage error = new ResponseErrorMessage(e.getMessage());
				response = new ResponseMessage(Status.Error, error.getBufferData());
			} catch (SqlValidationException e) {
				ResponseErrorMessage error = new ResponseErrorMessage(e.getMessage());
				response = new ResponseMessage(Status.Error, error.getBufferData());
			} catch (Exception e) {
				ResponseErrorMessage error = new ResponseErrorMessage(
						"Improperly Formatted Query\n" + e.getStackTrace()[0]);
				response = new ResponseMessage(Status.Error, error.getBufferData());
			}
			return response.getBufferData();
		} else if (requestMessage.getHeaderType() == MessageType.DDL_CREATE_TABLE) {
			DDLCreateTableRequestMessage message = new DDLCreateTableRequestMessage(requestMessage.getPayloadBuffer());
			ResponseMessage response = null;
			try {
				ApplicationContext.getCatalogService(dataDirectory).createTable(message);
				// I am unsure at this point if we have to update the schema or not but for safety I do it here
				// need to see what hibernate moves around :)
				ApplicationContext.updateContext(dataDirectory);
				DDLResponseMessage responsePayload = new DDLResponseMessage(chronometer.elapsed(MILLISECONDS));
				response = new ResponseMessage(Status.Success, responsePayload.getBufferData());
			} catch (Exception e) {
				ResponseErrorMessage error = new ResponseErrorMessage("Could not create table");
				response = new ResponseMessage(Status.Error, error.getBufferData());

			}
			return response.getBufferData();
		} else if (requestMessage.getHeaderType() == MessageType.DDL_DROP_TABLE) {
			ResponseMessage response = null;

			DDLDropTableRequestMessage message = new DDLDropTableRequestMessage(requestMessage.getPayloadBuffer());
			try {
				ApplicationContext.getCatalogService(dataDirectory).dropTable(message);
				ApplicationContext.updateContext(dataDirectory);
				DDLResponseMessage responsePayload = new DDLResponseMessage(chronometer.elapsed(MILLISECONDS));
				response = new ResponseMessage(Status.Success, responsePayload.getBufferData());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				ResponseErrorMessage error = new ResponseErrorMessage("Could not drop table");
				response = new ResponseMessage(Status.Error, error.getBufferData());

			}
			return response.getBufferData();

		} else {
			ResponseMessage response = null;

			ResponseErrorMessage error = new ResponseErrorMessage("unhandled request type");
			response = new ResponseMessage(Status.Error, error.getBufferData());

			return response.getBufferData();

		}
	}

	public static int bytesToLong(byte[] bytes) {
		ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
		buffer.put(bytes);
		buffer.flip();//need flip
		return buffer.getInt();
	}

	public static void main(String[] args) throws IOException {
		final CalciteApplicationOptions calciteApplicationOptions = parseArguments(args);

		final Integer port = calciteApplicationOptions.port();
		final String dataDirectory = calciteApplicationOptions.dataDirectory();

		try {
			executeUpdate(dataDirectory);
		} catch (Exception e) {
			e.printStackTrace();
		}

		ServerSocket server = new ServerSocket(port);

		byte[] buf = new byte[1024*8];
		byte[] buf_len = new byte[4]; // NOTE always 8 bytes becouse blazing-protocol format

		while (true) {
			Socket connectionSocket = server.accept();
			try {
                int bytes_read = 0;
				bytes_read = connectionSocket.getInputStream().read(buf_len, 0, buf_len.length);

				int len = bytesToLong(buf_len);

				// This call to read() will wait forever, until the
				// program on the other side either sends some data,
				// or closes the socket.
				bytes_read = connectionSocket.getInputStream().read(buf, 0, len);

				// If the socket is closed, sockInput.read() will return -1.
				if (bytes_read < 0) {
					// TODO percy error
					System.err.println("Server: Tried to read from socket, read() returned < 0,  Closing socket.");
				}

				ByteBuffer inputBuffer = ByteBuffer.wrap(buf);
				ByteBuffer resultBuffer = calciteService(inputBuffer, dataDirectory);

				connectionSocket.getOutputStream().write(resultBuffer.array());
				// outToClient.flush();
			} catch (Exception e) {
				// TODO percy error
				System.err.println("Exception reading from/writing to socket, e=" + e);
				e.printStackTrace(System.err);
			}
		}
	}

	private static CalciteApplicationOptions parseArguments(String[] arguments) {
		final Options options = new Options();

		final String portDefaultValue = "8891";
		final String dataDirectoryDefaultValue = "/blazingsql";

		final Option portOption = Option.builder("p").required(false).longOpt("port").hasArg()
				.argName("INTEGER").desc("TCP port for this service").type(Integer.class).build();
		options.addOption(portOption);

		final Option dataDirectoryOption = Option.builder("d").required(false).longOpt("data_directory").hasArg()
				.argName("PATH").desc("Path to data directory where calcite put" + " the metastore files").build();
		options.addOption(dataDirectoryOption);

		try {
			final CommandLineParser commandLineParser = new DefaultParser();
			final CommandLine commandLine = commandLineParser.parse(options, arguments);

			final Integer port = Integer.valueOf(commandLine.getOptionValue(portOption.getLongOpt(), portDefaultValue));
			final String dataDirectory = commandLine.getOptionValue(dataDirectoryOption.getLongOpt(), dataDirectoryDefaultValue);

			CalciteApplicationOptions calciteApplicationOptions = new CalciteApplicationOptions(port, dataDirectory);

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

		public CalciteApplicationOptions(final Integer port, final String dataDirectory) {
			this.port = port;
			this.dataDirectory = dataDirectory;
		}

		public Integer port() {
			return this.port;
		}

		public String dataDirectory() {
			return dataDirectory;
		}
	}
}
