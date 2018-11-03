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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;

import javax.naming.NamingException;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.commons.dbcp.BasicDataSource;

import com.blazingdb.calcite.catalog.connection.CatalogService;
import com.blazingdb.calcite.catalog.connection.CatalogServiceImpl;
import com.blazingdb.calcite.schema.BlazingSchema;
import com.blazingdb.protocol.IService;
import com.blazingdb.protocol.UnixService;
import com.blazingdb.protocol.message.RequestMessage;
import com.blazingdb.protocol.message.ResponseErrorMessage;
import com.blazingdb.protocol.message.ResponseMessage;
import com.blazingdb.protocol.message.calcite.DDLCreateTableRequestMessage;
import com.blazingdb.protocol.message.calcite.DDLDropTableRequestMessage;
import com.blazingdb.protocol.message.calcite.DDLRequestMessage;
import com.blazingdb.protocol.message.calcite.DDLResponseMessage;
import com.blazingdb.protocol.message.calcite.DMLRequestMessage;
import com.blazingdb.protocol.message.calcite.DMLResponseMessage;

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
/**
 * Class which holds main function. Listens in on a unix domain socket
 * for protocol buffer requests and then processes these requests. 
 * @author felipe
 *
 */





public class UnixServer {

	private static void executeUpdate() throws NamingException, SQLException, LiquibaseException, InstantiationException,
	IllegalAccessException, ClassNotFoundException {
// setDataSource((String) servletValueContainer.getValue(LIQUIBASE_DATASOURCE));

 final String LIQUIBASE_CHANGELOG = "liquibase.changelog";
final String LIQUIBASE_DATASOURCE = "liquibase.datasource";


String dataSourceName;
String changeLogFile;
String contexts;
String labels;

dataSourceName = "bz3";



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
	dataSource.setUrl("jdbc:h2:/blazingsql/bz3;INIT=CREATE SCHEMA IF NOT EXISTS bz3;");
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
	database.setDefaultSchemaName(dataSourceName);
	Liquibase liquibase = new Liquibase(changeLogFile,
			new CompositeResourceAccessor(clFO, fsFO, threadClFO), database);

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


	
    public static void main(String[] args) throws IOException {


        ApplicationContext.init(); //any api call initializes it actually
        File unixSocketFile = new File("/tmp/calcite.socket");
        unixSocketFile.deleteOnExit();

        IService calciteService  = new IService() {
            @Override
            public ByteBuffer process(ByteBuffer buffer) {

                RequestMessage requestMessage = new RequestMessage(buffer);
                if(requestMessage.getHeaderType() == MessageType.DML) {
                    DMLRequestMessage requestPayload = new DMLRequestMessage(requestMessage.getPayloadBuffer());
                    ResponseMessage response = null;
                    System.out.println("DML: " + requestPayload.getQuery());

                    try {
                        String logicalPlan  = RelOptUtil.toString(ApplicationContext.getRelationalAlgebraGenerator().getRelationalAlgebra(requestPayload.getQuery()));
                        DMLResponseMessage responsePayload = new DMLResponseMessage(logicalPlan);
                        response = new ResponseMessage(Status.Success, responsePayload.getBufferData());
                    }catch (Exception e) {
                        //TODO: give something more meaningfu than this :)

                        ResponseErrorMessage error = new ResponseErrorMessage("Improperly Formatted Query\n" + e.getStackTrace()[0]);
                        response = new ResponseMessage(Status.Error, error.getBufferData());
                    }
                    return response.getBufferData();
                }else if(requestMessage.getHeaderType() == MessageType.DDL_CREATE_TABLE) {
                    DDLCreateTableRequestMessage message = new DDLCreateTableRequestMessage(requestMessage.getPayloadBuffer());
                    ResponseMessage response = null;
                    try {
                        ApplicationContext.getCatalogService().createTable(message);
                        //I am unsure at this point if we have to update the schema or not but for safety I do it here
                        //need to see what hibernate moves around :)
                        ApplicationContext.updateContext();
                        DDLResponseMessage responsePayload = new DDLResponseMessage();
                        response = new ResponseMessage(Status.Success, responsePayload.getBufferData());
                    }catch(Exception e){
                        ResponseErrorMessage error = new ResponseErrorMessage("Could not create table");
                        response = new ResponseMessage(Status.Error, error.getBufferData());

                    }
                    return response.getBufferData();
                }else if(requestMessage.getHeaderType() == MessageType.DDL_DROP_TABLE) {
                    ResponseMessage response = null;

                    DDLDropTableRequestMessage message = new DDLDropTableRequestMessage(requestMessage.getPayloadBuffer());
                    try {
                        ApplicationContext.getCatalogService().dropTable(message);
                        ApplicationContext.updateContext();
                        DDLResponseMessage responsePayload = new DDLResponseMessage();
                        response = new ResponseMessage(Status.Success, responsePayload.getBufferData());
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        ResponseErrorMessage error = new ResponseErrorMessage("Could not drop table");
                        response = new ResponseMessage(Status.Error, error.getBufferData());

                    }
                    return response.getBufferData();

                }else {
                    ResponseMessage response = null;

                    ResponseErrorMessage error = new ResponseErrorMessage("unhandled request type");
                    response = new ResponseMessage(Status.Error, error.getBufferData());

                    return response.getBufferData();

                }

            }
        };
        UnixService service = new UnixService(calciteService);
        service.bind(unixSocketFile);
        new Thread(service).start();
    }
}