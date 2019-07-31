package com.blazingdb.calcite.application;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.blazingdb.protocol.IService;
import org.apache.calcite.plan.RelOptUtil;

import com.blazingdb.calcite.application.Chrono.Chronometer;
import com.blazingdb.protocol.message.RequestMessage;
import com.blazingdb.protocol.message.ResponseErrorMessage;
import com.blazingdb.protocol.message.ResponseMessage;
import com.blazingdb.protocol.message.calcite.DDLCreateTableRequestMessage;
import com.blazingdb.protocol.message.calcite.DDLDropTableRequestMessage;
import com.blazingdb.protocol.message.calcite.DDLResponseMessage;
import com.blazingdb.protocol.message.calcite.DMLRequestMessage;
import com.blazingdb.protocol.message.calcite.DMLResponseMessage;

import blazingdb.protocol.Status;
import blazingdb.protocol.calcite.MessageType;

//NOTE this is a static class only thats why the ctr is private and the def is final
public final class CalciteService {
	private CalciteService() {
	}


	public static  ByteBuffer processRequestSample(ByteBuffer buffer, final String dataDirectory) {
		DMLRequestMessage request = new DMLRequestMessage(buffer);
		System.out.println("##ByteBuffer statement_:" + request.getQuery());

		String logicalPlan = "LogicalUnion(all=[false])\n" +
				"  LogicalUnion(all=[false])\n" +
				"    LogicalProject(EXPR$0=[$1], join_x=[$0])\n" +
				"      LogicalAggregate(group=[{0}], EXPR$0=[SUM($1)])\n" +
				"        LogicalProject(join_x=[$4], join_x0=[$7])\n" +
				"          LogicalJoin(condition=[=($7, $0)], joinType=[inner])\n";

		DMLResponseMessage response = new DMLResponseMessage(logicalPlan, 0);
		return response.getBufferData();
	}

	public static ByteBuffer processRequest(ByteBuffer buffer, final String dataDirectory) {
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
}
