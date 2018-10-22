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

import org.apache.calcite.plan.RelOptUtil;

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

public class UnixServer {



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
