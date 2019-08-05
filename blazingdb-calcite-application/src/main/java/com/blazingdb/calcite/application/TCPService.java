package com.blazingdb.calcite.application;

/**
 * Class which holds main function. Listens in on a TCP socket
 * for protocol buffer requests and then processes these requests.
 * @author felipe
 *
 */
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Scanner;

import blazingdb.protocol.Status;
import blazingdb.protocol.calcite.DDLCreateTableRequest;
import com.blazingdb.protocol.message.HeaderMessage;
import com.blazingdb.protocol.message.RequestMessage;

import blazingdb.protocol.orchestrator.MessageType;
import com.blazingdb.protocol.message.ResponseErrorMessage;
import com.blazingdb.protocol.message.ResponseMessage;
import com.blazingdb.protocol.message.calcite.DDLCreateTableRequestMessage;
import com.blazingdb.protocol.message.calcite.SchemaListMessage;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

public class TCPService implements Runnable {

	private Integer tcpPort;
	private String dataDirectory;
	private String orchestratorIp;
	private int orchestratorPort;

	public TCPService(final Integer tcpPort, final String dataDirectory, String orchestratorIp, int  orchestratorPort) {
		this.orchestratorIp = orchestratorIp;
		this.orchestratorPort = orchestratorPort;
		this.tcpPort = tcpPort;
		this.dataDirectory = dataDirectory;
		try {
			this.synchronizeSchema();
		} catch (Exception e) {
			System.out.println("schema synchronization with Orchestrator failed");
		}
	}

	private void synchronizeSchema() throws Exception {
		TCPClient client = new TCPClient(orchestratorIp, orchestratorPort);
		HeaderMessage header = new HeaderMessage(MessageType.SchemaList, 0L);
		RequestMessage message = new RequestMessage(header);
		ByteBuffer  responseBuffer = client.send( message.getBufferData());
		ResponseMessage response = new ResponseMessage(responseBuffer);

		if (response.getStatus() == Status.Error) {
			ResponseErrorMessage responsePayload = new ResponseErrorMessage(response.getPayload());
			System.out.println("No orchestrator found: " + responsePayload.getError());
		} else {
			SchemaListMessage responsePayload = new SchemaListMessage(response.getPayload());

			System.out.println("synchronizeSchema, number of tables: " + responsePayload.getSchemas().size());
			ApplicationContext.getCatalogService(dataDirectory).dropAllTables();

			if (responsePayload.getSchemas().size() > 0) {
				String dbName = responsePayload.getSchemas().get(0).getDbName();
				System.out.println("dbName from orch: " + dbName);
				for (DDLCreateTableRequestMessage schema : responsePayload.getSchemas()) {
					ApplicationContext.getCatalogService(dataDirectory).createTable(schema);
					// I am unsure at this point if we have to update the schema or not but for safety I do it here
					// need to see what hibernate moves around :)
					ApplicationContext.updateContext(dataDirectory);
				}
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

	@Override
	public void run() {
		try {
			ServerSocket server = new ServerSocket(this.tcpPort);

			while (!Thread.currentThread().isInterrupted()) {
				System.out.println("Waiting for messages in TCP port: " + this.tcpPort.toString());
				
				Socket connectionSocket = server.accept();
				try {
					DataInputStream dataIn = new DataInputStream(connectionSocket.getInputStream());
					
					byte[] buf_len = new byte[4]; // NOTE always 8 bytes becouse blazing-protocol format
					dataIn.readFully(buf_len);
					int len = bytesToInt(buf_len);
					
					byte[] buf = new byte[len];
					dataIn.readFully(buf);

					ByteBuffer inputBuffer = ByteBuffer.wrap(buf);
					ByteBuffer resultBuffer = CalciteService.processRequest(inputBuffer, this.dataDirectory);

					byte[] resultBytes = resultBuffer.array();

					byte[] aa = intToBytes(resultBytes.length);
					
					
					
					//connectionSocket.getOutputStream().write(aa );
		
					
						int value = resultBytes.length;
				        ByteBuffer mybuffer = ByteBuffer.allocate(4);
				        mybuffer.order(ByteOrder.LITTLE_ENDIAN);
				        mybuffer.putInt(value);
				        mybuffer.rewind();
				        byte[] mymyresultBytes = mybuffer.array();
				        connectionSocket.getOutputStream().write(mymyresultBytes);
				    

					
				      connectionSocket.getOutputStream().write(resultBytes);
					/*
				    for (byte i : resultBytes) {
						System.out.println("sendinnngggg byte : " + String.valueOf(i));

						
				    	connectionSocket.getOutputStream().write(i);
			        }
				*/
				      
				      
					// outToClient.flush();
				} catch (Exception e) {
					// TODO percy error
					System.err.println("Exception reading from/writing to socket, e=" + e);
					e.printStackTrace(System.err);
				}
				
				connectionSocket.close();
			}

			server.close();

		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
}
