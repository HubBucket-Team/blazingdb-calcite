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

import static java.nio.ByteOrder.LITTLE_ENDIAN;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;

import java.net.ServerSocket;
import java.net.Socket;

public class TCPService implements Runnable {

	private Integer tcpPort;
	private String dataDirectory;

	public TCPService(final Integer tcpPort, final String dataDirectory) {
		this.tcpPort = tcpPort;
		this.dataDirectory = dataDirectory;
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
					
					System.out.println("NOOOOOOOOOOOOOOOOOOOOOO LEN 1:  " + String.valueOf(len));

					byte[] buf = new byte[len];
					dataIn.readFully(buf);

					ByteBuffer inputBuffer = ByteBuffer.wrap(buf);
					ByteBuffer resultBuffer = CalciteService.processRequest(inputBuffer, this.dataDirectory);

					byte[] resultBytes = resultBuffer.array();

					System.out.println("NOOOOOOOOOOOOOOOOOOOOOO LEN 2:  " + String.valueOf(resultBytes.length));
					
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
