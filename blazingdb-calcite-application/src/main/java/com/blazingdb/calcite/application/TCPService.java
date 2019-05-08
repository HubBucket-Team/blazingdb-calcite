package com.blazingdb.calcite.application;

/**
 * Class which holds main function. Listens in on a TCP socket
 * for protocol buffer requests and then processes these requests.
 * @author felipe
 *
 */
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.nio.ByteBuffer;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

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

			byte[] buf = new byte[1024 * 8];
			byte[] buf_len = new byte[4]; // NOTE always 8 bytes becouse blazing-protocol format

			while (!Thread.currentThread().isInterrupted()) {
				System.out.println("Waiting for messages in TCP port: " + this.tcpPort.toString());
				
				Socket connectionSocket = server.accept();
				try {
					int bytes_read = 0;
					bytes_read = connectionSocket.getInputStream().read(buf_len, 0, buf_len.length);

					int len = bytesToInt(buf_len);

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
					ByteBuffer resultBuffer = CalciteService.processRequest(inputBuffer, this.dataDirectory);

					byte[] resultBytes = resultBuffer.array();
					connectionSocket.getOutputStream().write(intToBytes(resultBytes.length));
					connectionSocket.getOutputStream().write(resultBytes);
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
