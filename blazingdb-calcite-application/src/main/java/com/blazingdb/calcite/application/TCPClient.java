package com.blazingdb.calcite.application;

 import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import java.io.*;

import com.blazingdb.protocol.util.ByteBufferUtil;

public class TCPClient {
    String hostname;
    int port;
    public TCPClient(String hostname, int port){
        this.hostname = hostname;
        this.port = port;
    }

    public byte[] send(byte[] message) {
        byte[] result = null;
        try  (Socket socket = new Socket(hostname, port)) {
            OutputStream out = socket.getOutputStream();
            try {
                int length = message.length;
                ByteBuffer buffer = ByteBuffer.allocate(4);
                buffer.order(ByteOrder.LITTLE_ENDIAN);
                buffer.putInt(length);
                buffer.rewind();
                out.write(buffer.array());

                out.write(message);
                InputStream inputStream =socket.getInputStream();

                ByteBuffer bufferInt = ByteBuffer.allocate(4);
                bufferInt.order(ByteOrder.LITTLE_ENDIAN);
                inputStream.read(bufferInt.array());
                bufferInt.rewind();
                length = bufferInt.getInt();
                result = new byte[length];
                inputStream.read(result);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch(UnknownHostException u) {
            System.err.println("UnknownHostException reading from/writing to socket-orchestrator, e=" + u);
        }catch(IOException i) {
            System.err.println("IOException reading from/writing to socket-orchestrator, e=" + i);
         }
        return result;
    }

    public ByteBuffer send(ByteBuffer message) {
        return ByteBuffer.wrap(send(ByteBufferUtil.getByteArrayFromByteBuffer(message).array()));
    }
}

