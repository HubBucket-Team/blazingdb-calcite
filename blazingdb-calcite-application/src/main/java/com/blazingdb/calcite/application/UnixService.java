package com.blazingdb.calcite.application;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

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
import com.blazingdb.protocol.util.ByteBufferUtil;

import blazingdb.protocol.Status;
import blazingdb.protocol.calcite.MessageType;
import jnr.enxio.channels.NativeSelectorProvider;
import jnr.unixsocket.UnixServerSocket;
import jnr.unixsocket.UnixServerSocketChannel;
import jnr.unixsocket.UnixSocketAddress;
import jnr.unixsocket.UnixSocketChannel;

public class UnixService implements Runnable {
    static int MAX_BUFFER_SIZE = 1024*10;

    interface Actor {
        boolean rxready(String argdataDirectory);
    }

    static final class ServerActor implements Actor {
        private final UnixServerSocketChannel channel;
        private final Selector selector;

        public ServerActor(UnixServerSocketChannel channel, Selector selector) {
            this.channel = channel;
            this.selector = selector;
        }
        public final boolean rxready(String argdataDirectory) {
            try {
                UnixSocketChannel client = channel.accept();
                client.configureBlocking(false);
                client.register(selector, SelectionKey.OP_READ, new ClientActor(client));
                return true;
            } catch (IOException ex) {
                return false;
            }
        }
    }
    static final class ClientActor implements Actor {
        private final UnixSocketChannel channel;
        public ClientActor(UnixSocketChannel channel) {
            this.channel = channel;
        }

        public final boolean rxready(String argdataDirectory) {
            try {
                SocketChannelInputStream receiver = new SocketChannelInputStream(channel);
                int length = receiver.read();
                byte [] buffer = new byte[length];
                int n = receiver.read(buffer);
                if (n > 0) {
                    ByteBuffer response = calciteService(ByteBuffer.wrap(buffer), argdataDirectory);
                    SocketChannelOutputStream sender = new SocketChannelOutputStream(channel);
                    byte [] responseBytes = response.array();
                    sender.write(responseBytes.length);
                    sender.write(responseBytes);
                    return true;
                } else if (n < 0) {
                    return false;
                }
            } catch (IOException ex) {
                ex.printStackTrace();
                return false;
            }
            return true;
        }
    }
    
    private UnixSocketAddress address = null;
    private UnixServerSocketChannel channel = null;
    private IService handler;
    String dataDirectory;

    public UnixService(final String dataDirectory) {
    	this.dataDirectory = dataDirectory;
    }

    public void bind(File unixSocket) throws IOException {
        address = new UnixSocketAddress(unixSocket);
        channel = UnixServerSocketChannel.open();
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

    @Override
    public void run() {
        try {
            Selector sel = NativeSelectorProvider.getInstance().openSelector();

            channel.configureBlocking(false);
            channel.socket().bind(address);
            channel.register(sel, SelectionKey.OP_ACCEPT, new ServerActor(channel, sel));

            while (!Thread.currentThread().isInterrupted()) {
                System.out.println("Waiting for messages");
                while (sel.select() > 0) {
                    Set<SelectionKey> keys = sel.selectedKeys();
                    Iterator<SelectionKey> iterator = keys.iterator();
                    boolean running = false;
                    boolean cancelled = false;
                    while (iterator.hasNext()) {
                        SelectionKey k = iterator.next();
                        Actor a = (Actor) k.attachment();
                        if (a.rxready(this.dataDirectory)) {
                            running = true;
                        } else {
                            k.cancel();
                            cancelled = true;
                        }
                        iterator.remove();
                    }
                    if (!running && cancelled) {
                        break;
                    }
                }
            }
            this.channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}