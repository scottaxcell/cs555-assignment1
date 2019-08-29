package cs555.dfs.transport;

import cs555.dfs.node.Node;
import cs555.dfs.wireformats.Event;
import cs555.dfs.wireformats.EventFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;

public class TcpReceiver implements Runnable {
    private Socket socket;
    private DataInputStream dataInputStream;
    private Node node;

    public TcpReceiver(Socket socket, Node node) throws IOException {
        this.socket = socket;
        this.node = node;
        dataInputStream = new DataInputStream(socket.getInputStream());
    }

    public Socket getSocket() {
        return socket;
    }

    @Override
    public void run() {
        while (socket != null) {
            try {
                int dataLength = dataInputStream.readInt();
                byte[] data = new byte[dataLength];
                dataInputStream.readFully(data, 0, dataLength);
                Event event = EventFactory.getMessageFromData(data, socket);
                node.onEvent(event);
            }
            catch (SocketException e) {
                e.printStackTrace();
                break;
            }
            catch (IOException ignore) {
                // connection drops
                break;
            }
        }
    }
}
