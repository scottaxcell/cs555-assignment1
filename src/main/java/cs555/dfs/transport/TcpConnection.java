package cs555.dfs.transport;

import cs555.dfs.node.Node;

import java.io.IOException;
import java.net.Socket;

public class TcpConnection {
    private final Socket socket;
    private final Node node;
    private TcpReceiver tcpReceiver;
    private TcpSender tcpSender;

    public TcpConnection(Socket socket, Node node) {
        this.socket = socket;
        this.node = node;

        tcpReceiver = new TcpReceiver(socket, node);
        Thread thread = new Thread(tcpReceiver);
        thread.start();

        tcpSender = new TcpSender(socket);
    }

    public String getRemoteSocketAddress() {
        return socket.getRemoteSocketAddress().toString();
    }

    public String getLocalSocketAddress() {
        return socket.getLocalSocketAddress().toString();
    }

    public Socket getSocket() {
        return socket;
    }

    public void send(byte[] data) {
        tcpSender.send(data);
    }

    public void sendNoCatch(byte[] data) throws IOException {
        tcpSender.sendNoCatch(data);
    }

    public int getPort() {
        return socket.getLocalPort();
    }

    public TcpSender getTcpSender() {
        return tcpSender;
    }
}
