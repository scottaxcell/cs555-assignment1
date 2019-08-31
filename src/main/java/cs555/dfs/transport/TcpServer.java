package cs555.dfs.transport;

import cs555.dfs.node.Node;
import cs555.dfs.util.Utils;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

public class TcpServer implements Runnable {
    private ServerSocket serverSocket;
    private Node node;

    public TcpServer(int port, Node node) {
        this.node = node;
        try {
            serverSocket = new ServerSocket(port);
            Utils.info(String.format("%s TCP server started on %s:%d", node.getNodeTypeAsString(), getIp(), getPort()));
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        try {
            while (!Thread.interrupted()) {
                Socket socket = serverSocket.accept();
                Thread thread = new Thread(new TcpReceiver(socket, node));
                thread.start();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getIp() {
        try {
            return serverSocket.getInetAddress().getLocalHost().getCanonicalHostName();
        }
        catch (UnknownHostException e) {
            e.printStackTrace();
            return "";
        }
    }

    public int getPort() {
        return serverSocket.getLocalPort();
    }
}
