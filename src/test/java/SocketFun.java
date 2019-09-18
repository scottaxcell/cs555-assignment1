import cs555.dfs.node.Node;
import cs555.dfs.transport.TcpConnection;
import cs555.dfs.transport.TcpSender;
import cs555.dfs.transport.TcpServer;
import cs555.dfs.util.Utils;
import cs555.dfs.wireformats.Message;
import cs555.dfs.wireformats.RegisterRequest;

import java.io.IOException;
import java.net.Socket;

public class SocketFun {
    private static final int SERVER_PORT = 30000;
    private static final String SERVER_IP = "127.0.0.1";

    public static void main(String[] args) throws IOException, InterruptedException {
        TestNode testNode = new TestNode();
        TcpServer tcpServer = new TcpServer(SERVER_PORT, testNode);
        new Thread(tcpServer).start();
        Thread.sleep(1000);

        registerWithServer();
        registerWithServer();
        registerWithServer();
        registerWithServer();
        registerWithServer();
    }

    private static void registerWithServer() throws IOException {
        Socket socket = new Socket(SERVER_IP, SERVER_PORT);
        TcpSender tcpSender = new TcpSender(socket);

        RegisterRequest registerRequest = new RegisterRequest(SERVER_IP, tcpSender.getLocalSocketAddress());
        tcpSender.send(registerRequest.getBytes());
    }

    private static class TestNode implements Node {

        @Override
        public void onMessage(Message message) {
            Utils.debug("received a message: " + message);
        }

        @Override
        public String getNodeTypeAsString() {
            return "TestNode";
        }

        @Override
        public void registerNewTcpConnection(TcpConnection tcpConnection) {
            Utils.debug("registering connection: " + tcpConnection.getRemoteSocketAddress() + " to " + tcpConnection.getLocalSocketAddress());
        }

        @Override
        public String getServerAddress() {
            return null;
        }
    }
}
