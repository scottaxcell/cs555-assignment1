package cs555.dfs.node;

import cs555.dfs.transport.TcpSender;
import cs555.dfs.transport.TcpServer;
import cs555.dfs.util.Utils;
import cs555.dfs.wireformats.Event;
import cs555.dfs.wireformats.Protocol;
import cs555.dfs.wireformats.RegisterRequest;

import java.net.Socket;

public class Controller implements Node {
    private final TcpServer tcpServer;

    public Controller(int port) {
        tcpServer = new TcpServer(port, this);
        new Thread(tcpServer).start();
        Utils.sleep(500);
    }

    @Override
    public void onEvent(Event event) {
        int protocol = event.getProtocol();
        switch (protocol) {
            case Protocol.REGISTER_REQUEST:
                handleRegisterRequest(event);
                break;
            default:
                throw new RuntimeException(String.format("received an unknown event with protocol %d", protocol));
        }
    }

    private void handleRegisterRequest(Event event) {
        RegisterRequest request = (RegisterRequest) event;
        Utils.debug("received: " + request);
        Socket socket = request.getSocket();
        TcpSender tcpSender = new TcpSender(socket);
        String address = String.format("%s:%d", request.getIp(), request.getPort());
        Utils.debug("address: " + address);

    }

    @Override
    public String getNodeTypeAsString() {
        return "Controller";
    }

    public static void main(String[] args) {
        if (args.length != 1)
            printHelpAndExit();

        int port = Integer.parseInt(args[0]);

        new Controller(port);
    }

    private static void printHelpAndExit() {
        Utils.out("USAGE: java Controller <port>\n");
        System.exit(-1);
    }
}
