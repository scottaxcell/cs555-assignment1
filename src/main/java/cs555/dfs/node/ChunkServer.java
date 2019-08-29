package cs555.dfs.node;

import cs555.dfs.transport.TcpConnection;
import cs555.dfs.transport.TcpServer;
import cs555.dfs.util.Utils;
import cs555.dfs.wireformats.Event;
import cs555.dfs.wireformats.RegisterRequest;

import java.io.IOException;
import java.net.Socket;

public class ChunkServer implements Node {
    private final TcpServer tcpServer;
    private TcpConnection controllerTcpConnection;

    private ChunkServer(String controllerIp, int controllerPort) {
        tcpServer = new TcpServer(0, this);
        new Thread(tcpServer).start();
        Utils.sleep(500);

        registerWithController(controllerIp, controllerPort);

        // todo -- handle command line input
    }

    private void registerWithController(String controllerIp, int controllerPort) {
        try {
            Socket socket = new Socket(controllerIp, controllerPort);
            controllerTcpConnection = TcpConnection.of(socket, this);
            RegisterRequest request = new RegisterRequest(tcpServer.getIp(), tcpServer.getPort(), controllerTcpConnection.getSocket());
            controllerTcpConnection.send(request.getBytes());
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onEvent(Event event) {

    }

    @Override
    public String getNodeTypeAsString() {
        return "ChunkServer";
    }

    public static void main(String[] args) {
        if (args.length != 2)
            printHelpAndExit();

        String controllerIp = args[0];
        int controllerPort = Integer.parseInt(args[1]);

        new ChunkServer(controllerIp, controllerPort);
    }

    private static void printHelpAndExit() {
        Utils.out("USAGE: java ChunkServer <controller-host> <controller-port>\n");
        System.exit(-1);
    }
}
