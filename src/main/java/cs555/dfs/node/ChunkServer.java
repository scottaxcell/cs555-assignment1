package cs555.dfs.node;

import cs555.dfs.transport.TcpConnection;
import cs555.dfs.transport.TcpServer;
import cs555.dfs.util.Utils;
import cs555.dfs.wireformats.Event;
import cs555.dfs.wireformats.Protocol;
import cs555.dfs.wireformats.RegisterRequest;
import cs555.dfs.wireformats.StoreChunkRequest;

import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ChunkServer implements Node {
    private final TcpServer tcpServer;
    private TcpConnection controllerTcpConnection;
    private Map<String, List<Chunk>> filesToChunks = new HashMap<>();

    private ChunkServer(String controllerIp, int controllerPort) {
//        tcpServer = new TcpServer(0, this);
        tcpServer = new TcpServer(11322, this);
        new Thread(tcpServer).start();
        Utils.sleep(500);

//        registerWithController(controllerIp, controllerPort);
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
        int protocol = event.getProtocol();
        switch (protocol) {
            case Protocol.STORE_CHUNK_REQUEST:
                handleStoreChunkRequest(event);
                break;
            default:
                throw new RuntimeException(String.format("received an unknown event with protocol %d", protocol));
        }
    }

    private void handleStoreChunkRequest(Event event) {
        StoreChunkRequest request = (StoreChunkRequest) event;
        Utils.debug("received: " + request);
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
