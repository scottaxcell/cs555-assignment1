package cs555.dfs.node.controller;

import cs555.dfs.node.Node;
import cs555.dfs.transport.TcpConnection;
import cs555.dfs.transport.TcpServer;
import cs555.dfs.util.Utils;
import cs555.dfs.wireformats.Message;
import cs555.dfs.wireformats.MinorHeartbeat;
import cs555.dfs.wireformats.Protocol;
import cs555.dfs.wireformats.RegisterRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * USE CASES
 * =========
 * <p>
 * provide 3 chunk servers for a given chunk to write:
 * - read client request for 3 servers
 * - find servers that do not hold this chunk already
 * - choose 3 servers with highest usable disk space
 * - send list of servers to client
 * <p>
 * track live chunk servers
 * - register server on message from server
 * - update chunk data when a heartbeat comes in
 * - de-register server when it goes down
 * <p>
 * provide list of servers with all chunks for a file:
 * - read client request for a file
 * - send client list of servers with files
 * <p>
 * storage:
 * - live chunk server
 * - all chunks associated with server
 * - metadata associated with server
 */
public class Controller implements Node {
    private final TcpServer tcpServer;
    private final Map<String, TcpConnection> connections = new ConcurrentHashMap<>();
    private final Map<String, LiveChunkServer> chunkServers = new HashMap<>();


    public Controller(int port) {
        tcpServer = new TcpServer(port, this);
        new Thread(tcpServer).start();
        Utils.sleep(500);
    }

    @Override
    public void onMessage(Message message) {
        int protocol = message.getProtocol();
        switch (protocol) {
            case Protocol.REGISTER_REQUEST:
                handleRegisterRequest(message);
                break;
            case Protocol.MINOR_HEART_BEAT:
                handleMinorHeartbeat(message);
                break;
            default:
                throw new RuntimeException(String.format("received an unknown message with protocol %d", protocol));
        }
    }

    private void handleMinorHeartbeat(Message message) {
        MinorHeartbeat heartbeat = (MinorHeartbeat) message;
        Utils.debug("received: " + heartbeat);
//        Socket socket = heartbeat.getSocket();
    }

    private void handleRegisterRequest(Message message) {
        RegisterRequest request = (RegisterRequest) message;
        Utils.debug("received: " + request);
//        Socket socket = request.getSocket();
//        String chunkServerId = String.format("%s:%d", request.getIp(), request.getPort());
//        if (!chunkServers.containsKey(chunkServerId)) {
//            TcpSender tcpSender = new TcpSender(socket);
//            chunkServers.put(chunkServerId, new LiveChunkServer(tcpSender));
//        }
//        Utils.debug("chunkServerId: " + chunkServerId);
//        Utils.debug("socketID: " + socket.getInetAddress().getCanonicalHostName() + ":" + socket.getPort());
//        Utils.debug("socketLocal: " + socket.getLocalSocketAddress());
//        Utils.debug("socketRemote: " + socket.getRemoteSocketAddress());
    }

    @Override
    public String getNodeTypeAsString() {
        return "Controller";
    }

    @Override
    public void registerNewTcpConnection(TcpConnection tcpConnection) {
        connections.put(tcpConnection.getRemoteSocketAddress(), tcpConnection);
        Utils.debug("registering: " + tcpConnection.getRemoteSocketAddress());
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
