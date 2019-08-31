package cs555.dfs.node.controller;

import cs555.dfs.node.Node;
import cs555.dfs.transport.TcpConnection;
import cs555.dfs.transport.TcpServer;
import cs555.dfs.util.Utils;
import cs555.dfs.wireformats.*;

import java.io.IOException;
import java.util.*;
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
    private final Map<String, TcpConnection> connections = new ConcurrentHashMap<>(); // key = remote socket address
    private final Map<String, LiveChunkServer> liveChunkServers = new ConcurrentHashMap<>(); // key = tcp server address

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
            case Protocol.STORE_CHUNK_REQUEST:
                handleStoreChunkRequest(message);
                break;
            default:
                throw new RuntimeException(String.format("received an unknown message with protocol %d", protocol));
        }
    }

    private void handleStoreChunkRequest(Message message) {
        // todo -- is the map necessary? can i push the address into the livechunkserver object
        StoreChunkRequest request = (StoreChunkRequest) message;
        Utils.debug("received: " + request);
        String fileName = request.getFileName();
        int chunkSequence = request.getChunkSequence();

        // find chunk servers that do not have a copy of the chunk
        List<String> chunkServerAddresses = new ArrayList<>();
        for (Map.Entry<String, LiveChunkServer> entry : liveChunkServers.entrySet()) {
            String chunkServerAddress = entry.getKey();
            LiveChunkServer liveChunkServer = entry.getValue();
            if (!liveChunkServer.containsChunk(fileName, chunkSequence))
                chunkServerAddresses.add(chunkServerAddress);
        }

        // find 3 chunk servers with highest usable space
        Collections.sort(chunkServerAddresses, new Comparator<String>() {
            @Override
            public int compare(String s1, String s2) {
                long usableSpace1 = liveChunkServers.get(s1).getUsableSpace();
                long usableSpace2 = liveChunkServers.get(s2).getUsableSpace();
                return usableSpace1 < usableSpace2 ? -1 : usableSpace1 == usableSpace2 ? 0 : 1;
            }
        });

        String sourceAddress = request.getSourceAddress();
        TcpConnection tcpConnection = connections.get(sourceAddress);

        StoreChunkResponse response = new StoreChunkResponse(getServerAddress(), tcpConnection.getLocalSocketAddress(), fileName, chunkSequence, chunkServerAddresses);
        try {
            tcpConnection.send(response.getBytes());
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleMinorHeartbeat(Message message) {
        MinorHeartbeat heartbeat = (MinorHeartbeat) message;
        Utils.debug("received: " + heartbeat);
        liveChunkServers.get(heartbeat.getServerAddress()).minorHeartbeatUpdate(heartbeat);
        Utils.debug(liveChunkServers.get(heartbeat.getServerAddress()));
    }

    private void handleRegisterRequest(Message message) {
        RegisterRequest request = (RegisterRequest) message;
        Utils.debug("received: " + request);
        String serverAddress = request.getServerAddress();
        String sourceAddress = request.getSourceAddress();
        if (!liveChunkServers.containsKey(serverAddress)) {
            liveChunkServers.put(serverAddress, new LiveChunkServer(connections.get(sourceAddress)));
            Utils.debug("registering chunk server: " + serverAddress);
        }
    }

    @Override
    public String getNodeTypeAsString() {
        return "Controller";
    }

    @Override
    public void registerNewTcpConnection(TcpConnection tcpConnection) {
        connections.put(tcpConnection.getRemoteSocketAddress(), tcpConnection);
        Utils.debug("registering tcp connection: " + tcpConnection.getRemoteSocketAddress());
    }

    @Override
    public String getServerAddress() {
        return Utils.getServerAddress(tcpServer);
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
