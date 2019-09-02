package cs555.dfs.node.controller;

import cs555.dfs.node.Node;
import cs555.dfs.transport.TcpConnection;
import cs555.dfs.transport.TcpServer;
import cs555.dfs.util.Utils;
import cs555.dfs.wireformats.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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
    private static final int REPLICATION_LEVEL = 3;
    private final TcpServer tcpServer;
    private final Map<String, TcpConnection> connections = new ConcurrentHashMap<>(); // key = remote socket address
    private final List<LiveChunkServer> liveChunkServers = Collections.synchronizedList(new ArrayList<>());

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
            case Protocol.MAJOR_HEART_BEAT:
                handleMajorHeartbeat(message);
                break;
            case Protocol.STORE_CHUNK_REQUEST:
                handleStoreChunkRequest(message);
                break;
            case Protocol.RETRIEVE_FILE_REQUEST:
                handleRetrieveFileRequest(message);
                break;
            default:
                throw new RuntimeException(String.format("received an unknown message with protocol %d", protocol));
        }
    }

    private void handleRetrieveFileRequest(Message message) {
        RetrieveFileRequest request = (RetrieveFileRequest) message;
        Utils.debug("received: " + request);
    }

    private void handleStoreChunkRequest(Message message) {
        StoreChunkRequest request = (StoreChunkRequest) message;
        Utils.debug("received: " + request);
        String fileName = request.getFileName();
        int chunkSequence = request.getChunkSequence();

        List<LiveChunkServer> validChunkServers;
        synchronized (liveChunkServers) {
            validChunkServers = liveChunkServers.stream()
                .filter(lcs -> !lcs.containsChunk(fileName, chunkSequence))
                .sorted(Comparator.comparingLong(LiveChunkServer::getUsableSpace).reversed())
                .limit(REPLICATION_LEVEL)
                .collect(Collectors.toList());
        }

        List<String> validServerAddresses = validChunkServers.stream()
            .map(LiveChunkServer::getServerAddress)
            .collect(Collectors.toList());

        if (validServerAddresses.size() != REPLICATION_LEVEL) {
            Utils.error("failed to find " + REPLICATION_LEVEL + " live chunk servers");
            return;
        }

        String sourceAddress = request.getSourceAddress();
        TcpConnection tcpConnection = connections.get(sourceAddress);

        StoreChunkResponse response = new StoreChunkResponse(getServerAddress(), tcpConnection.getLocalSocketAddress(), fileName, chunkSequence, validServerAddresses);
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
        synchronized (liveChunkServers) {
            liveChunkServers.stream()
                .filter(lcs -> lcs.getServerAddress().equals(heartbeat.getServerAddress()))
                .findFirst()
                .ifPresent(lcs -> lcs.minorHeartbeatUpdate(heartbeat));
        }
    }

    private void handleMajorHeartbeat(Message message) {
        MajorHeartbeat heartbeat = (MajorHeartbeat) message;
        Utils.debug("received: " + heartbeat);
        synchronized (liveChunkServers) {
            liveChunkServers.stream()
                .filter(lcs -> lcs.getServerAddress().equals(heartbeat.getServerAddress()))
                .findFirst()
                .ifPresent(lcs -> lcs.majorHeartbeatUpdate(heartbeat));
        }
    }

    private void handleRegisterRequest(Message message) {
        RegisterRequest request = (RegisterRequest) message;
        Utils.debug("received: " + request);
        String serverAddress = request.getServerAddress();
        String sourceAddress = request.getSourceAddress();
        synchronized (liveChunkServers) {
            boolean noneMatch = liveChunkServers.stream()
                .noneMatch(lcs -> lcs.getServerAddress().equals(serverAddress));
            if (noneMatch) {
                liveChunkServers.add(new LiveChunkServer(connections.get(sourceAddress), serverAddress));
                Utils.debug("registering chunk server: " + serverAddress);
            }
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
