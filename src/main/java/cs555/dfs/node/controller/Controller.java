package cs555.dfs.node.controller;

import cs555.dfs.node.Chunk;
import cs555.dfs.node.Node;
import cs555.dfs.transport.TcpConnection;
import cs555.dfs.transport.TcpSender;
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
    private static final int ALIVE_HEARTBEAT_INTERVAL = 3 * 1000;
    private static final int REPLICATION_LEVEL = 3;
    private final TcpServer tcpServer;
    private final Map<String, TcpConnection> connections = new ConcurrentHashMap<>(); // key = remote socket address
    private final List<LiveChunkServer> liveChunkServers = Collections.synchronizedList(new ArrayList<>());

    public Controller(int port) {
        tcpServer = new TcpServer(port, this);
        new Thread(tcpServer).start();
        Utils.sleep(500);

        AliveHeartBeatTimerTask aliveHeartBeatTimerTask = new AliveHeartBeatTimerTask();
        Timer timer = new Timer(true);
        timer.schedule(aliveHeartBeatTimerTask, ALIVE_HEARTBEAT_INTERVAL, ALIVE_HEARTBEAT_INTERVAL);
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

    @Override
    public void onMessage(Message message) {
        int protocol = message.getProtocol();
        switch (protocol) {
            case Protocol.REGISTER_REQUEST:
                handleRegisterRequest(message);
                break;
            case Protocol.MINOR_HEARTBEAT:
                handleMinorHeartbeat(message);
                break;
            case Protocol.MAJOR_HEARTBEAT:
                handleMajorHeartbeat(message);
                break;
            case Protocol.STORE_CHUNK_REQUEST:
                handleStoreChunkRequest(message);
                break;
            case Protocol.RETRIEVE_FILE_REQUEST:
                handleRetrieveFileRequest(message);
                break;
            case Protocol.CORRUPT_CHUNK:
                handleCorruptChunk(message);
                break;
            default:
                throw new RuntimeException(String.format("received an unknown message with protocol %d", protocol));
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

    private void handleMinorHeartbeat(Message message) {
        Heartbeat heartbeat = (Heartbeat) message;
        Utils.debug("received: " + heartbeat);
        synchronized (liveChunkServers) {
            liveChunkServers.stream()
                .filter(lcs -> lcs.getServerAddress().equals(heartbeat.getServerAddress()))
                .findFirst()
                .ifPresent(lcs -> lcs.minorHeartbeatUpdate(heartbeat));
        }
    }

    private void handleMajorHeartbeat(Message message) {
        Heartbeat heartbeat = (Heartbeat) message;
        Utils.debug("received: " + heartbeat);
        synchronized (liveChunkServers) {
            liveChunkServers.stream()
                .filter(lcs -> lcs.getServerAddress().equals(heartbeat.getServerAddress()))
                .findFirst()
                .ifPresent(lcs -> lcs.majorHeartbeatUpdate(heartbeat));
        }
    }

    private void handleStoreChunkRequest(Message message) {
        StoreChunkRequest request = (StoreChunkRequest) message;
        Utils.debug("received: " + request);
        String fileName = request.getFileName();
        int chunkSequence = request.getSequence();

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

        // todo turn on
//        if (validServerAddresses.size() != REPLICATION_LEVEL) {
//            Utils.error("failed to find " + REPLICATION_LEVEL + " live chunk servers, found " + validServerAddresses.size());
//            return;
//        }

        String sourceAddress = request.getSourceAddress();
        TcpConnection tcpConnection = connections.get(sourceAddress);

        StoreChunkResponse response = new StoreChunkResponse(getServerAddress(), tcpConnection.getLocalSocketAddress(),
            new cs555.dfs.wireformats.Chunk(fileName, chunkSequence), validServerAddresses);
        tcpConnection.send(response.getBytes());
    }

    private void handleRetrieveFileRequest(Message message) {
        RetrieveFileRequest request = (RetrieveFileRequest) message;
        Utils.debug("received: " + request);

        String fileName = request.getFileName();
        List<ChunkLocation> chunkLocations = new ArrayList<>();
        synchronized (liveChunkServers) {
            for (LiveChunkServer lcs : liveChunkServers) {
                List<Chunk> chunks = lcs.getChunks(fileName);
                if (chunks == null)
                    continue;
                for (Chunk c : chunks) {
                    ChunkLocation chunkLocation = new ChunkLocation(new cs555.dfs.wireformats.Chunk(fileName, c.getSequence()), lcs.getServerAddress());
                    if (!chunkLocations.contains(chunkLocation))
                        chunkLocations.add(chunkLocation);
                }
            }
        }

        if (chunkLocations.isEmpty())
            return;

        String sourceAddress = request.getSourceAddress();
        TcpConnection tcpConnection = connections.get(sourceAddress);

        RetrieveFileResponse response = new RetrieveFileResponse(getServerAddress(), tcpConnection.getLocalSocketAddress(), fileName, chunkLocations);
        tcpConnection.send(response.getBytes());
    }

    private void handleCorruptChunk(Message message) {
        CorruptChunk corruptChunk = (CorruptChunk) message;
        Utils.debug("received: " + corruptChunk);

        String fileName = corruptChunk.getFileName();
        int sequence = corruptChunk.getSequence();
        String corruptChunkServerAddress = corruptChunk.getServerAddress();

        synchronized (liveChunkServers) {
            for (LiveChunkServer lcs : liveChunkServers) {
                if (lcs.getServerAddress().equals(corruptChunkServerAddress))
                    continue;
                Chunk chunk = lcs.getChunk(fileName, sequence);
                if (chunk != null) {
                    Utils.debug("sending replicate chunk to " + lcs.getServerAddress());
                    TcpSender tcpSender = TcpSender.of(lcs.getServerAddress());
                    ReplicateChunk replicateChunk = new ReplicateChunk(getServerAddress(),
                        tcpSender.getLocalSocketAddress(),
                        new cs555.dfs.wireformats.Chunk(fileName, sequence), corruptChunkServerAddress);
                    tcpSender.send(replicateChunk.getBytes());
                    break;
                }
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

    private class AliveHeartBeatTimerTask extends TimerTask {

        @Override
        public void run() {
            synchronized (liveChunkServers) {
                for (LiveChunkServer lcs : liveChunkServers) {
                    TcpConnection tcpConnection = lcs.getTcpConnection();
                    AliveHeartbeat aliveHeartbeat = new AliveHeartbeat(getServerAddress(), tcpConnection.getLocalSocketAddress());
                    try {
                        tcpConnection.sendNoCatch(aliveHeartbeat.getBytes());
                    }
                    catch (IOException e) {
                        // todo handle chunk server dead
                        Utils.debug("chunk server died");
                    }
                }
            }
        }
    }
}
