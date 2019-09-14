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

public class Controller implements Node {
    private static final int ALIVE_HEARTBEAT_INTERVAL = 3 * 1000;
    private static final int REPLICATION_LEVEL = 3;
    private final TcpServer tcpServer;
    private final Map<String, TcpConnection> connections = new ConcurrentHashMap<>(); // key = remote socket address
    private final List<LiveChunkServer> liveChunkServers = Collections.synchronizedList(new ArrayList<>());

    public Controller(int port) {
        tcpServer = new TcpServer(port, this);
    }

    public static void main(String[] args) {
        if (args.length != 1)
            printHelpAndExit();

        int port = Integer.parseInt(args[0]);

        new Controller(port).run();
    }

    void run() {
        new Thread(tcpServer).start();
        Utils.sleep(500);

        AliveHeartBeatTimerTask aliveHeartBeatTimerTask = new AliveHeartBeatTimerTask();
        Timer timer = new Timer(true);
        timer.schedule(aliveHeartBeatTimerTask, ALIVE_HEARTBEAT_INTERVAL, ALIVE_HEARTBEAT_INTERVAL);
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
            case Protocol.FILE_LIST_REQUEST:
                handleFileListRequest(message);
                break;
            case Protocol.STORE_SHARD_REQUEST:
                handleStoreShardRequest(message);
                break;
            default:
                throw new RuntimeException(String.format("received an unknown message with protocol %d", protocol));
        }
    }

    private void handleStoreShardRequest(Message message) {
        StoreShardRequest request = (StoreShardRequest) message;
        Utils.debug("received: " + request);
        String fileName = request.getFileName();
        int sequence = request.getSequence();
        int fragment = request.getFragment();

        List<LiveChunkServer> serversWithoutShard = findServersWithoutShard(new Shard(fileName, sequence, fragment));

        List<String> validServerAddresses = serversWithoutShard.stream()
            .map(LiveChunkServer::getServerAddress)
            .collect(Collectors.toList());

        if (validServerAddresses.isEmpty()) {
            Utils.error("failed to find live chunk server for shard");
            return;
        }

        String sourceAddress = request.getSourceAddress();
        TcpConnection tcpConnection = connections.get(sourceAddress);
        if (tcpConnection == null) {
            Utils.error("failed to find connection for: " + sourceAddress);
            return;
        }

        StoreShardResponse response = new StoreShardResponse(getServerAddress(),
            tcpConnection.getLocalSocketAddress(),
            new cs555.dfs.wireformats.Shard(fileName, sequence, fragment),
            validServerAddresses.get(0));

        tcpConnection.send(response.getBytes());
    }

    private List<LiveChunkServer> findServersWithoutShard(Shard shard) {
        List<LiveChunkServer> servers;

        synchronized (liveChunkServers) {
            servers = liveChunkServers.stream()
                .filter(lcs -> !lcs.containsShard(shard.getFileName(), shard.getSequence(), shard.getFragment()))
                .sorted(Comparator.comparingLong(LiveChunkServer::getUsableSpace).reversed())
                .collect(Collectors.toList());
        }

        return servers;
    }

    private void handleFileListRequest(Message message) {
        FileListRequest request = (FileListRequest) message;
        Utils.debug("received: " + request);

        Set<String> fileNames;
        synchronized (liveChunkServers) {
            fileNames = liveChunkServers.stream()
                .flatMap(lcs -> lcs.getFileNames().stream())
                .collect(Collectors.toSet());
        }

        String sourceAddress = request.getSourceAddress();
        TcpConnection tcpConnection = connections.get(sourceAddress);
        if (tcpConnection == null) {
            Utils.error("failed to find connection for: " + sourceAddress);
            return;
        }

        FileListResponse response = new FileListResponse(getServerAddress(), tcpConnection.getLocalSocketAddress(), new ArrayList<>(fileNames));
        tcpConnection.send(response.getBytes());
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
        if (!heartbeat.getChunks().isEmpty())
            printState();
    }

    private void printState() {
        StringBuilder stringBuilder = new StringBuilder("Current State\n");
        stringBuilder.append("=============\n");
        synchronized (liveChunkServers) {
            for (LiveChunkServer server : liveChunkServers) {
                stringBuilder.append(server.getServerAddress());
                stringBuilder.append(":\n");
                Set<String> fileNames = server.getFileNames();
                for (String fileName : fileNames) {
                    stringBuilder.append("  ");
                    stringBuilder.append(fileName);
                    stringBuilder.append(": ");
                    List<Chunk> chunks = server.getChunks(fileName);
                    for (int i = 0; i < chunks.size(); i++) {
                        stringBuilder.append(chunks.get(i).getSequence());
                        if (i != chunks.size() - 1)
                            stringBuilder.append(", ");
                    }
                    stringBuilder.append("\n");
                }
            }
        }
        Utils.info(stringBuilder.toString());
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
        printState();
    }

    private void handleStoreChunkRequest(Message message) {
        StoreChunkRequest request = (StoreChunkRequest) message;
        Utils.debug("received: " + request);
        String fileName = request.getFileName();
        int sequence = request.getSequence();

        List<LiveChunkServer> serversWithoutChunk = findServersWithoutChunk(fileName, sequence);

        List<String> validServerAddresses = serversWithoutChunk.stream()
            .map(LiveChunkServer::getServerAddress)
            .collect(Collectors.toList());

        if (validServerAddresses.size() != REPLICATION_LEVEL) {
            Utils.error("failed to find " + REPLICATION_LEVEL + " live chunk servers, found " + validServerAddresses.size());
            return;
        }

        String sourceAddress = request.getSourceAddress();
        TcpConnection tcpConnection = connections.get(sourceAddress);
        if (tcpConnection == null) {
            Utils.error("failed to find connection for: " + sourceAddress);
            return;
        }

        StoreChunkResponse response = new StoreChunkResponse(getServerAddress(), tcpConnection.getLocalSocketAddress(),
            new cs555.dfs.wireformats.Chunk(fileName, sequence), validServerAddresses);
        tcpConnection.send(response.getBytes());
    }

    private List<LiveChunkServer> findServersWithoutChunk(String fileName, int sequence) {
        List<LiveChunkServer> servers;

        synchronized (liveChunkServers) {
            servers = liveChunkServers.stream()
                .filter(lcs -> !lcs.containsChunk(fileName, sequence))
                .sorted(Comparator.comparingLong(LiveChunkServer::getUsableSpace).reversed())
                .limit(REPLICATION_LEVEL)
                .collect(Collectors.toList());
        }

        return servers;
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
        if (tcpConnection == null) {
            Utils.error("failed to find connection for: " + sourceAddress);
            return;
        }

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

    private void processDeadChunkServer(LiveChunkServer deadServer) {
        List<Chunk> chunks = deadServer.getChunks();
        for (Chunk chunk : chunks) {
            String fileName = chunk.getFileName();
            int sequence = chunk.getSequence();
            int numberOfReplications = getNumberOfReplications(fileName, sequence);
            int requiredReplcations = REPLICATION_LEVEL - numberOfReplications;
            if (requiredReplcations == 0)
                continue;
            List<LiveChunkServer> serversWithoutChunk = findServersWithoutChunk(fileName, sequence);
            if (serversWithoutChunk.size() < requiredReplcations) {
                Utils.error("failed to find required number of servers without chunk for replication");
                return;
            }
            List<LiveChunkServer> replicationServers = serversWithoutChunk.subList(0, requiredReplcations);

            List<LiveChunkServer> serversWithChunk = findServersWithChunk(fileName, sequence);
            if (serversWithChunk.isEmpty()) {
                Utils.error("failed to find server with chunk for replication");
                return;
            }

            for (LiveChunkServer replicationServer : replicationServers) {
                Utils.debug("sending replicate chunk to " + serversWithChunk.get(0).getServerAddress() + " for " + replicationServer.getServerAddress());

                TcpSender tcpSender = TcpSender.of(serversWithChunk.get(0).getServerAddress());

                ReplicateChunk replicateChunk = new ReplicateChunk(getServerAddress(),
                    tcpSender.getLocalSocketAddress(),
                    new cs555.dfs.wireformats.Chunk(fileName, sequence), replicationServer.getServerAddress());

                tcpSender.send(replicateChunk.getBytes());
            }
        }
    }

    private List<LiveChunkServer> findServersWithChunk(String fileName, int sequence) {
        List<LiveChunkServer> servers;

        synchronized (liveChunkServers) {
            servers = liveChunkServers.stream()
                .filter(lcs -> lcs.containsChunk(fileName, sequence))
                .sorted(Comparator.comparingLong(LiveChunkServer::getUsableSpace).reversed())
                .limit(REPLICATION_LEVEL)
                .collect(Collectors.toList());
        }

        return servers;
    }

    private int getNumberOfReplications(String fileName, int sequence) {
        int numReplications = 0;
        synchronized (liveChunkServers) {
            for (LiveChunkServer lcs : liveChunkServers) {
                if (lcs.containsChunk(fileName, sequence))
                    numReplications++;
            }
        }
        return numReplications;
    }

    private class AliveHeartBeatTimerTask extends TimerTask {
        @Override
        public void run() {
            List<LiveChunkServer> deadServers = new ArrayList<>();

            synchronized (liveChunkServers) {
                for (LiveChunkServer lcs : liveChunkServers) {
                    TcpConnection tcpConnection = lcs.getTcpConnection();
                    AliveHeartbeat aliveHeartbeat = new AliveHeartbeat(getServerAddress(), tcpConnection.getLocalSocketAddress());
                    try {
                        tcpConnection.sendNoCatch(aliveHeartbeat.getBytes());
                    }
                    catch (IOException e) {
                        Utils.debug("chunk server died: " + lcs.getServerAddress());
                        deadServers.add(lcs);
                    }
                }
            }

            for (LiveChunkServer deadServer : deadServers) {
                liveChunkServers.remove(deadServer);
                connections.remove(deadServer.getTcpConnection().getRemoteSocketAddress());
                processDeadChunkServer(deadServer);
            }
        }
    }
}
