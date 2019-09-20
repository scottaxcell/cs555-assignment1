package cs555.dfs.node.controller;

import cs555.dfs.node.Chunk;
import cs555.dfs.node.Node;
import cs555.dfs.node.Shard;
import cs555.dfs.transport.TcpConnection;
import cs555.dfs.transport.TcpSender;
import cs555.dfs.transport.TcpServer;
import cs555.dfs.util.Utils;
import cs555.dfs.wireformats.*;
import cs555.dfs.wireformats.erasure.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
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
            case Protocol.FILE_LIST_REQUEST_ERASURE:
                handleFileListRequestErasure(message);
                break;
            case Protocol.SHARD_HEARTBEAT:
                handleShardHeartbeat(message);
                break;
            case Protocol.RETRIEVE_FILE_REQUEST_ERASURE:
                handleRetrieveFileRequestErasure(message);
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
            new cs555.dfs.wireformats.erasure.Shard(fileName, sequence, fragment),
            validServerAddresses.get(ThreadLocalRandom.current().nextInt(validServerAddresses.size())));

        tcpConnection.send(response.getBytes());
    }

    private List<LiveChunkServer> findServersWithoutShard(Shard shard) {
        List<LiveChunkServer> servers;

        synchronized (liveChunkServers) {
            servers = liveChunkServers.stream()
                .filter(lcs -> !lcs.containsShardOfChunk(shard.getFileName(), shard.getSequence()))
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
                .filter(lcs -> !lcs.getChunks().isEmpty())
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

    private void handleFileListRequestErasure(Message message) {
        FileListRequestErasure request = (FileListRequestErasure) message;
        Utils.debug("received: " + request);

        Set<String> fileNames;
        synchronized (liveChunkServers) {
            fileNames = liveChunkServers.stream()
                .flatMap(lcs -> lcs.getShards().stream())
                .map(Shard::getFileName)
                .collect(Collectors.toSet());
        }

        String sourceAddress = request.getSourceAddress();
        TcpConnection tcpConnection = connections.get(sourceAddress);
        if (tcpConnection == null) {
            Utils.error("failed to find connection for: " + sourceAddress);
            return;
        }

        FileListResponseErasure response = new FileListResponseErasure(getServerAddress(), tcpConnection.getLocalSocketAddress(), new ArrayList<>(fileNames));
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
                Utils.info("Registered chunk server @ " + serverAddress);
            }
        }
    }

    private void handleMinorHeartbeat(Message message) {
        Heartbeat heartbeat = (Heartbeat) message;
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
        stringBuilder.append("===================\n");
        synchronized (liveChunkServers) {
            for (LiveChunkServer server : liveChunkServers) {
                stringBuilder.append(server.getServerAddress());
                stringBuilder.append(":\n");
                Set<String> fileNames = server.getFileNames();
                for (String fileName : fileNames) {
                    stringBuilder.append("  ");
                    stringBuilder.append(fileName);
                    stringBuilder.append(": ");
                    List<Chunk> sortedChunks = new ArrayList<>(server.getChunks(fileName));
                    Collections.sort(sortedChunks, Comparator.comparingInt(Chunk::getSequence));
                    for (int i = 0; i < sortedChunks.size(); i++) {
                        stringBuilder.append(sortedChunks.get(i).getSequence());
                        if (i != sortedChunks.size() - 1)
                            stringBuilder.append(", ");
                    }
                    stringBuilder.append("\n");
                }
            }
        }
        Utils.info(stringBuilder.toString());
    }

    private void handleShardHeartbeat(Message message) {
        ShardHeartbeat heartbeat = (ShardHeartbeat) message;
        synchronized (liveChunkServers) {
            liveChunkServers.stream()
                .filter(lcs -> lcs.getServerAddress().equals(heartbeat.getServerAddress()))
                .findFirst()
                .ifPresent(lcs -> lcs.shardHeartbeatUpdate(heartbeat));
        }
        printShardState();
    }

    private void printShardState() {
        StringBuilder stringBuilder = new StringBuilder("Current State (erasure)\n");
        stringBuilder.append("=============\n");
        synchronized (liveChunkServers) {
            for (LiveChunkServer server : liveChunkServers) {
                stringBuilder.append(server.getServerAddress());
                stringBuilder.append(":\n");
                Set<String> fileNames = server.getShardFileNames();
                for (String fileName : fileNames) {
                    stringBuilder.append("  ");
                    stringBuilder.append(fileName);
                    stringBuilder.append(": ");
                    List<Shard> sortedShards = new ArrayList<>(server.getShards(fileName));
                    Collections.sort(sortedShards, Comparator.comparingInt(Shard::getSequence).thenComparing(Shard::getFragment));
                    if (!sortedShards.isEmpty()) {
                        for (int i = 0; i < sortedShards.size(); i++) {
                            stringBuilder.append(sortedShards.get(i).getSequence());
                            stringBuilder.append("(");
                            stringBuilder.append(sortedShards.get(i).getFragment());
                            stringBuilder.append(")");
                            if (i != sortedShards.size() - 1)
                                stringBuilder.append(", ");
                        }
                    }
                    stringBuilder.append("\n");
                }
            }
        }
        Utils.info(stringBuilder.toString());
    }

    private void handleMajorHeartbeat(Message message) {
        Heartbeat heartbeat = (Heartbeat) message;
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
        int size = request.getSize();

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
            new cs555.dfs.wireformats.Chunk(fileName, sequence, -1, size), validServerAddresses);
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
                    ChunkLocation chunkLocation = new ChunkLocation(new cs555.dfs.wireformats.Chunk(fileName, c.getSequence(), -1, -1), lcs.getServerAddress());
                    if (!chunkLocations.contains(chunkLocation))
                        chunkLocations.add(chunkLocation);
                }
            }
        }

        if (chunkLocations.isEmpty())
            return;

        Utils.debug("sending " + chunkLocations.size() + " chunks");

        String sourceAddress = request.getSourceAddress();
        TcpConnection tcpConnection = connections.get(sourceAddress);
        if (tcpConnection == null) {
            Utils.error("failed to find connection for: " + sourceAddress);
            return;
        }

        RetrieveFileResponse response = new RetrieveFileResponse(getServerAddress(), tcpConnection.getLocalSocketAddress(), fileName, chunkLocations);
        tcpConnection.send(response.getBytes());
    }

    private void handleRetrieveFileRequestErasure(Message message) {
        RetrieveFileRequestErasure request = (RetrieveFileRequestErasure) message;
        Utils.debug("received: " + request);

        String fileName = request.getFileName();
        List<ShardLocation> shardLocations = new ArrayList<>();
        synchronized (liveChunkServers) {
            for (LiveChunkServer lcs : liveChunkServers) {
                List<Shard> shards = lcs.getShards(fileName);
                if (shards == null)
                    continue;
                for (Shard s : shards) {
                    ShardLocation shardLocation = new ShardLocation(new cs555.dfs.wireformats.erasure.Shard(fileName, s.getSequence(), s.getFragment()), lcs.getServerAddress());
                    if (!shardLocations.contains(shardLocation))
                        shardLocations.add(shardLocation);
                }
            }
        }

        if (shardLocations.isEmpty())
            return;

        String sourceAddress = request.getSourceAddress();
        TcpConnection tcpConnection = connections.get(sourceAddress);
        if (tcpConnection == null) {
            Utils.error("failed to find connection for: " + sourceAddress);
            return;
        }

        RetrieveFileResponseErasure response = new RetrieveFileResponseErasure(getServerAddress(), tcpConnection.getLocalSocketAddress(), fileName, shardLocations);
        tcpConnection.send(response.getBytes());
    }

    private void handleCorruptChunk(Message message) {
        CorruptChunk corruptChunk = (CorruptChunk) message;
        Utils.debug("received: " + corruptChunk);

        String fileName = corruptChunk.getFileName();
        int sequence = corruptChunk.getSequence();
        List<Integer> corruptSlices = corruptChunk.getCorruptSlices();
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
                        new cs555.dfs.wireformats.Chunk(fileName, sequence, chunk.getVersion(), chunk.getSize()),
                        corruptSlices, corruptChunkServerAddress);
                    Utils.debug("sending: " + replicateChunk);
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
                if (tcpSender != null) {
                    ReplicateChunk replicateChunk = new ReplicateChunk(getServerAddress(),
                        tcpSender.getLocalSocketAddress(),
                        new cs555.dfs.wireformats.Chunk(fileName, sequence, chunk.getVersion(), chunk.getSize()),
                        Collections.emptyList(),
                        replicationServer.getServerAddress());

                    tcpSender.send(replicateChunk.getBytes());
                }
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
                        deadServers.add(lcs);
                    }
                }
            }

            for (LiveChunkServer deadServer : deadServers) {
                Utils.info("Chunk server @ " + deadServer.getServerAddress() + " died");
                liveChunkServers.remove(deadServer);
                connections.remove(deadServer.getTcpConnection().getRemoteSocketAddress());
                processDeadChunkServer(deadServer);
            }
        }
    }
}
