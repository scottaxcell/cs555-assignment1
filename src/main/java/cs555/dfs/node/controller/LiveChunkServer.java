package cs555.dfs.node.controller;

import cs555.dfs.node.Chunk;
import cs555.dfs.node.Shard;
import cs555.dfs.transport.TcpConnection;
import cs555.dfs.util.Utils;
import cs555.dfs.wireformats.Heartbeat;
import cs555.dfs.wireformats.Message;
import cs555.dfs.wireformats.erasure.ShardHeartbeat;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LiveChunkServer {
    private final TcpConnection tcpConnection;
    private final String serverAddress;
    private Map<String, List<Chunk>> filesToChunks = new ConcurrentHashMap<>();
    private Map<String, List<Shard>> filesToShards = new ConcurrentHashMap<>();
    private long usableSpace;
    private int totalNumberOfChunks;

    public LiveChunkServer(TcpConnection tcpConnection, String serverAddress) {
        this.tcpConnection = tcpConnection;
        this.serverAddress = serverAddress;
    }

    public void minorHeartbeatUpdate(Heartbeat heartbeat) {
        usableSpace = heartbeat.getUsableSpace();
        totalNumberOfChunks = heartbeat.getNumberOfChunks();
        List<Chunk> chunks = heartbeat.getChunks();
        for (Chunk chunk : chunks) {
            List<Chunk> computeIfAbsent = filesToChunks.computeIfAbsent(chunk.getFileName(), c -> new ArrayList<>());
            if (!computeIfAbsent.contains(chunk))
                computeIfAbsent.add(chunk);
        }
    }

    public void majorHeartbeatUpdate(Heartbeat heartbeat) {
        usableSpace = heartbeat.getUsableSpace();
        totalNumberOfChunks = heartbeat.getNumberOfChunks();
        filesToChunks.clear();
        List<Chunk> chunks = heartbeat.getChunks();
        for (Chunk chunk : chunks)
            filesToChunks.computeIfAbsent(chunk.getFileName(), c -> new ArrayList<>()).add(chunk);
    }

    public void shardHeartbeatUpdate(ShardHeartbeat heartbeat) {
        List<cs555.dfs.node.Shard> shards = heartbeat.getShards();
        for (cs555.dfs.node.Shard shard : shards)
            filesToShards.computeIfAbsent(shard.getFileName(), s -> new ArrayList<>()).add(shard);
        Utils.out(filesToShards);
    }

    public long getUsableSpace() {
        return usableSpace;
    }

    public void sendMessage(Message message) {
        tcpConnection.send(message.getBytes());
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverAddress);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LiveChunkServer that = (LiveChunkServer) o;
        return serverAddress.equals(that.serverAddress);
    }

    @Override
    public String toString() {
        return "LiveChunkServer{" +
            "tcpConnection=" + tcpConnection +
            ", filesToChunks=" + filesToChunks +
            ", usableSpace=" + usableSpace +
            ", totalNumberOfChunks=" + totalNumberOfChunks +
            '}';
    }

    public boolean containsChunk(String fileName, int chunkSequence) {
        List<Chunk> chunks = filesToChunks.get(fileName);
        if (chunks == null)
            return false;

        for (Chunk chunk : chunks)
            if (chunk.getSequence() == chunkSequence)
                return true;

        return false;
    }

    public Chunk getChunk(String fileName, int sequence) {
        List<Chunk> chunks = filesToChunks.get(fileName);
        if (chunks == null)
            return null;

        for (Chunk chunk : chunks)
            if (chunk.getSequence() == sequence)
                return chunk;

        return null;
    }

    public List<Chunk> getChunks() {
        List<Chunk> chunks = new ArrayList<>();

        filesToChunks.values().stream()
            .flatMap(Collection::stream)
            .forEach(chunks::add);

        return chunks;
    }

    public List<Shard> getShards() {
        List<Shard> shards = new ArrayList<>();

        filesToShards.values().stream()
            .flatMap(Collection::stream)
            .forEach(shards::add);

        return shards;
    }

    public String getServerAddress() {
        return serverAddress;
    }

    public List<Chunk> getChunks(String fileName) {
        return filesToChunks.get(fileName);
    }

    public TcpConnection getTcpConnection() {
        return tcpConnection;
    }

    public Set<String> getFileNames() {
        return filesToChunks.keySet();
    }

    public Set<String> getShardFileNames() {
        return filesToShards.keySet();
    }

    public boolean containsShard(String fileName, int sequence, int fragment) {
        List<Shard> shards = filesToShards.get(fileName);
        if (shards == null)
            return false;

        for (Shard shard : shards)
            if (shard.getSequence() == sequence && shard.getFragment() == fragment)
                return true;

        return false;
    }

    public List<Shard> getShards(String fileName) {
        return filesToShards.get(fileName);
    }

    public boolean containsShardOfChunk(String fileName, int sequence) {
        List<Shard> shards = filesToShards.get(fileName);
        if (shards == null)
            return false;

        for (Shard shard : shards)
            if (shard.getSequence() == sequence)
                return true;

        return false;
    }
}
