package cs555.dfs.node.controller;

import cs555.dfs.node.Chunk;
import cs555.dfs.transport.TcpConnection;
import cs555.dfs.wireformats.MajorHeartbeat;
import cs555.dfs.wireformats.Message;
import cs555.dfs.wireformats.MinorHeartbeat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class LiveChunkServer {
    private final TcpConnection tcpConnection;
    private final String serverAddress;
    private Map<String, List<Chunk>> filesToChunks = new ConcurrentHashMap<>();
    private long usableSpace;
    private int totalNumberOfChunks;

    public LiveChunkServer(TcpConnection tcpConnection, String serverAddress) {
        this.tcpConnection = tcpConnection;
        this.serverAddress = serverAddress;
    }

    public void minorHeartbeatUpdate(MinorHeartbeat heartbeat) {
        usableSpace = heartbeat.getUsableSpace();
        totalNumberOfChunks = heartbeat.getNumberOfChunks();
        List<Chunk> chunks = heartbeat.getChunks();
        for (Chunk chunk : chunks) {
            List<Chunk> computeIfAbsent = filesToChunks.computeIfAbsent(chunk.getFileName(), c -> new ArrayList<>());
            if (!computeIfAbsent.contains(chunk))
                computeIfAbsent.add(chunk);
        }
    }

    public void majorHeartbeatUpdate(MajorHeartbeat heartbeat) {
        usableSpace = heartbeat.getUsableSpace();
        totalNumberOfChunks = heartbeat.getNumberOfChunks();
        filesToChunks.clear();
        List<Chunk> chunks = heartbeat.getChunks();
        for (Chunk chunk : chunks)
            filesToChunks.computeIfAbsent(chunk.getFileName(), c -> new ArrayList<>());
    }

    public long getUsableSpace() {
        return usableSpace;
    }

    public void sendMessage(Message message) {
        tcpConnection.send(message.getBytes());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LiveChunkServer that = (LiveChunkServer) o;
        return serverAddress.equals(that.serverAddress);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverAddress);
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

    public String getServerAddress() {
        return serverAddress;
    }

    public List<Chunk> getChunks(String fileName) {
        return filesToChunks.get(fileName);
    }
}
