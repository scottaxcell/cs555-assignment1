package cs555.dfs.node.controller;

import cs555.dfs.node.Chunk;
import cs555.dfs.transport.TcpConnection;
import cs555.dfs.wireformats.Message;
import cs555.dfs.wireformats.MinorHeartbeat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LiveChunkServer {
    private final TcpConnection tcpConnection;
    private Map<String, List<Chunk>> filesToChunks = new HashMap<>();
    private long usableSpace;
    private int totalNumberOfChunks;

    public LiveChunkServer(TcpConnection tcpConnection) {
        this.tcpConnection = tcpConnection;
    }

    public void minorHeartbeatUpdate(MinorHeartbeat heartbeat) {
        usableSpace = heartbeat.getUsableSpace();
        totalNumberOfChunks = heartbeat.getNumberOfChunks();
        List<Chunk> chunks = heartbeat.getChunks();
        for (Chunk chunk : chunks)
            filesToChunks.computeIfAbsent(chunk.getFileName(), c -> new ArrayList<>());
    }

    public long getUsableSpace() {
        return usableSpace;
    }

    public void sendMessage(Message message) {
        try {
            tcpConnection.send(message.getBytes());
        }
        catch (IOException e) {
            e.printStackTrace();
        }
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
}
