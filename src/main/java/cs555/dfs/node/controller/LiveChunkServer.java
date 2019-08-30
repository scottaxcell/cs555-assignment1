package cs555.dfs.node.controller;

import cs555.dfs.node.Chunk;
import cs555.dfs.transport.TcpConnection;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LiveChunkServer {
    private final TcpConnection tcpConnection;
    private Map<String, List<Chunk>> filesToChunks = new HashMap<>();

    public LiveChunkServer(TcpConnection tcpConnection) {
        this.tcpConnection = tcpConnection;
    }
}
