package cs555.dfs.node.controller;

import cs555.dfs.node.Chunk;
import cs555.dfs.transport.TcpSender;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LiveChunkServer {
    private final TcpSender tcpSender;
    private Map<String, List<Chunk>> filesToChunks = new HashMap<>();

    public LiveChunkServer(TcpSender tcpSender) {
        this.tcpSender = tcpSender;
    }
}
