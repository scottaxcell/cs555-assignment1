package cs555.dfs.node.client;

import cs555.dfs.transport.TcpSender;
import cs555.dfs.util.*;
import cs555.dfs.wireformats.Chunk;
import cs555.dfs.wireformats.StoreChunk;
import cs555.dfs.wireformats.StoreChunkRequest;
import cs555.dfs.wireformats.StoreChunkResponse;
import cs555.dfs.wireformats.erasure.Shard;
import cs555.dfs.wireformats.erasure.StoreShard;
import cs555.dfs.wireformats.erasure.StoreShardRequest;
import cs555.dfs.wireformats.erasure.StoreShardResponse;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

class FileStorer {
    private final Client client;
    private final List<ChunkData> chunkDataList = new ArrayList<>();
    private final List<ShardData> shardDataList = new ArrayList<>();
    private AtomicBoolean isRunning = new AtomicBoolean();

    FileStorer(Client client) {
        this.client = client;
    }

    public void handleStoreChunkResponse(StoreChunkResponse response) {
        String fileName = response.getFileName();
        int chunkSequence = response.getChunkSequence();
        ChunkData chunkData;

        synchronized (chunkDataList) {
            ChunkData finderChunk = new ChunkData(fileName, chunkSequence);
            int idx = chunkDataList.indexOf(finderChunk);
            if (idx == -1) {
                Utils.error("chunkDataList not found");
                return;
            }
            chunkData = chunkDataList.get(idx);
            chunkDataList.remove(chunkData);
        }
        List<String> chunkServerAddresses = response.getChunkServerAddresses();
        if (chunkServerAddresses.isEmpty())
            return;

        String firstChunkServerAddress = chunkServerAddresses.get(0);

        TcpSender tcpSender = TcpSender.of(firstChunkServerAddress);
        if (tcpSender == null) {
            Utils.error("tcpServer is null");
            return;
        }

        List<String> nextServers = chunkServerAddresses.stream()
            .skip(1).collect(Collectors.toList());

        StoreChunk storeChunk = new StoreChunk(client.getServerAddress(), tcpSender.getLocalSocketAddress(),
            new Chunk(fileName, chunkData.sequence), chunkData.data, nextServers);
        tcpSender.send(storeChunk.getBytes());

        sendNextStoreChunkRequest();
    }

    public void handleStoreShardResponse(StoreShardResponse response) {
        String fileName = response.getFileName();
        int sequence = response.getChunkSequence();
        int fragment = response.getFragment();
        String shardServerAddress = response.getShardServerAddress();
        ShardData shardData;

        synchronized (shardDataList) {
            ShardData finderShard = new ShardData(fileName, sequence, fragment);
            int idx = shardDataList.indexOf(finderShard);
            if (idx == -1) {
                Utils.error("ShardData not found");
                return;
            }
            shardData = shardDataList.get(idx);
            shardDataList.remove(shardData);
        }

        TcpSender tcpSender = TcpSender.of(shardServerAddress);
        if (tcpSender == null) {
            Utils.error("tcpServer is null");
            return;
        }

        StoreShard storeShard = new StoreShard(client.getServerAddress(), tcpSender.getLocalSocketAddress(),
            new Shard(fileName, shardData.sequence, shardData.fragment), shardData.data);
        tcpSender.send(storeShard.getBytes());

        sendNextStoreShardRequest();
    }

    private void sendNextStoreChunkRequest() {
        synchronized (chunkDataList) {
            if (!chunkDataList.isEmpty()) {
                ChunkData chunkData = chunkDataList.get(0);
                StoreChunkRequest request = new StoreChunkRequest(client.getServerAddress(),
                    client.getControllerTcpConnection().getLocalSocketAddress(),
                    new Chunk(chunkData.fileName, chunkData.sequence));
                client.getControllerTcpConnection().send(request.getBytes());
            }
            else
                setIsRunning(false);
        }
    }

    private void sendNextStoreShardRequest() {
        synchronized (shardDataList) {
            if (!shardDataList.isEmpty()) {
                ShardData shardData = shardDataList.get(0);
                StoreShardRequest request = new StoreShardRequest(client.getServerAddress(),
                    client.getControllerTcpConnection().getLocalSocketAddress(),
                    new Shard(shardData.fileName, shardData.sequence, shardData.fragment));
                client.getControllerTcpConnection().send(request.getBytes());
            }
            else
                setIsRunning(false);
        }
    }

    public void setIsRunning(boolean isRunning) {
        this.isRunning.set(isRunning);
    }

    public void storeFile(Path path) {
        setIsRunning(true);
        synchronized (chunkDataList) {
            chunkDataList.addAll(FileChunkifier.chunkifyFileToDataChunks(path));
            if (!chunkDataList.isEmpty()) {
                sendNextStoreChunkRequest();
            }
        }
    }

    public void storeFileErasure(Path path) {
        setIsRunning(true);
        synchronized (shardDataList) {
            List<ChunkData> chunks = FileChunkifier.chunkifyFileToDataChunks(path);
            for (ChunkData chunk : chunks) {
                byte[][] encoded = ErasureEncoderDecoder.encode(chunk.getData());
                int i = 0;
                for (byte[] e : encoded)
                    shardDataList.add(new ShardData(chunk.getFileName(), chunk.getSequence(), i++, e));
            }
            if (!shardDataList.isEmpty())
                sendNextStoreShardRequest();
        }
    }

    public boolean isRunning() {
        return isRunning.get();
    }
}
