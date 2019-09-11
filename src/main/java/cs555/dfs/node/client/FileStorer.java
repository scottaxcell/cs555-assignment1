package cs555.dfs.node.client;

import cs555.dfs.transport.TcpSender;
import cs555.dfs.util.ChunkData;
import cs555.dfs.util.FileChunkifier;
import cs555.dfs.util.Utils;
import cs555.dfs.wireformats.Chunk;
import cs555.dfs.wireformats.StoreChunk;
import cs555.dfs.wireformats.StoreChunkRequest;
import cs555.dfs.wireformats.StoreChunkResponse;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

class FileStorer {
    private final Client client;
    private final List<ChunkData> chunkDataList = new ArrayList<>();
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

    public void storeFile(Path path) {
        setIsRunning(true);
        synchronized (chunkDataList) {
            chunkDataList.addAll(FileChunkifier.chunkifyFileToDataChunks(path));
            if (!chunkDataList.isEmpty()) {
                sendNextStoreChunkRequest();
            }
        }
    }

    public void setIsRunning(boolean isRunning) {
        this.isRunning.set(isRunning);
    }

    public boolean isRunning() {
        return isRunning.get();
    }
}
