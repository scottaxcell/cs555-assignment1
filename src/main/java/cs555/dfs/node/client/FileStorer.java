package cs555.dfs.node.client;

import cs555.dfs.transport.TcpSender;
import cs555.dfs.util.FileChunkifier;
import cs555.dfs.util.Utils;
import cs555.dfs.wireformats.Chunk;
import cs555.dfs.wireformats.StoreChunk;
import cs555.dfs.wireformats.StoreChunkRequest;
import cs555.dfs.wireformats.StoreChunkResponse;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

class FileStorer {
    private final Client client;
    private final List<FileChunkifier.FileDataChunk> fileDataChunks = new ArrayList<>();

    FileStorer(Client client) {
        this.client = client;
    }

    public void handleStoreChunkResponse(StoreChunkResponse response) {
        String fileName = response.getFileName();
        int chunkSequence = response.getChunkSequence();
        FileChunkifier.FileDataChunk fileDataChunk;

        synchronized (fileDataChunks) {
            FileChunkifier.FileDataChunk finderChunk = new FileChunkifier.FileDataChunk(fileName, chunkSequence);
            int idx = fileDataChunks.indexOf(finderChunk);
            if (idx == -1) {
                Utils.error("fileDataChunks not found");
                return;
            }
            fileDataChunk = fileDataChunks.get(idx);
            fileDataChunks.remove(fileDataChunk);
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

        StoreChunk storeChunk = new StoreChunk(client.getServerAddress(), tcpSender.getSocket().getLocalSocketAddress().toString(),
            new Chunk(fileName, fileDataChunk.sequence), fileDataChunk.fileData, nextServers);
        tcpSender.send(storeChunk.getBytes());

        sendNextStoreChunkRequest();
    }


    private void sendNextStoreChunkRequest() {
        synchronized (fileDataChunks) {
            if (!fileDataChunks.isEmpty()) {
                FileChunkifier.FileDataChunk fileDataChunk = fileDataChunks.get(0);
                StoreChunkRequest request = new StoreChunkRequest(client.getServerAddress(),
                    client.getControllerTcpConnection().getLocalSocketAddress(),
                    new Chunk(fileDataChunk.fileName, fileDataChunk.sequence));
                client.getControllerTcpConnection().send(request.getBytes());
            }
        }
    }

    public void storeFile(Path path) {
        synchronized (fileDataChunks) {
            fileDataChunks.addAll(FileChunkifier.chunkifyFileToFileDataChunks(path));
            if (!fileDataChunks.isEmpty()) {
                sendNextStoreChunkRequest();
            }
        }
    }
}
