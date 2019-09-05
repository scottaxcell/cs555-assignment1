package cs555.dfs.node.client;

import cs555.dfs.transport.TcpSender;
import cs555.dfs.util.ChunkData;
import cs555.dfs.util.FileChunkifier;
import cs555.dfs.util.Utils;
import cs555.dfs.wireformats.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

class FileReader {
    private final Client client;
    private final List<ChunkData> chunkDataList = new ArrayList<>();
    private final List<ChunkLocation> chunkLocations = new ArrayList<>();
    private final AtomicInteger numReceivedChunks = new AtomicInteger();
    private final AtomicInteger numExpectedChunks = new AtomicInteger();
    private final AtomicBoolean isRunning = new AtomicBoolean();

    FileReader(Client client) {
        this.client = client;
    }

    void handleRetrieveChunkResponse(RetrieveChunkResponse response) {
        String fileName = response.getFileName();
        int sequence = response.getSequence();
        byte[] fileData = response.getFileData();
        ChunkData chunkData = new ChunkData(fileName, sequence, fileData);

        synchronized (chunkDataList) {
            if (!chunkDataList.contains(chunkData)) {
                chunkDataList.add(chunkData);
            }
            Utils.debug("received: " + numReceivedChunks.get());
            Utils.debug("expected: " + numExpectedChunks.get());
            if (numReceivedChunks.incrementAndGet() == numExpectedChunks.get()) {
                Utils.debug("got all " + numReceivedChunks.get() + " chunks");
                numReceivedChunks.set(0);
                writeFile();
                return;
            }
        }

        sendNextRetrieveChunkRequest();
    }

    private void writeFile() {
        synchronized (chunkDataList) {
            if (chunkDataList.isEmpty())
                return;
            List<byte[]> list = chunkDataList.stream()
                .sorted(Comparator.comparingInt(ChunkData::getSequence))
                .map(ChunkData::getData)
                .collect(Collectors.toList());
            byte[] bytes = FileChunkifier.convertByteArrayListToByteArray(list);
            Path path = Paths.get("./bogus.txt_retrieved");
            try {
                setIsRunning(false);
                Files.createDirectories(path.getParent());
                Files.write(path, bytes);
                Utils.sleep(1500);
                Utils.info("File written to " + path);
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void sendNextRetrieveChunkRequest() {
        synchronized (chunkLocations) {
            if (!chunkLocations.isEmpty()) {
                ChunkLocation chunkLocation = chunkLocations.get(0);
                String serverAddress = chunkLocation.getServerAddress();

                TcpSender tcpSender = TcpSender.of(serverAddress);
                if (tcpSender == null) {
                    Utils.error("tcpServer is null");
                    return;
                }

                RetrieveChunkRequest request = new RetrieveChunkRequest(client.getServerAddress(),
                    tcpSender.getSocket().getLocalSocketAddress().toString(),
                    new cs555.dfs.wireformats.Chunk(chunkLocation.getFileName(),
                        chunkLocation.getSequence()));
                tcpSender.send(request.getBytes());

                chunkLocations.remove(chunkLocation);
            }
        }
    }

    public void handleCorruptChunk(CorruptChunk corruptChunk) {
        synchronized (chunkLocations) {
            chunkLocations.clear();
        }
        synchronized (chunkDataList) {
            chunkDataList.clear();
        }
        numReceivedChunks.set(0);
        numExpectedChunks.set(0);
        Utils.info("Chunk " + corruptChunk.getSequence() + " of " + corruptChunk.getFileName() + " was corrupt. Please request the file again.");
        setIsRunning(false);
    }

    public void handleRetrieveFileResponse(RetrieveFileResponse response) {
        synchronized (chunkLocations) {
            chunkLocations.addAll(response.getChunkLocations());
            if (chunkLocations.isEmpty())
                return;

            numExpectedChunks.set(chunkLocations.size());

            ChunkLocation chunkLocation = chunkLocations.get(0);
            String fileName = chunkLocation.getFileName();
            int sequence = chunkLocation.getSequence();
            String serverAddress = chunkLocation.getServerAddress();

            TcpSender tcpSender = TcpSender.of(serverAddress);
            if (tcpSender == null) {
                Utils.error("tcpServer is null");
                return;
            }

            RetrieveChunkRequest request = new RetrieveChunkRequest(client.getServerAddress(),
                tcpSender.getSocket().getLocalSocketAddress().toString(),
                new Chunk(fileName, sequence));
            tcpSender.send(request.getBytes());

            chunkLocations.remove(chunkLocation);
        }
    }

    public void setIsRunning(boolean isRunning) {
        this.isRunning.set(isRunning);
    }

    public boolean isRunning() {
        return isRunning.get();
    }
}
