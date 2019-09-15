package cs555.dfs.node.client;

import cs555.dfs.transport.TcpSender;
import cs555.dfs.util.*;
import cs555.dfs.wireformats.*;
import cs555.dfs.wireformats.erasure.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

class FileReader {
    private final Client client;
    private final List<ChunkData> chunkDataList = new ArrayList<>();
    private final List<ShardData> shardDataList = new ArrayList<>();
    private final List<ChunkLocation> chunkLocations = new ArrayList<>();
    private final List<ShardLocation> shardLocations = new ArrayList<>();
    private final AtomicInteger numReceivedChunks = new AtomicInteger();
    private final AtomicInteger numReceivedShards = new AtomicInteger();
    private final AtomicInteger numExpectedChunks = new AtomicInteger();
    private final AtomicInteger numExpectedShards = new AtomicInteger();
    private final AtomicBoolean isRunning = new AtomicBoolean();
    private String fileName;

    FileReader(Client client) {
        this.client = client;
    }

    public void setIsRunning(boolean isRunning) {
        this.isRunning.set(isRunning);
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    void handleRetrieveChunkResponse(RetrieveChunkResponse response) {
        String fileName = response.getFileName();
        int sequence = response.getSequence();
        byte[] fileData = response.getFileData();
        ChunkData chunkData = new ChunkData(fileName, sequence, fileData);

        synchronized (chunkDataList) {
            chunkDataList.add(chunkData);
            if (numReceivedChunks.incrementAndGet() == numExpectedChunks.get()) {
                Utils.debug("got all " + numReceivedChunks.get() + " expected chunks");
                writeFile();
                return;
            }
        }

        sendNextRetrieveChunkRequest();
    }

    void handleRetrieveShardResponse(RetrieveShardResponse response) {
        String fileName = response.getFileName();
        int sequence = response.getSequence();
        int fragment = response.getFragment();
        byte[] fileData = response.getFileData();
        ShardData shardData = new ShardData(fileName, sequence, fragment, fileData);

        synchronized (shardDataList) {
            shardDataList.add(shardData);
            if (numReceivedShards.incrementAndGet() == numExpectedShards.get()) {
                Utils.debug("got all " + numReceivedShards.get() + " expected shards");
                numReceivedShards.set(0);
                writeFileErasure();
                return;
            }
        }

        sendNextRetrieveShardRequest();
    }

    private void writeFile() {
        synchronized (chunkDataList) {
            if (chunkDataList.isEmpty())
                return;
            List<byte[]> list = chunkDataList.stream()
                .sorted(Comparator.comparingInt(ChunkData::getSequence))
                .map(ChunkData::getData)
                .collect(Collectors.toList());

            chunkDataList.clear();
            numExpectedChunks.set(0);
            numReceivedChunks.set(0);

            byte[] bytes = FileChunkifier.convertByteArrayListToByteArray(list);
            Path path = Paths.get(String.format("./%s", fileName));
            try {
                setIsRunning(false);
                Files.createDirectories(path.getParent());
                Files.write(path, bytes);
                Utils.sleep(1500);
                Utils.info("File written to " + path.toAbsolutePath());
                setFileName("");
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void writeFileErasure() {
        Map<Integer, List<ShardData>> sequenceToShardData = new HashMap<>();
        List<ChunkData> chunkDatas = new ArrayList<>();

        synchronized (shardDataList) {
            if (shardDataList.isEmpty())
                return;

            for (ShardData shardData : shardDataList)
                sequenceToShardData.computeIfAbsent(shardData.getSequence(), l -> new ArrayList<>()).add(shardData);

            for (Map.Entry<Integer, List<ShardData>> entry : sequenceToShardData.entrySet()) {
                Integer sequence = entry.getKey();
                List<ShardData> shardDatas = entry.getValue();
                List<byte[]> chunkShards = shardDatas.stream()
                    .sorted(Comparator.comparingInt(ShardData::getFragment))
                    .map(ShardData::getData)
                    .collect(Collectors.toList());

                byte[] decoded = ErasureEncoderDecoder.decode(chunkShards.toArray(new byte[chunkShards.size()][]));
                ChunkData chunkData = new ChunkData(fileName, sequence, decoded);
                chunkDatas.add(chunkData);
            }

            List<byte[]> byteList = chunkDatas.stream()
                .sorted(Comparator.comparingInt(ChunkData::getSequence))
                .map(ChunkData::getData)
                .collect(Collectors.toList());

            shardDataList.clear();
            numExpectedShards.set(0);
            numReceivedShards.set(0);

            byte[] bytes = FileChunkifier.convertByteArrayListToByteArray(byteList);
            Path path = Paths.get(String.format("./%s", fileName));
            try {
                setIsRunning(false);
                Files.createDirectories(path.getParent());
                Files.write(path, bytes);
                Utils.sleep(1500);
                Utils.info("File written to " + path.toAbsolutePath());
                setFileName("");
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
                    tcpSender.getLocalSocketAddress(),
                    new cs555.dfs.wireformats.Chunk(chunkLocation.getFileName(),
                        chunkLocation.getSequence()));
                tcpSender.send(request.getBytes());

                chunkLocations.remove(chunkLocation);
            }
        }
    }

    private void sendNextRetrieveShardRequest() {
        synchronized (shardLocations) {
            if (!shardLocations.isEmpty()) {
                ShardLocation shardLocation = shardLocations.get(0);
                String serverAddress = shardLocation.getServerAddress();

                TcpSender tcpSender = TcpSender.of(serverAddress);
                if (tcpSender == null) {
                    Utils.error("tcpServer is null");
                    return;
                }

                RetrieveShardRequest request = new RetrieveShardRequest(client.getServerAddress(),
                    tcpSender.getLocalSocketAddress(),
                    new cs555.dfs.wireformats.erasure.Shard(shardLocation.getFileName(),
                        shardLocation.getSequence(),
                        shardLocation.getFragment()));
                tcpSender.send(request.getBytes());

                shardLocations.remove(shardLocation);
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
                tcpSender.getLocalSocketAddress(),
                new Chunk(fileName, sequence));
            tcpSender.send(request.getBytes());

            chunkLocations.remove(chunkLocation);
        }
    }

    public void handleRetrieveFileResponseErasure(RetrieveFileResponseErasure response) {
        synchronized (shardLocations) {
            shardLocations.addAll(response.getShardLocations());
            if (shardLocations.isEmpty())
                return;

            numExpectedShards.set(shardLocations.size());

            ShardLocation shardLocation = shardLocations.get(0);
            String fileName = shardLocation.getFileName();
            int sequence = shardLocation.getSequence();
            int fragment = shardLocation.getFragment();
            String serverAddress = shardLocation.getServerAddress();

            TcpSender tcpSender = TcpSender.of(serverAddress);
            if (tcpSender == null) {
                Utils.error("tcpServer is null");
                return;
            }

            RetrieveShardRequest request = new RetrieveShardRequest(client.getServerAddress(),
                tcpSender.getLocalSocketAddress(),
                new Shard(fileName, sequence, fragment));
            tcpSender.send(request.getBytes());

            shardLocations.remove(shardLocation);
        }
    }

    public boolean isRunning() {
        return isRunning.get();
    }
}
