package cs555.dfs.node.chunkserver;

import cs555.dfs.node.Chunk;
import cs555.dfs.transport.TcpSender;
import cs555.dfs.util.FileChunkifier;
import cs555.dfs.util.Utils;
import cs555.dfs.wireformats.*;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

class ChunkStorage {
    private static final String TMP_DIR = "/tmp";
    private static final String USER_NAME = System.getProperty("user.name");
    private final ChunkServer server;
    private final Path storageDir;
    private final List<Chunk> newChunks = new ArrayList<>();
    private Map<String, List<Chunk>> filesToChunks = new ConcurrentHashMap<>();
    private Map<String, List<cs555.dfs.node.Shard>> filesToShards = new ConcurrentHashMap<>();

    ChunkStorage(ChunkServer server, String serverName) {
        this.server = server;
        storageDir = Paths.get(TMP_DIR, USER_NAME, "chunkserver" + serverName);
    }

    synchronized int getTotalNumberOfChunks() {
        int numChunks = 0;
        for (List<Chunk> chunks : filesToChunks.values()) {
            numChunks += chunks.size();
        }
        return numChunks;
    }

    public void handleStoreChunk(StoreChunk storeChunk) {

        String fileName = storeChunk.getFileName();
        int sequence = storeChunk.getSequence();
        Path path = generateWritePath(fileName, sequence);

        Chunk chunk = new Chunk(fileName, sequence, path);
        filesToChunks.computeIfAbsent(fileName, fn -> new ArrayList<>());

        List<Chunk> chunks = filesToChunks.get(fileName);
        if (!chunks.contains(chunk)) {
            synchronized (newChunks) {
                Utils.debug("adding new chunk");
                newChunks.add(chunk);
            }
            chunks.add(chunk);
        }
        int idx = chunks.indexOf(chunk);
        chunk = chunks.get(idx);

        byte[] chunkData = storeChunk.getFileData();

        List<String> checksums = FileChunkifier.createSliceChecksums(chunkData);
        chunk.setChecksum(checksums);

        chunk.writeChunk(chunkData);

        List<String> nextServers = storeChunk.getNextServers();
        if (nextServers.isEmpty())
            return;

        TcpSender tcpSender = TcpSender.of(nextServers.get(0));
        if (tcpSender == null) {
            Utils.error("tcpServer is null");
            return;
        }

        List<String> nextNextServers = nextServers.stream()
            .skip(1).collect(Collectors.toList());

        StoreChunk forwardStoreChunk = new StoreChunk(server.getServerAddress(),
            tcpSender.getLocalSocketAddress(),
            new cs555.dfs.wireformats.Chunk(fileName, sequence),
            chunkData, nextNextServers);
        tcpSender.send(forwardStoreChunk.getBytes());
    }

    public void handleStoreShard(StoreShard storeShard) {
        String fileName = storeShard.getFileName();
        int sequence = storeShard.getSequence();
        int fragment = storeShard.getFragment();
        Path path = generateShardWritePath(fileName, sequence, fragment);

        cs555.dfs.node.Shard shard = new cs555.dfs.node.Shard(fileName, sequence, fragment, path);
        filesToShards.computeIfAbsent(fileName, fn -> new ArrayList<>());

        List<cs555.dfs.node.Shard> shards = filesToShards.get(fileName);
        if (!shards.contains(shard))
            shards.add(shard);

        int idx = shards.indexOf(shard);
        shard = shards.get(idx);

        byte[] fileData = storeShard.getFileData();

        shard.writeShard(fileData);

        ShardHeartbeat shardHeartbeat = new ShardHeartbeat(server.getServerAddress(), server.getControllerTcpConnection().getLocalSocketAddress(),
            Collections.singletonList(shard));
        server.sendMessageToController(shardHeartbeat);
    }

    private Path generateWritePath(String fileName, int chunkSequence) {
        Path path = Paths.get(storageDir.toString(), fileName + "_chunk" + chunkSequence);
        return path;
    }

    private Path generateShardWritePath(String fileName, int chunkSequence, int fragment) {
        Path path = Paths.get(storageDir.toString(), fileName + "_chunk" + chunkSequence + "_fragment" + fragment);
        return path;
    }

    synchronized long getUsableSpace() {
        return new File(TMP_DIR).getUsableSpace();
    }

    void handleRetrieveChunkRequest(RetrieveChunkRequest request) {
        String fileName = request.getFileName();
        int sequence = request.getSequence();
        Chunk chunk = getChunk(fileName, sequence);
        if (chunk == null) {
            Utils.error("did not find chunk " + chunk);
            return;
        }

        TcpSender tcpSender = TcpSender.of(request.getServerAddress());
        if (tcpSender == null) {
            Utils.error("tcpServer is null");
            return;
        }

        List<Integer> corruptSlices = new ArrayList<>();

        Path path = chunk.getPath();
        if (!path.toFile().exists()) {
            for (int i = 0; i < 8; i++)
                corruptSlices.add(i);
            sendCorruptChunkMessage(new cs555.dfs.wireformats.Chunk(fileName, sequence), corruptSlices, tcpSender);
        }

        byte[] bytes = chunk.readChunk();
        List<String> sliceChecksums = FileChunkifier.createSliceChecksums(bytes);
        Utils.compareChecksums(chunk.getChecksums(), sliceChecksums, corruptSlices);

        if (corruptSlices.isEmpty()) {
            RetrieveChunkResponse response = new RetrieveChunkResponse(server.getServerAddress(),
                tcpSender.getLocalSocketAddress(),
                new cs555.dfs.wireformats.Chunk(fileName, sequence), bytes);
            tcpSender.send(response.getBytes());
        }
        else
            sendCorruptChunkMessage(new cs555.dfs.wireformats.Chunk(fileName, sequence), corruptSlices, tcpSender);
    }

    private void sendCorruptChunkMessage(cs555.dfs.wireformats.Chunk chunk, List<Integer> corruptSlices, TcpSender tcpSender) {
        CorruptChunk corruptChunk = new CorruptChunk(server.getServerAddress(),
            tcpSender.getLocalSocketAddress(),
            chunk, corruptSlices);
        tcpSender.send(corruptChunk.getBytes());

        server.sendMessageToController(corruptChunk);
    }

    private Chunk getChunk(String fileName, int sequence) {
        Chunk finderChunk = new Chunk(fileName, sequence, generateWritePath(fileName, sequence));
        return getChunks().stream()
            .filter(c -> c.equals(finderChunk))
            .findFirst()
            .orElse(null);
    }

    private cs555.dfs.node.Shard getShard(String fileName, int sequence, int fragment) {
        cs555.dfs.node.Shard finderShard = new cs555.dfs.node.Shard(fileName, sequence, fragment, generateShardWritePath(fileName, sequence, fragment));
        return getShards().stream()
            .filter(s -> s.equals(finderShard))
            .findFirst()
            .orElse(null);
    }

    synchronized List<Chunk> getChunks() {
        return filesToChunks.values().stream()
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    }

    synchronized List<cs555.dfs.node.Shard> getShards() {
        return filesToShards.values().stream()
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    }

    synchronized List<Chunk> getNewChunks() {
        return newChunks;
    }

    public void handleReplicateChunk(ReplicateChunk replicateChunk) {
        String fileName = replicateChunk.getFileName();
        int sequence = replicateChunk.getSequence();
        String corruptChunkServerAddress = replicateChunk.getDestinationAddress();

        Chunk chunk = getChunk(fileName, sequence);
        if (chunk == null) {
            Utils.error("did not fund chunk " + chunk);
            return;
        }

        byte[] bytes = chunk.readChunk();
        List<Integer> corruptSlices = new ArrayList<>();
        List<String> sliceChecksums = FileChunkifier.createSliceChecksums(bytes);
        Utils.compareChecksums(chunk.getChecksums(), sliceChecksums, corruptSlices);

        TcpSender tcpSender = TcpSender.of(corruptChunkServerAddress);

        StoreChunk storeChunk = new StoreChunk(server.getServerAddress(),
            tcpSender.getLocalSocketAddress(),
            new cs555.dfs.wireformats.Chunk(fileName, sequence),
            bytes, Collections.emptyList());
        tcpSender.send(storeChunk.getBytes());
    }
}
