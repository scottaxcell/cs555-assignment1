package cs555.dfs.node.chunkserver;

import cs555.dfs.node.Chunk;
import cs555.dfs.node.Node;
import cs555.dfs.transport.TcpConnection;
import cs555.dfs.transport.TcpSender;
import cs555.dfs.transport.TcpServer;
import cs555.dfs.util.Utils;
import cs555.dfs.wireformats.*;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ChunkServer implements Node {
    private static final String TMP_DIR = "/tmp";
    private static final String USER_NAME = System.getProperty("user.name");
    private static final long MINOR_HEARTBEAT_DELAY = 30 * 1000; // 30 seconds
    private static final long MAJOR_HEARTBEAT_DELAY = 5 * 60 * 1000; // 5 minutes
    private final int port;
    private final Path storageDir;
    private final TcpServer tcpServer;
    private final Map<String, TcpConnection> connections = new ConcurrentHashMap<>(); // key = remote socket address
    private TcpConnection controllerTcpConnection;
    private Map<String, List<Chunk>> filesToChunks = new ConcurrentHashMap<>();
    private final List<Chunk> newChunks = new ArrayList<>();

    private ChunkServer(int port, String controllerIp, int controllerPort, String serverName) {
        this.port = port;
        storageDir = Paths.get(TMP_DIR, USER_NAME, "chunkserver" + serverName);
        tcpServer = new TcpServer(port, this);
        new Thread(tcpServer).start();
        Utils.sleep(500);

        registerWithController(controllerIp, controllerPort);
        initMinorHeartbeatTimer();
    }

    private void initMinorHeartbeatTimer() {
        HeartbeatTimerTask heartbeatTimerTask = new HeartbeatTimerTask();
        Timer timer = new Timer(true);
        timer.schedule(heartbeatTimerTask, MINOR_HEARTBEAT_DELAY, MINOR_HEARTBEAT_DELAY);
    }

    private void registerWithController(String controllerIp, int controllerPort) {
        try {
            Socket socket = new Socket(controllerIp, controllerPort);
            controllerTcpConnection = new TcpConnection(socket, this);
            RegisterRequest request = new RegisterRequest(getServerAddress(), controllerTcpConnection.getLocalSocketAddress());
            controllerTcpConnection.send(request.getBytes());
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onMessage(Message message) {
        int protocol = message.getProtocol();
        switch (protocol) {
            case Protocol.STORE_CHUNK:
                handleStoreChunk(message);
                break;
            default:
                throw new RuntimeException(String.format("received an unknown message with protocol %d", protocol));
        }
    }

    private List<String> createChunkChecksums(byte[] fileData) {
        List<String> checksums = new ArrayList<>();
        Utils.debug("fileData.length: " + fileData.length);
//        Utils.createSha1FromBytes()
        List<byte[]> bytes = chunkifyFile(fileData);
        for (byte[] b : bytes) {
            String sha1FromBytes = Utils.createSha1FromBytes(b);
            Utils.debug(sha1FromBytes);
            checksums.add(sha1FromBytes);
        }
        return checksums;
    }

    private static final int CHUNK_SIZE = 8 * 1024; // 8 KB

    public static List<byte[]> chunkifyFile(byte[] fileData) {
        List<byte[]> bytes = new ArrayList<>();

        long numChunks = fileData.length / CHUNK_SIZE;
        Utils.debug("numChunks: " + numChunks);
        int remainderChunk = (int) (fileData.length % CHUNK_SIZE);
        Utils.debug("remainderChunk size: " + remainderChunk);

        int chunkSequence = 0;
        for (; chunkSequence < numChunks; chunkSequence++) {
            byte[] b = new byte[CHUNK_SIZE];
            System.arraycopy(fileData, chunkSequence * CHUNK_SIZE, b, 0, CHUNK_SIZE);
            bytes.add(b);
        }
        if (remainderChunk > 0) {
            byte[] b = new byte[remainderChunk];
            System.arraycopy(fileData, chunkSequence * CHUNK_SIZE, b, 0, remainderChunk);
            bytes.add(b);
        }

        return bytes;
    }

    private void handleStoreChunk(Message message) {
        StoreChunk storeChunk = (StoreChunk) message;
        Utils.debug("received: " + storeChunk);

        String fileName = storeChunk.getFileName();
        int sequence = storeChunk.getChunkSequence();
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

        List<String> checksums = createChunkChecksums(chunkData);
        chunk.setChecksum(checksums);

        chunk.writeChunk(chunkData);

        List<String> nextServers = storeChunk.getNextServers();
        if (nextServers.isEmpty())
            return;

        TcpSender tcpSender = null;
        TcpConnection chunkServerTcpConnection = connections.get(nextServers.get(0));
        if (chunkServerTcpConnection != null) {
            tcpSender = chunkServerTcpConnection.getTcpSender();
        }
        else {
            String[] splitServerAddress = Utils.splitServerAddress(nextServers.get(0));
            try {
                Socket socket = new Socket(splitServerAddress[0], Integer.valueOf(splitServerAddress[1]));
                tcpSender = new TcpSender(socket);
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (tcpSender == null) {
            Utils.error("tcpServer is null");
            return;
        }


        List<String> nextNextServers = nextServers.stream()
            .skip(1).collect(Collectors.toList());


        StoreChunk forwardStoreChunk = new StoreChunk(getServerAddress(), tcpSender.getSocket().getLocalSocketAddress().toString(),
            fileName, sequence, chunkData, nextNextServers);
        try {
            tcpSender.send(forwardStoreChunk.getBytes());
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Path generateWritePath(String fileName, int chunkSequence) {
        Path path = Paths.get(storageDir.toString(), fileName + "_chunk" + chunkSequence);
        return path;
    }

    private long getUsableSpace() {
        return new File(TMP_DIR).getUsableSpace();
    }

    @Override
    public String getNodeTypeAsString() {
        return "ChunkServer";
    }

    @Override
    public void registerNewTcpConnection(TcpConnection tcpConnection) {
        connections.put(tcpConnection.getRemoteSocketAddress(), tcpConnection);
        Utils.debug("registering tcp connection: " + tcpConnection.getRemoteSocketAddress());
    }

    @Override
    public String getServerAddress() {
        return Utils.getServerAddress(tcpServer);
    }

    public static void main(String[] args) {
        if (args.length != 3 && args.length != 4)
            printHelpAndExit();

        int port = Integer.parseInt(args[0]);
        String controllerIp = args[1];
        int controllerPort = Integer.parseInt(args[2]);
        String serverName = args.length == 4 ? args[3] : "";

        new ChunkServer(port, controllerIp, controllerPort, serverName);
    }

    private static void printHelpAndExit() {
        Utils.out("USAGE: java ChunkServer <port> <controller-host> <controller-port>\n");
        System.exit(-1);
    }

    private void sendMessageToController(Message message) {
        try {
            controllerTcpConnection.send(message.getBytes());
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private int getTotalNumberOfChunks() {
        int numChunks = 0;
        for (List<Chunk> chunks : filesToChunks.values()) {
            numChunks += chunks.size();
        }
        return numChunks;
    }

    private List<Chunk> getChunks() {
        return filesToChunks.values().stream()
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    }

    private class HeartbeatTimerTask extends TimerTask {
        private static final int MAJOR_HEARTBEAT_INTERVAL = 10;
        private final AtomicInteger counter = new AtomicInteger();

        @Override
        public void run() {
            if (counter.incrementAndGet() == MAJOR_HEARTBEAT_INTERVAL) {
                MajorHeartbeat heartbeat = new MajorHeartbeat(getServerAddress(), controllerTcpConnection.getLocalSocketAddress(),
                    getUsableSpace(), getTotalNumberOfChunks(), getChunks());
                sendMessageToController(heartbeat);
                counter.set(0);
            }
            else {
                synchronized (newChunks) {
                    MinorHeartbeat heartbeat = new MinorHeartbeat(getServerAddress(), controllerTcpConnection.getLocalSocketAddress(),
                        getUsableSpace(), getTotalNumberOfChunks(), newChunks);
                    sendMessageToController(heartbeat);
                    newChunks.clear();
                }
            }
        }
    }
}
