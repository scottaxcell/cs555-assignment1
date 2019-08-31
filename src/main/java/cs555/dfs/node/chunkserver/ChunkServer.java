package cs555.dfs.node.chunkserver;

import cs555.dfs.node.Chunk;
import cs555.dfs.node.Node;
import cs555.dfs.transport.TcpConnection;
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

public class ChunkServer implements Node {
    private static final String TMP_DIR = "/tmp";
    private static final long MINOR_HEARTBEAT_DELAY = 2 * 1000; // todo -- should be 30 seconds
    private static final long MAJOR_HEARTBEAT_DELAY = 5 * 1000; // todo -- should be 5 minutes
    private final Path storageDir;
    private final TcpServer tcpServer;
    private TcpConnection controllerTcpConnection;
    private Map<String, List<Chunk>> filesToChunks = new ConcurrentHashMap<>();
    private final List<Chunk> newChunks = new ArrayList<>();

    private ChunkServer(String controllerIp, int controllerPort, String serverName) {
        storageDir = Paths.get(TMP_DIR, "sgaxcell/chunkserver" + serverName);
//        tcpServer = new TcpServer(0, this); // todo -- use random port
        tcpServer = new TcpServer(11322, this);
        new Thread(tcpServer).start();
        Utils.sleep(500);

        registerWithController(controllerIp, controllerPort);
        initMinorHeartbeatTimer();
    }

    private void initMinorHeartbeatTimer() {
        MinorHeartbeatTimerTask minorHeartbeatTimerTask = new MinorHeartbeatTimerTask();
        Timer timer = new Timer(true);
        timer.schedule(minorHeartbeatTimerTask, MINOR_HEARTBEAT_DELAY, MINOR_HEARTBEAT_DELAY);
    }

    private void registerWithController(String controllerIp, int controllerPort) {
        try {
            Socket socket = new Socket(controllerIp, controllerPort);
            controllerTcpConnection = new TcpConnection(socket, this);
            RegisterRequest request = new RegisterRequest(controllerTcpConnection);
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
            case Protocol.STORE_CHUNK_REQUEST:
                handleStoreChunkRequest(message);
                break;
            default:
                throw new RuntimeException(String.format("received an unknown message with protocol %d", protocol));
        }
    }

    private void handleStoreChunkRequest(Message message) {
        StoreChunkRequest request = (StoreChunkRequest) message;
        Utils.debug("received: " + request);

        // todo -- write chunk
        String fileName = request.getFileName();
        int sequence = request.getChunkSequence();
        Path path = generateWritePath(fileName, sequence);

        Chunk chunk = new Chunk(fileName, sequence, path);
        filesToChunks.computeIfAbsent(fileName, fn -> new ArrayList<>());
        List<Chunk> chunks = filesToChunks.get(fileName);
        if (!chunks.contains(chunk)) {
            synchronized (newChunks) {
                newChunks.add(chunk);
            }
            chunks.add(chunk);
        }
        int idx = chunks.indexOf(chunk);
        chunk = chunks.get(idx);

        byte[] chunkData = request.getFileData();
        chunk.writeChunk(chunkData);

        // todo -- forward chunk to next servers
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
        // todo
    }

    public static void main(String[] args) {
        if (args.length != 2 && args.length != 3)
            printHelpAndExit();

        String controllerIp = args[0];
        int controllerPort = Integer.parseInt(args[1]);

        new ChunkServer(controllerIp, controllerPort, args.length == 3 ? args[2] : "");
    }

    private static void printHelpAndExit() {
        Utils.out("USAGE: java ChunkServer <controller-host> <controller-port>\n");
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

    TcpConnection getControllerTcpConnection() {
        return controllerTcpConnection;
    }

    private List<Chunk> getNewChunks() {
        return newChunks;
    }

    private class MinorHeartbeatTimerTask extends TimerTask {
        @Override
        public void run() {
            synchronized (newChunks) {
                MinorHeartbeat heartbeat = new MinorHeartbeat(getControllerTcpConnection(), getUsableSpace(), getTotalNumberOfChunks(), getNewChunks());
                newChunks.clear();
                sendMessageToController(heartbeat);
            }
        }
    }
}
