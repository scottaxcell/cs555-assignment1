package cs555.dfs.node.chunkserver;

import cs555.dfs.node.Node;
import cs555.dfs.transport.TcpConnection;
import cs555.dfs.transport.TcpServer;
import cs555.dfs.util.Utils;
import cs555.dfs.wireformats.*;
import cs555.dfs.wireformats.erasure.RetrieveShardRequest;
import cs555.dfs.wireformats.erasure.StoreShard;

import java.io.IOException;
import java.net.Socket;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ChunkServer implements Node {
    private static final long MINOR_HEARTBEAT_DELAY = 10000; // todo 30 * 1000; // 30 seconds
    private final ChunkStorage chunkStorage;
    private final TcpServer tcpServer;
    private final Map<String, TcpConnection> connections = new ConcurrentHashMap<>(); // key = remote socket address
    private TcpConnection controllerTcpConnection;

    private ChunkServer(int port, String controllerIp, int controllerPort, String serverName) {
        chunkStorage = new ChunkStorage(this, serverName);
        tcpServer = new TcpServer(port, this);

        registerWithController(controllerIp, controllerPort);
    }

    void run() {
        new Thread(tcpServer).start();
        Utils.sleep(500);

        initMinorHeartbeatTimer();
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

    private void initMinorHeartbeatTimer() {
        HeartbeatTimerTask heartbeatTimerTask = new HeartbeatTimerTask();
        Timer timer = new Timer(true);
        timer.schedule(heartbeatTimerTask, MINOR_HEARTBEAT_DELAY, MINOR_HEARTBEAT_DELAY);
    }

    public static void main(String[] args) {
        if (args.length != 3 && args.length != 4)
            printHelpAndExit();

        int port = Integer.parseInt(args[0]);
        String controllerIp = args[1];
        int controllerPort = Integer.parseInt(args[2]);
        String serverName = args.length == 4 ? args[3] : "";

        new ChunkServer(port, controllerIp, controllerPort, serverName).run();
    }

    private static void printHelpAndExit() {
        Utils.out("USAGE: java ChunkServer <port> <controller-host> <controller-port>\n");
        System.exit(-1);
    }

    @Override
    public void onMessage(Message message) {
        int protocol = message.getProtocol();
        switch (protocol) {
            case Protocol.STORE_CHUNK:
                handleStoreChunk(message);
                break;
            case Protocol.STORE_SLICE:
                handleStoreSlice(message);
                break;
            case Protocol.RETRIEVE_CHUNK_REQUEST:
                handleRetrieveChunkRequest(message);
                break;
            case Protocol.REPLICATE_CHUNK:
                handleReplicateChunk(message);
                break;
            case Protocol.ALIVE_HEARTBEAT:
                handleAliveHeartbeat(message);
                break;


            case Protocol.STORE_SHARD:
                handleStoreShard(message);
                break;
            case Protocol.RETRIEVE_SHARD_REQUEST:
                handleRetrieveShardRequest(message);
                break;
            default:
                throw new RuntimeException(String.format("received an unknown message with protocol %d", protocol));
        }
    }

    private void handleAliveHeartbeat(Message message) {
        AliveHeartbeat heartbeat = (AliveHeartbeat) message;
    }

    private void handleReplicateChunk(Message message) {
        ReplicateChunk replicateChunk = (ReplicateChunk) message;
        Utils.debug("received: " + replicateChunk);
        chunkStorage.handleReplicateChunk(replicateChunk);
    }

    private void handleStoreChunk(Message message) {
        StoreChunk storeChunk = (StoreChunk) message;
        Utils.debug("received: " + storeChunk);
        chunkStorage.handleStoreChunk(storeChunk);
    }

    private void handleStoreShard(Message message) {
        StoreShard storeShard = (StoreShard) message;
        Utils.debug("received: " + storeShard);
        chunkStorage.handleStoreShard(storeShard);
    }

    private void handleStoreSlice(Message message) {
        StoreSlice storeSlice = (StoreSlice) message;
        Utils.debug("received: " + storeSlice);
        chunkStorage.handleStoreSlice(storeSlice);
    }

    private void handleRetrieveChunkRequest(Message message) {
        RetrieveChunkRequest request = (RetrieveChunkRequest) message;
        Utils.debug("received: " + request);
        chunkStorage.handleRetrieveChunkRequest(request);
    }

    private void handleRetrieveShardRequest(Message message) {
        RetrieveShardRequest request = (RetrieveShardRequest) message;
        Utils.debug("received: " + request);
        chunkStorage.handleRetrieveShardRequest(request);
    }

    @Override
    public String getNodeTypeAsString() {
        return "ChunkServer";
    }

    @Override
    public void registerNewTcpConnection(TcpConnection tcpConnection) {
        connections.put(tcpConnection.getRemoteSocketAddress(), tcpConnection);
    }

    @Override
    public String getServerAddress() {
        return Utils.getServerAddress(tcpServer);
    }

    public void sendMessageToController(Message message) {
        controllerTcpConnection.send(message.getBytes());
    }

    public TcpConnection getControllerTcpConnection() {
        return controllerTcpConnection;
    }

    private class HeartbeatTimerTask extends TimerTask {
        private static final int MAJOR_HEARTBEAT_INTERVAL = 10;
        private final AtomicInteger counter = new AtomicInteger();

        @Override
        public void run() {
            if (counter.incrementAndGet() == MAJOR_HEARTBEAT_INTERVAL) {
                Heartbeat heartbeat = new Heartbeat(Protocol.MAJOR_HEARTBEAT,
                    getServerAddress(),
                    controllerTcpConnection.getLocalSocketAddress(),
                    chunkStorage.getUsableSpace(),
                    chunkStorage.getTotalNumberOfChunks(),
                    chunkStorage.getChunks());
                sendMessageToController(heartbeat);
                counter.set(0);
            }
            else {
                Heartbeat heartbeat = new Heartbeat(Protocol.MINOR_HEARTBEAT,
                    getServerAddress(),
                    controllerTcpConnection.getLocalSocketAddress(),
                    chunkStorage.getUsableSpace(),
                    chunkStorage.getTotalNumberOfChunks(),
                    chunkStorage.getNewChunks());
                sendMessageToController(heartbeat);
                chunkStorage.getNewChunks().clear();
            }
        }
    }
}
