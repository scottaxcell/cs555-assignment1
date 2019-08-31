package cs555.dfs.node;

import cs555.dfs.transport.TcpConnection;
import cs555.dfs.transport.TcpSender;
import cs555.dfs.transport.TcpServer;
import cs555.dfs.util.FileChunkifier;
import cs555.dfs.util.Utils;
import cs555.dfs.wireformats.*;

import java.io.IOException;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * USE CASES
 * =========
 * <p>
 * store file on dfs:
 * - chunkify file
 * - ask controller for 3 servers
 * - send chunk to first server
 * - wait for response before asking controller for next set of servers for the next chunk
 * - repeat for each chunk
 * <p>
 * read file from dfs:
 * - ask controller for servers
 * - ask each server for all available chunks
 * - sort chunks according to sequence number
 * - write chunks to file on disk -- user will specify location
 */
public class Client implements Node {
    private final TcpServer tcpServer;
    private final Map<String, TcpConnection> connections = new ConcurrentHashMap<>(); // key = remote socket address
    private TcpConnection controllerTcpConnection;

    public Client(String controllerIp, int controllerPort) {
        tcpServer = new TcpServer(0, this);
        new Thread(tcpServer).start();

        try {
            Socket socket = new Socket(controllerIp, controllerPort);
            controllerTcpConnection = new TcpConnection(socket, this);
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        handleCmdLineInput();
    }

    @Override
    public void onMessage(Message message) {
        int protocol = message.getProtocol();
        switch (protocol) {
            case Protocol.STORE_CHUNK_RESPONSE:
                handleStoreChunkResponse(message);
                break;
            default:
                throw new RuntimeException(String.format("received an unknown message with protocol %d", protocol));
        }
    }

    private void handleStoreChunkResponse(Message message) {
        StoreChunkResponse response = (StoreChunkResponse) message;
        Utils.debug("received: " + response);

        int chunkSequence = response.getChunkSequence();
        FileChunkifier.FileDataChunk fileDataChunk;
        synchronized (currentFileDataChunks) {
            if (currentFileDataChunks == null) {
                Utils.error("currentFileDataChunks is null");
                return;
            }
            FileChunkifier.FileDataChunk finderChunk = new FileChunkifier.FileDataChunk(chunkSequence);
            int idx = currentFileDataChunks.indexOf(finderChunk);
            if (idx == -1) {
                Utils.error("fileDataChunks not found");
                return;
            }
            fileDataChunk = currentFileDataChunks.get(idx);
            currentFileDataChunks.remove(fileDataChunk);
        }
        List<String> chunkServerAddresses = response.getChunkServerAddresses();
        String firstChunkServerAddress = chunkServerAddresses.get(0);

        TcpSender tcpSender = null;
        TcpConnection chunkServerTcpConnection = connections.get(firstChunkServerAddress);
        if (chunkServerTcpConnection != null) {
            tcpSender = chunkServerTcpConnection.getTcpSender();
        }
        else {
            String[] splitServerAddress = Utils.splitServerAddress(chunkServerAddresses.get(0));
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

        List<String> nextServers = new ArrayList<>();
        nextServers.add(chunkServerAddresses.get(1));
        nextServers.add(chunkServerAddresses.get(2));

        StoreChunk storeChunk = new StoreChunk(getServerAddress(), tcpSender.getSocket().getLocalSocketAddress().toString(),
            response.getFileName(), fileDataChunk.sequence, fileDataChunk.fileData, nextServers);
        try {
            tcpSender.send(storeChunk.getBytes());
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String getNodeTypeAsString() {
        return "Client";
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

    private void handleCmdLineInput() {
        printMenu();

        String input;
        Scanner scanner = new Scanner(System.in);
        scanner.useDelimiter(Pattern.compile("[\\r\\n;]+"));

        while (true) {
            Utils.out("\n");

            input = scanner.next();
            if (input.startsWith("sf")) {
                // todo -- turn on ask for file
//                Utils.out("fileName: \n");
//                String fileName = scanner.next();
                String fileName = "/s/chopin/a/grad/sgaxcell/cs555-assignment1/bogus.bin";
                Path path = Paths.get(fileName);
                if (!path.toFile().exists()) {
                    Utils.out("file does not exist: " + path + "\n");
                    continue;
                }
                storeFile(path);
            }
            else if (input.startsWith("pm")) {
                printMenu();
            }
            else if (input.startsWith("q")) {
                Utils.out("goodbye\n");
                System.exit(0);
            }
        }
    }

    private List<FileChunkifier.FileDataChunk> currentFileDataChunks;

    private void storeFile(Path path) {
        Utils.debug("storing file: " + Utils.getCanonicalPath(path));

        currentFileDataChunks = Collections.synchronizedList(FileChunkifier.chunkifyFileToFileDataChunks(path.toFile()));
        synchronized (currentFileDataChunks) {
            for (int i = 0; i < currentFileDataChunks.size(); i++) {
                StoreChunkRequest request = new StoreChunkRequest(getServerAddress(), controllerTcpConnection.getLocalSocketAddress(), Utils.getCanonicalPath(path), i);
                try {
                    controllerTcpConnection.send(request.getBytes());
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        if (args.length != 2)
            printHelpAndExit();

        String controllerIp = args[0];
        int controllerPort = Integer.parseInt(args[1]);

        new Client(controllerIp, controllerPort);
    }

    private static void printMenu() {
        Utils.out("****************\n");
        Utils.out("pm -- print menu\n");
        Utils.out("sf -- store file\n");
        Utils.out("q  -- quit\n");
        Utils.out("****************\n");
    }

    private static void printHelpAndExit() {
        Utils.out("USAGE: java Client <controller-host> <controller-port>\n");
        System.exit(-1);
    }
}
