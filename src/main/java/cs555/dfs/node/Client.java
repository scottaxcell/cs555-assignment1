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
import java.util.stream.Collectors;

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
    private List<FileChunkifier.FileDataChunk> currentFileDataChunks = new ArrayList<>();
    private List<WireChunk> currentWireChunks = new ArrayList<>();

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
            case Protocol.RETRIEVE_FILE_RESPONSE:
                handleRetrieveFileResponse(message);
                break;
            case Protocol.RETRIEVE_CHUNK_RESPONSE:
                handleRetrieveChunkResponse(message);
                break;
            default:
                throw new RuntimeException(String.format("received an unknown message with protocol %d", protocol));
        }
    }

    private void handleRetrieveChunkResponse(Message message) {
        RetrieveFileResponse response = (RetrieveFileResponse) message;
        Utils.debug("received: " + response);
    }

    private void handleRetrieveFileResponse(Message message) {
        RetrieveFileResponse response = (RetrieveFileResponse) message;
        Utils.debug("received: " + response);

        synchronized (currentWireChunks) {
            currentWireChunks = response.getWireChunks();
            if (currentWireChunks.isEmpty())
                return;

            WireChunk wireChunk = currentWireChunks.get(0);
            String fileName = wireChunk.getFileName();
            int sequence = wireChunk.getSequence();
            String serverAddress = wireChunk.getServerAddress();

            TcpSender tcpSender = getTcpSenderFromServerAddress(serverAddress);
            if (tcpSender == null) {
                Utils.error("tcpServer is null");
                return;
            }

            RetrieveChunkRequest request = new RetrieveChunkRequest(getServerAddress(), tcpSender.getSocket().getLocalSocketAddress().toString(),
                fileName, sequence);
            tcpSender.send(request.getBytes());

            currentWireChunks.remove(wireChunk);
        }
    }

    private TcpSender getTcpSenderFromServerAddress(String serverAddress) {
        TcpConnection tcpConnection = connections.get(serverAddress);
        if (tcpConnection != null)
            return tcpConnection.getTcpSender();
        else {
            String[] splitServerAddress = Utils.splitServerAddress(serverAddress);
            try {
                Socket socket = new Socket(splitServerAddress[0], Integer.valueOf(splitServerAddress[1]));
                return new TcpSender(socket);
            }
            catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }
    }

    private void handleStoreChunkResponse(Message message) {
        StoreChunkResponse response = (StoreChunkResponse) message;
        Utils.debug("received: " + response);

        String fileName = response.getFileName();
        int chunkSequence = response.getChunkSequence();
        FileChunkifier.FileDataChunk fileDataChunk;
        synchronized (currentFileDataChunks) {
            FileChunkifier.FileDataChunk finderChunk = new FileChunkifier.FileDataChunk(fileName, chunkSequence);
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

        TcpSender tcpSender = getTcpSenderFromServerAddress(firstChunkServerAddress);
        if (tcpSender == null) {
            Utils.error("tcpServer is null");
            return;
        }

        List<String> nextServers = chunkServerAddresses.stream()
            .skip(1).collect(Collectors.toList());

        StoreChunk storeChunk = new StoreChunk(getServerAddress(), tcpSender.getSocket().getLocalSocketAddress().toString(),
            fileName, fileDataChunk.sequence, fileDataChunk.fileData, nextServers);
        tcpSender.send(storeChunk.getBytes());

        sendNextStoreChunkRequest();
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
                // todo -- wait for file to write before giving back command line prompt
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
            if (input.startsWith("rf")) {
                // todo -- turn on ask for file
                // todo -- wait for file to write before giving back command line prompt
                // todo -- ask for output path
//                Utils.out("fileName: \n");
//                String fileName = scanner.next();
                String fileName = "/s/chopin/a/grad/sgaxcell/cs555-assignment1/bogus.bin";
                Path path = Paths.get(fileName);
                if (!path.toFile().exists()) {
                    Utils.out("file does not exist: " + path + "\n");
                    continue;
                }
                retrieveFile(path);
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

    private void retrieveFile(Path path) {
        RetrieveFileRequest request = new RetrieveFileRequest(getServerAddress(), controllerTcpConnection.getLocalSocketAddress(), Utils.getCanonicalPath(path));
        controllerTcpConnection.send(request.getBytes());
    }

    private void storeFile(Path path) {
        synchronized (currentFileDataChunks) {
            currentFileDataChunks = Collections.synchronizedList(FileChunkifier.chunkifyFileToFileDataChunks(path));
            if (!currentFileDataChunks.isEmpty()) {
                sendNextStoreChunkRequest();
            }
        }
    }

    private void sendNextStoreChunkRequest() {
        synchronized (currentFileDataChunks) {
            if (!currentFileDataChunks.isEmpty()) {
                FileChunkifier.FileDataChunk fileDataChunk = currentFileDataChunks.get(0);
                StoreChunkRequest request = new StoreChunkRequest(getServerAddress(),
                    controllerTcpConnection.getLocalSocketAddress(), fileDataChunk.fileName, fileDataChunk.sequence);
                controllerTcpConnection.send(request.getBytes());
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
