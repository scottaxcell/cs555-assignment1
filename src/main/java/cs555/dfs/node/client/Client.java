package cs555.dfs.node.client;

import cs555.dfs.node.Node;
import cs555.dfs.transport.TcpConnection;
import cs555.dfs.transport.TcpServer;
import cs555.dfs.util.Utils;
import cs555.dfs.wireformats.*;
import cs555.dfs.wireformats.erasure.*;

import java.io.IOException;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.regex.Pattern;

public class Client implements Node {
    private final FileReader fileReader;
    private final FileStorer fileStorer;
    private final FileLister fileLister;
    private final TcpServer tcpServer;
    private TcpConnection controllerTcpConnection;

    public Client(String controllerIp, int controllerPort) {
        fileReader = new FileReader(this);
        fileStorer = new FileStorer(this);
        fileLister = new FileLister(this);
        tcpServer = new TcpServer(0, this);

        try {
            Socket socket = new Socket(controllerIp, controllerPort);
            controllerTcpConnection = new TcpConnection(socket, this);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if (args.length != 2)
            printHelpAndExit();

        String controllerIp = args[0];
        int controllerPort = Integer.parseInt(args[1]);

        new Client(controllerIp, controllerPort).run();
    }

    void run() {
        new Thread(tcpServer).start();
        handleCmdLineInput();
    }

    private void handleCmdLineInput() {
        printMenu();

        String input;
        Scanner scanner = new Scanner(System.in);
        scanner.useDelimiter(Pattern.compile("[\\r\\n;]+"));

        while (true) {
            Utils.out("\n");

            input = scanner.next();
            if (input.startsWith("sfe")) {
//                Utils.out("file: \n");
//                String fileName = scanner.next();
                String fileName = "/s/chopin/a/grad/sgaxcell/cs555-assignment1/files/MobyDick.txt";
                Path path = Paths.get(fileName);
                if (!path.toFile().exists()) {
                    Utils.error("file does not exist: " + path);
                    continue;
                }
                storeFileErasure(path);
                printProgressBar();
                Utils.info("Stored (erasure) " + path);
            }
            else if (input.startsWith("rfe")) {
//                Utils.out("file: \n");
//                String fileName = scanner.next();
                String fileName = "/s/chopin/a/grad/sgaxcell/cs555-assignment1/files/MobyDick.txt";
                Path path = Paths.get(fileName);
                retrieveFileErasure(path);
                printProgressBar();
            }
            else if (input.startsWith("lfe")) {
                listFilesErasure();
                printProgressBar();
            }
            else if (input.startsWith("sf")) {
                Utils.out("file: \n");
                String fileName = scanner.next();
                Path path = Paths.get(fileName);
                if (!path.toFile().exists()) {
                    Utils.error("file does not exist: " + path);
                    continue;
                }
                storeFile(path);
                printProgressBar();
                Utils.info("Stored " + path);
            }
            else if (input.startsWith("rf")) {
                Utils.out("file: \n");
                String fileName = scanner.next();
                Path path = Paths.get(fileName);
                retrieveFile(path);
                printProgressBar();
            }
            else if (input.startsWith("lf")) {
                listFiles();
                printProgressBar();
            }
            else if (input.startsWith("h")) {
                printMenu();
            }
            else if (input.startsWith("e")) {
                Utils.info("Auf Wiedersehen");
                System.exit(0);
            }
        }
    }

    private void listFiles() {
        fileLister.setIsRunning(true);
        FileListRequest request = new FileListRequest(getServerAddress(), controllerTcpConnection.getLocalSocketAddress());
        controllerTcpConnection.send(request.getBytes());
    }

    private void listFilesErasure() {
        fileLister.setIsRunning(true);
        FileListRequestErasure request = new FileListRequestErasure(getServerAddress(), controllerTcpConnection.getLocalSocketAddress());
        controllerTcpConnection.send(request.getBytes());
    }

    private void printProgressBar() {
        try {
            while (fileReader.isRunning() || fileLister.isRunning() || fileStorer.isRunning()) {
                Thread.sleep(500);
                Utils.out(".");
            }
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void printMenu() {
        Utils.out("***************************\n");
        Utils.out("h   -- print menu\n");
        Utils.out("sf  -- store file\n");
        Utils.out("sfe -- store file (erasure)\n");
        Utils.out("lf  -- list files\n");
        Utils.out("lfe -- list files (erasure)\n");
        Utils.out("rf  -- read file\n");
        Utils.out("rfe -- read file (erasure)\n");
        Utils.out("e   -- exit\n");
        Utils.out("***************************\n");
    }

    private void storeFile(Path path) {
        Utils.info("Storing " + path + " ...", false);
        fileStorer.storeFile(path);
    }

    private void storeFileErasure(Path path) {
        Utils.info("Storing (erasure) " + path + " ...", false);
        fileStorer.storeFileErasure(path);
    }

    private void retrieveFile(Path path) {
        Utils.info("Retrieving " + path + " ...", false);
        fileReader.setIsRunning(true);
        fileReader.setFileName(path.getFileName().toString());
        RetrieveFileRequest request = new RetrieveFileRequest(getServerAddress(), controllerTcpConnection.getLocalSocketAddress(), Utils.getCanonicalPath(path));
        controllerTcpConnection.send(request.getBytes());
    }

    private void retrieveFileErasure(Path path) {
        Utils.info("Retrieving (erasure) " + path + " ...", false);
        fileReader.setIsRunning(true);
        fileReader.setFileName(path.getFileName().toString());
        RetrieveFileRequestErasure request = new RetrieveFileRequestErasure(getServerAddress(), controllerTcpConnection.getLocalSocketAddress(), Utils.getCanonicalPath(path));
        controllerTcpConnection.send(request.getBytes());
    }

    private static void printHelpAndExit() {
        Utils.out("USAGE: java Client <controller-host> <controller-port>\n");
        System.exit(-1);
    }

    public TcpConnection getControllerTcpConnection() {
        return controllerTcpConnection;
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
            case Protocol.CORRUPT_CHUNK:
                handleChunkCorruption(message);
                break;
            case Protocol.FILE_LIST_RESPONSE:
                handleFileListResponse(message);
                break;


            case Protocol.STORE_SHARD_RESPONSE:
                handleStoreShardResponse(message);
                break;
            case Protocol.FILE_LIST_RESPONSE_ERASURE:
                handleFileListResponseErasure(message);
                break;
            case Protocol.RETRIEVE_FILE_RESPONSE_ERASURE:
                handleRetrieveFileResponseErasure(message);
                break;
            case Protocol.RETRIEVE_SHARD_RESPONSE:
                handleRetrieveShardResponse(message);
                break;
            default:
                throw new RuntimeException(String.format("received an unknown message with protocol %d", protocol));
        }
    }

    private void handleFileListResponse(Message message) {
        FileListResponse response = (FileListResponse) message;
        Utils.debug("received: " + response);
        fileLister.handleFileListResponse(response);
    }

    private void handleFileListResponseErasure(Message message) {
        FileListResponseErasure response = (FileListResponseErasure) message;
        Utils.debug("received: " + response);
        fileLister.handleFileListResponseErasure(response);
    }

    private void handleStoreChunkResponse(Message message) {
        StoreChunkResponse response = (StoreChunkResponse) message;
        Utils.debug("received: " + response);
        fileStorer.handleStoreChunkResponse(response);
    }

    private void handleStoreShardResponse(Message message) {
        StoreShardResponse response = (StoreShardResponse) message;
        Utils.debug("received: " + response);
        fileStorer.handleStoreShardResponse(response);
    }

    private void handleRetrieveFileResponse(Message message) {
        RetrieveFileResponse response = (RetrieveFileResponse) message;
        Utils.debug("received: " + response);
        fileReader.handleRetrieveFileResponse(response);
    }

    private void handleRetrieveFileResponseErasure(Message message) {
        RetrieveFileResponseErasure response = (RetrieveFileResponseErasure) message;
        Utils.debug("received: " + response);
        fileReader.handleRetrieveFileResponseErasure(response);
    }

    private void handleRetrieveChunkResponse(Message message) {
        RetrieveChunkResponse response = (RetrieveChunkResponse) message;
        Utils.debug("received: " + response);
        fileReader.handleRetrieveChunkResponse(response);
    }

    private void handleRetrieveShardResponse(Message message) {
        RetrieveShardResponse response = (RetrieveShardResponse) message;
        Utils.debug("received: " + response);
        fileReader.handleRetrieveShardResponse(response);
    }

    private void handleChunkCorruption(Message message) {
        CorruptChunk corruptChunk = (CorruptChunk) message;
        Utils.debug("received: " + corruptChunk);
        fileReader.handleCorruptChunk(corruptChunk);
    }

    @Override
    public String getNodeTypeAsString() {
        return "Client";
    }

    @Override
    public void registerNewTcpConnection(TcpConnection tcpConnection) {
    }

    @Override
    public String getServerAddress() {
        return Utils.getServerAddress(tcpServer);
    }
}
