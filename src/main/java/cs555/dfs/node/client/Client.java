package cs555.dfs.node.client;

import cs555.dfs.node.Node;
import cs555.dfs.transport.TcpConnection;
import cs555.dfs.transport.TcpServer;
import cs555.dfs.util.Utils;
import cs555.dfs.wireformats.*;

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
            if (input.startsWith("sf")) {
                // todo -- turn on ask for file
                // todo -- wait for file to write before giving back command line prompt
//                Utils.out("fileName: \n");
//                String fileName = scanner.next();
                String fileName = "/s/chopin/a/grad/sgaxcell/cs555-assignment1/bogus.txt";
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
                String fileName = "/s/chopin/a/grad/sgaxcell/cs555-assignment1/bogus.txt";
                Path path = Paths.get(fileName);
                if (!path.toFile().exists()) {
                    Utils.out("file does not exist: " + path + "\n");
                    continue;
                }
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
            else if (input.startsWith("q")) {
                Utils.out("goodbye\n");
                System.exit(0);
            }
        }
    }

    private void listFiles() {
        fileLister.setIsRunning(true);
        FileListRequest request = new FileListRequest(getServerAddress(), controllerTcpConnection.getLocalSocketAddress());
        controllerTcpConnection.send(request.getBytes());
    }

    private void printProgressBar() {
        try {
            while (fileReader.isRunning() || fileLister.isRunning()) {
                Thread.sleep(1000);
                Utils.out(".");
            }
//            Utils.out("\n");
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void printMenu() {
        Utils.out("****************\n");
        Utils.out("h  -- print menu\n");
        Utils.out("sf -- store file\n");
        Utils.out("lf -- list files\n");
        Utils.out("rf -- read file\n");
        Utils.out("q  -- quit\n");
        Utils.out("****************\n");
    }

    private void storeFile(Path path) {
        fileStorer.storeFile(path);
    }

    private void retrieveFile(Path path) {
        Utils.info("Retrieving " + path + " ...", false);
        fileReader.setIsRunning(true);
        RetrieveFileRequest request = new RetrieveFileRequest(getServerAddress(), controllerTcpConnection.getLocalSocketAddress(), Utils.getCanonicalPath(path));
        controllerTcpConnection.send(request.getBytes());
    }

    public static void main(String[] args) {
        if (args.length != 2)
            printHelpAndExit();

        String controllerIp = args[0];
        int controllerPort = Integer.parseInt(args[1]);

        new Client(controllerIp, controllerPort).run();
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
            default:
                throw new RuntimeException(String.format("received an unknown message with protocol %d", protocol));
        }
    }

    private void handleFileListResponse(Message message) {
        FileListResponse response = (FileListResponse) message;
        Utils.debug("received: " + response);
        fileLister.handleFileListResponse(response);
    }

    private void handleStoreChunkResponse(Message message) {
        StoreChunkResponse response = (StoreChunkResponse) message;
        Utils.debug("received: " + response);
        fileStorer.handleStoreChunkResponse(response);

    }

    private void handleRetrieveFileResponse(Message message) {
        RetrieveFileResponse response = (RetrieveFileResponse) message;
        Utils.debug("received: " + response);
        fileReader.handleRetrieveFileResponse(response);
    }

    private void handleRetrieveChunkResponse(Message message) {
        RetrieveChunkResponse response = (RetrieveChunkResponse) message;
        Utils.debug("received: " + response);
        fileReader.handleRetrieveChunkResponse(response);
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
