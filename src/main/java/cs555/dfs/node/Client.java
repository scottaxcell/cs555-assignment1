package cs555.dfs.node;

import cs555.dfs.transport.TcpConnection;
import cs555.dfs.transport.TcpServer;
import cs555.dfs.util.FileChunkifier;
import cs555.dfs.util.Utils;
import cs555.dfs.wireformats.Event;
import cs555.dfs.wireformats.StoreChunkRequest;

import java.io.IOException;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Pattern;

public class Client implements Node {
    private final TcpServer tcpServer;
    private TcpConnection controllerTcpConnection;

    public Client(String controllerIp, int controllerPort) {
        tcpServer = new TcpServer(0, this);
//        new Thread(tcpServer).start();
//
//        try {
//            Socket socket = new Socket(controllerIp, controllerPort);
//            controllerTcpConnection = TcpConnection.of(socket, this);
//        }
//        catch (IOException e) {
//            e.printStackTrace();
//        }

        handleCmdLineInput();
    }

    @Override
    public void onEvent(Event event) {

    }

    @Override
    public String getNodeTypeAsString() {
        return "Client";
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
                String fileName = "./bogus.bin";
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

    private void storeFile(Path path) {
        Utils.debug("storing file: " + path);
        Utils.debug("storing file: " + path.toAbsolutePath());

        Socket socket = null;
        try {
            socket = new Socket("127.0.0.1", 11322);
        }
        catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        TcpConnection chunkServerTcpConnection = TcpConnection.of(socket, this);

        List<byte[]> bytes = FileChunkifier.chunkifyFile(path.toFile());
        int chunkIdx = 0;
        for (byte[] chunkData : bytes) {
            StoreChunkRequest request = new StoreChunkRequest(path.toAbsolutePath().toString(), chunkIdx, chunkData);
            try {
                chunkServerTcpConnection.send(request.getBytes());
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            chunkIdx++;
        }
        // todo -- chunkify file, then for each chunk do the following
        // todo -- ask controller for chunk servers
        // todo -- send chunk plus next servers to first server
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
