package cs555.dfs.util;

import cs555.dfs.transport.TcpServer;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Random;

public class Utils {
    private static boolean debug = true;

    public static void out(Object o) {
        System.out.print(o);
    }

    public static void info(Object o) {
        System.out.println("\nINFO: " + o);
    }

    public static void debug(Object o) {
        if (debug)
            System.out.println("DEBUG: " + o);
    }

    public static void error(Object o) {
        System.err.println("\nERROR: " + o);
    }

    public static byte intToByte(int i) {
        return (byte) i;
    }

    public static int generateRandomWeight() {
        return new Random().nextInt(10) + 1;
    }

    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static String getCanonicalPath(Path path) {
        try {
            return path.toFile().getCanonicalPath();
        }
        catch (IOException e) {
            e.printStackTrace();
            return "";
        }
    }

    public static String getServerAddress(TcpServer tcpServer) {
        if (tcpServer == null)
            return "";
        return String.format("%s:%d", tcpServer.getIp(), tcpServer.getPort());
    }
}
