package cs555.dfs.util;

import cs555.dfs.transport.TcpServer;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class Utils {
    public static final int HASH_CODE_BYTE_SIZE = 40;
    private static boolean debug = true;

    public static void out(Object o) {
        System.out.print(o);
    }

    public static void info(Object o) {
        info(o, true);
    }

    public static void info(Object o, boolean newLine) {
        if (newLine)
            System.out.println("INFO: " + o);
        else
            System.out.print("INFO: " + o);
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

    public static String[] splitServerAddress(String serverAddress) {
        if (serverAddress == null)
            return new String[0];
        return serverAddress.split(":");
    }

    public static String createSha1FromBytes(byte[] data) {
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA1");
        }
        catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return "";
        }
        byte[] hash = digest.digest(data);
        BigInteger hashInt = new BigInteger(1, hash);
        return hashInt.toString(16);
    }

    public static String padHashCodeWithZeros(String hashCode) {
        return padStringWithZeros(hashCode, HASH_CODE_BYTE_SIZE);
    }

    public static String padStringWithZeros(String string, int expectedStringLength) {
        String paddedString = string;
        if (paddedString.length() < expectedStringLength) {
            StringBuilder sb = new StringBuilder(paddedString);
            while (sb.length() < expectedStringLength)
                sb.insert(0, "0");
            paddedString = sb.toString();
        }
        return paddedString;
    }

    public static boolean compareChecksums(List<String> checksums1, List<String> checksums2, List<Integer> corruptSlices) {
        boolean equal = true;

        int slice1 = 0;
        int slice2 = 0;

        Iterator<String> iterator1 = checksums1.iterator();
        Iterator<String> iterator2 = checksums2.iterator();

        while (iterator1.hasNext() && iterator2.hasNext()) {
            String cs1 = iterator1.next();
            String cs2 = iterator2.next();
            if (!cs1.equals(cs2)) {
                equal = false;
                corruptSlices.add(slice1);
            }
            slice1++;
            slice2++;
        }

        while (iterator1.hasNext()) {
            iterator1.next();
            equal = false;
            corruptSlices.add(slice1);
            slice1++;
        }

        while (iterator2.hasNext()) {
            iterator2.next();
            equal = false;
            corruptSlices.add(slice2);
            slice2++;
        }

        return equal;
    }
}
