package cs555.dfs.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class FileChunkifier {
    private static final int CHUNK_SIZE = 64 * 1024; // 64 KB

    public static List<byte[]> chunkifyFile(File file) {
        List<byte[]> bytes = new ArrayList<>();
        byte[] fileAsBytes = readFileToBytes(file);

        long numChunks = file.length() / CHUNK_SIZE;
        Utils.debug("numChunks: " + numChunks);
        int remainderChunk = (int) (file.length() % CHUNK_SIZE);
        Utils.debug("remainderChunk: " + remainderChunk);

        int chunkIdx = 0;
        for (; chunkIdx < numChunks; chunkIdx++) {
            byte[] b = new byte[CHUNK_SIZE];
            System.arraycopy(fileAsBytes, chunkIdx * CHUNK_SIZE, b, 0, CHUNK_SIZE);
            bytes.add(b);
        }
        if (remainderChunk > 0) {
            byte[] b = new byte[remainderChunk];
            System.arraycopy(fileAsBytes, chunkIdx * CHUNK_SIZE, b, 0, remainderChunk);
            bytes.add(b);
        }

        return bytes;
    }

    private static byte[] readFileToBytes(File file) {
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            byte[] bytes = new byte[(int) randomAccessFile.length()];
            randomAccessFile.readFully(bytes);
            randomAccessFile.close();
            return bytes;
        }
        catch (IOException e) {
            e.printStackTrace();
            return new byte[0];
        }
    }

    public static byte[] convertByteArrayListToByteArray(List<byte[]> byteArrayList) {
        int numBytes = 0;
        for (byte[] b : byteArrayList)
            numBytes += b.length;
        byte[] bytes = new byte[numBytes];
        int chunkIdx = 0;
        for (byte[] b : byteArrayList) {
            System.arraycopy(b, 0, bytes, chunkIdx * CHUNK_SIZE, b.length);
            chunkIdx++;
        }
        return bytes;
    }

    public static void main(String[] args) {
        Path path = Paths.get("bogus.bin");
        byte[] randomBytes = new byte[CHUNK_SIZE * 5 + 2];
        new Random().nextBytes(randomBytes);
        try {
            Files.write(path, randomBytes);
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        List<byte[]> fileChunks = chunkifyFile(path.toFile());
        Utils.debug("num fileChunks: " + fileChunks.size());
        byte[] convertedBytes = convertByteArrayListToByteArray(fileChunks);
        Path duplicate = Paths.get("bogus.duplicate.bin");
        try {
            Files.write(duplicate, convertedBytes);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}

