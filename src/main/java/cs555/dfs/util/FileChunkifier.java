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
    private static final int SLICE_SIZE = 8 * 1024; // 8 KB

    public static List<ChunkData> chunkifyFileToDataChunks(Path path) {
        List<ChunkData> dataChunks = new ArrayList<>();
        List<byte[]> bytes = chunkifyFile(path);
        for (int i = 0; i < bytes.size(); i++) {
            dataChunks.add(new ChunkData(Utils.getCanonicalPath(path), i, bytes.get(i)));
        }
        return dataChunks;
    }

    public static List<byte[]> chunkifyFile(Path path) {
        List<byte[]> bytes = new ArrayList<>();
        byte[] fileAsBytes = readFileToBytes(path.toFile());

        long numChunks = path.toFile().length() / CHUNK_SIZE;
        Utils.debug("numChunks: " + numChunks);
        int remainderChunk = (int) (path.toFile().length() % CHUNK_SIZE);
        Utils.debug("remainderChunk size: " + remainderChunk);

        int chunkSequence = 0;
        for (; chunkSequence < numChunks; chunkSequence++) {
            byte[] b = new byte[CHUNK_SIZE];
            System.arraycopy(fileAsBytes, chunkSequence * CHUNK_SIZE, b, 0, CHUNK_SIZE);
            bytes.add(b);
        }
        if (remainderChunk > 0) {
            byte[] b = new byte[remainderChunk];
            System.arraycopy(fileAsBytes, chunkSequence * CHUNK_SIZE, b, 0, remainderChunk);
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

    public static void main(String[] args) {
        Path path = Paths.get("bogus.txt");
        byte[] randomBytes = new byte[CHUNK_SIZE * 5 + 2];
        new Random().nextBytes(randomBytes);
        try {
            Files.write(path, randomBytes);
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        List<byte[]> fileChunks = chunkifyFile(path);
        Utils.debug("num fileChunks: " + fileChunks.size());
        byte[] convertedBytes = convertByteArrayListToByteArray(fileChunks);
        Path duplicate = Paths.get("bogus.duplicate.bin");
        try {
            Files.write(duplicate, convertedBytes);
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        int multiplier = 1;
        // checksumming some slices bro
        randomBytes = new byte[CHUNK_SIZE * multiplier];
        new Random().nextBytes(randomBytes);
        Utils.debug("randomBytes.length: " + randomBytes.length);
        List<String> sliceChecksums = createSliceChecksums(randomBytes);
        Utils.debug("sliceChecksums.size: " + sliceChecksums.size());
        for (String checksum : sliceChecksums) {
            Utils.debug(checksum);
        }

//        randomBytes = new byte[CHUNK_SIZE * multiplier];
//        randomBytes[0] = Byte.parseByte("123");
//        List<Integer> corruptSlices = new ArrayList<>();
//        List<String> sliceChecksums2 = FileChunkifier.createSliceChecksums(randomBytes);
//        FileChunkifier.compareChecksums(sliceChecksums, sliceChecksums2, corruptSlices);
//        Utils.debug("num corrupt slices: " + corruptSlices.size());
//        Utils.debug(corruptSlices.stream().map(String::valueOf).collect(Collectors.joining(", ")));
    }

    public static byte[] convertByteArrayListToByteArray(List<byte[]> byteArrayList) {
        int numBytes = 0;
        for (byte[] b : byteArrayList)
            numBytes += b.length;
        byte[] bytes = new byte[numBytes];
        int bytesCopied = 0;
        for (byte[] b : byteArrayList) {
            System.arraycopy(b, 0, bytes, bytesCopied, b.length);
            bytesCopied += b.length;
        }
        return bytes;
    }

    public static List<String> createSliceChecksums(byte[] data) {
        List<String> checksums = new ArrayList<>();
        List<byte[]> slices = FileChunkifier.sliceData(data);
        for (byte[] slice : slices) {
            String sha1FromBytes = Utils.createSha1FromBytes(slice);
            checksums.add(sha1FromBytes);
        }
        return checksums;
    }

    public static List<byte[]> sliceData(byte[] data) {
        List<byte[]> slices = new ArrayList<>();

        long numSlices = data.length / SLICE_SIZE;
        Utils.debug("numSlices: " + numSlices);
        int remainderSlice = (data.length % SLICE_SIZE);
        Utils.debug("remainderSlice size: " + remainderSlice);

        int sequence = 0;
        for (; sequence < numSlices; sequence++) {
            byte[] slice = new byte[SLICE_SIZE];
            System.arraycopy(data, sequence * SLICE_SIZE, slice, 0, SLICE_SIZE);
            slices.add(slice);
        }
        if (remainderSlice > 0) {
            byte[] slice = new byte[remainderSlice];
            System.arraycopy(data, sequence * SLICE_SIZE, slice, 0, remainderSlice);
            slices.add(slice);
        }

        return slices;
    }

    public static byte[] truncateBytes(byte[] bytes, int size) {
        byte[] truncatedBytes = new byte[size];
        System.arraycopy(bytes, 0, truncatedBytes, 0, size);
        return truncatedBytes;
    }
}

