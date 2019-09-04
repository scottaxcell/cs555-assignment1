package cs555.dfs.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class FileChunkifier {
    private static final int CHUNK_SIZE = 64 * 1024; // 64 KB

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

    public static List<FileDataChunk> chunkifyFileToFileDataChunks(Path path) {
        List<FileDataChunk> fileDataChunks = new ArrayList<>();
        List<byte[]> bytes = chunkifyFile(path);
        for (int i = 0; i < bytes.size(); i++) {
            fileDataChunks.add(new FileDataChunk(Utils.getCanonicalPath(path), i, bytes.get(i)));
        }
        return fileDataChunks;
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
        int chunkSequence = 0;
        for (byte[] b : byteArrayList) {
            System.arraycopy(b, 0, bytes, chunkSequence * CHUNK_SIZE, b.length);
            chunkSequence++;
        }
        return bytes;
    }


    private static final int SLICE_SIZE = 8 * 1024; // 8 KB

    public static List<byte[]> sliceFileData(byte[] fileData) {
        List<byte[]> slices = new ArrayList<>();

        long numSlices = fileData.length / SLICE_SIZE;
        Utils.debug("numSlices: " + numSlices);
        int remainderSlice = (fileData.length % SLICE_SIZE);
        Utils.debug("remainderSlice size: " + remainderSlice);

        int chunkSequence = 0;
        for (; chunkSequence < numSlices; chunkSequence++) {
            byte[] slice = new byte[SLICE_SIZE];
            System.arraycopy(fileData, chunkSequence * SLICE_SIZE, slice, 0, SLICE_SIZE);
            slices.add(slice);
        }
        if (remainderSlice > 0) {
            byte[] slice = new byte[remainderSlice];
            System.arraycopy(fileData, chunkSequence * SLICE_SIZE, slice, 0, remainderSlice);
            slices.add(slice);
        }

        return slices;
    }

    public static List<String> createSliceChecksums(byte[] fileData) {
        List<String> checksums = new ArrayList<>();
        List<byte[]> slices = FileChunkifier.sliceFileData(fileData);
        for (byte[] slice : slices) {
            String sha1FromBytes = Utils.createSha1FromBytes(slice);
            checksums.add(sha1FromBytes);
        }
        return checksums;
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
        randomBytes[0] = Byte.parseByte("123");
        List<Integer> corruptSlices = new ArrayList<>();
        List<String> sliceChecksums2 = FileChunkifier.createSliceChecksums(randomBytes);
        FileChunkifier.compareChecksums(sliceChecksums, sliceChecksums2, corruptSlices);
        Utils.debug("num corrupt slices: " + corruptSlices.size());
        Utils.debug(corruptSlices.stream().map(String::valueOf).collect(Collectors.joining(", ")));

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
            equal = false;
            corruptSlices.add(slice1);
            slice1++;
        }

        while (iterator2.hasNext()) {
            equal = false;
            corruptSlices.add(slice2);
            slice2++;
        }

        return equal;
    }

    public static class FileDataChunk {
        public final String fileName;
        public final int sequence;
        public byte[] fileData;

        public FileDataChunk(String fileName, int sequence, byte[] fileData) {
            this.fileName = fileName;
            this.sequence = sequence;
            this.fileData = fileData;
        }

        public FileDataChunk(String fileName, int sequence) {
            this.fileName = fileName;
            this.sequence = sequence;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FileDataChunk that = (FileDataChunk) o;
            return sequence == that.sequence;
        }

        @Override
        public int hashCode() {
            return Objects.hash(sequence);
        }

        public int getSequence() {
            return sequence;
        }

        public byte[] getFileData() {
            return fileData;
        }
    }
}

