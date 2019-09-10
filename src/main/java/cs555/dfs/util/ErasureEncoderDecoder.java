package cs555.dfs.util;

import cs555.dfs.wireformats.WireformatUtils;
import erasure.ReedSolomon;

import java.io.*;
import java.util.Random;

public class ErasureEncoderDecoder {
    public static final int DATA_SHARDS = 6;
    public static final int PARITY_SHARDS = 3;
    public static final int TOTAL_SHARDS = DATA_SHARDS + PARITY_SHARDS;

    public static final int BYTES_IN_INT = 4;

    public static byte[][] encode(byte[] data) {

        // total size of the stored data = length of the payload paylod size
        int storedSize = data.length + BYTES_IN_INT;

        // size of a shard. Make sure all the shards are of the same size.
        // In order to do this, you can padd 0s at the end.
        // This particular code works for 4 data shards.
        // Based on the numer of shards, use a appropriate way to
        // decide on shard size.
        int shardSize = (storedSize + DATA_SHARDS - 1) / DATA_SHARDS;

        // Create a buffer holding the file size, followed by the contents of the file
        // (and padding if required)
        int bufferSize = shardSize * DATA_SHARDS;
        byte[] allBytes;// = new byte[bufferSize];

        /* You should implement the code for copying the file size, payload and padding into the byte array in here. */
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

        try {
            dataOutputStream.writeInt(data.length);
            dataOutputStream.write(data, 0, data.length);

            int requiredZeroPadding = bufferSize - dataOutputStream.size();
            if (requiredZeroPadding > 0)
                dataOutputStream.write(new byte[requiredZeroPadding], 0, requiredZeroPadding);

            dataOutputStream.flush();

            allBytes = byteArrayOutputStream.toByteArray();

            byteArrayOutputStream.close();
            dataOutputStream.close();
        }
        catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        // Make the buffers to hold the shards.
        byte[][] shards = new byte[TOTAL_SHARDS][shardSize];

        // Fill in the data shards
        for (int i = 0; i < DATA_SHARDS; i++)
            System.arraycopy(allBytes, i * shardSize, shards[i], 0, shardSize);

        // Use Reed-Solomon to calculate the parity. Parity codes
        // will be stored in the last two positions in 'shards' 2-D array.
        ReedSolomon reedSolomon = new ReedSolomon(DATA_SHARDS, PARITY_SHARDS);
        reedSolomon.encodeParity(shards, 0, shardSize);

        // finally store the contents of the 'shards' 2-D array

        return shards;
    }

    private static byte[] decode(byte[][] encoded) {
        // Read in any of the shards that are present.
        // (There should be checking here to make sure the input
        // shards are the same size, but there isn't.)
//        byte [] [] shards = new byte [TOTAL_SHARDS] [];
        boolean [] shardPresent = new boolean [TOTAL_SHARDS];
        int shardSize = 0;
        int shardCount = 0;

        // now read the shards from the persistance store
        for (int i = 0; i < TOTAL_SHARDS; i++) {
            // Check if the shard is available.
            // If avaialbe, read its content into shards[i]
            // set shardPresent[i] = true and increase the shardCount by 1.
            if (encoded[i] != null) {
                shardSize = encoded[i].length;
                shardPresent[i] = true;
                shardCount++;
            }
        }

        // We need at least DATA_SHARDS to be able to reconstruct the file.
        if (shardCount < DATA_SHARDS) {
            return null;
        }

        // Make empty buffers for the missing shards.
        for (int i = 0; i < TOTAL_SHARDS; i++) {
            if (!shardPresent[i]) {
                encoded[i] = new byte [shardSize];
            }
        }

        // Use Reed-Solomon to fill in the missing shards
        ReedSolomon reedSolomon = new ReedSolomon(DATA_SHARDS, PARITY_SHARDS);
        reedSolomon.decodeMissing(encoded, shardPresent, 0, shardSize);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

        try {
            for (int i = 0; i < TOTAL_SHARDS; i++)
                dataOutputStream.write(encoded[i]);

            dataOutputStream.flush();

            byte[] bytes = byteArrayOutputStream.toByteArray();

            byteArrayOutputStream.close();
            dataOutputStream.close();

            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

            int dataLength = WireformatUtils.deserializeInt(dataInputStream);
            byte[] data = new byte[dataLength];
            dataInputStream.readFully(data);

            byteArrayInputStream.close();
            dataInputStream.close();

            return data;
        }
        catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) {
        int chunkSize = 64 * 1024;
        byte[] randomBytes = new byte[chunkSize * 5 + 2];
        new Random().nextBytes(randomBytes);

        byte[][] encoded = ErasureEncoderDecoder.encode(randomBytes);
        String preEncodingHash = Utils.createSha1FromBytes(randomBytes);
        Utils.debug("pre:  " + preEncodingHash);

        byte[] decoded = ErasureEncoderDecoder.decode(encoded);
        String postDecodeHash = Utils.createSha1FromBytes(decoded);
        Utils.debug("post: " + postDecodeHash);
    }
}
