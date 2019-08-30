package cs555.dfs.wireformats;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class StoreChunkRequest implements Event {
    private final String fileName;
    private final int chunkSequence;
    private final byte[] data;
    // todo -- add support for additional chunk servers

    public StoreChunkRequest(String fileName, int chunkSequence, byte[] data) {
        this.fileName = fileName;
        this.chunkSequence = chunkSequence;
        this.data = data;
    }

    @Override
    public int getProtocol() {
        return Protocol.STORE_CHUNK_REQUEST;
    }

    @Override
    public byte[] getBytes() throws IOException {
        /**
         * Event Type (int): STORE_CHUNK_REQUEST
         * Filename size (int)
         * Filename (String)
         * Chunk index (int)
         * Chunk size (int)
         * Chunk data (byte[])
         */
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

        dataOutputStream.writeInt(getProtocol());
        dataOutputStream.writeInt(chunkSequence);
        dataOutputStream.writeInt(fileName.length());
        dataOutputStream.write(fileName.getBytes());
        dataOutputStream.writeInt(data.length);
        dataOutputStream.write(data);
        dataOutputStream.flush();

        byte[] data = byteArrayOutputStream.toByteArray();

        byteArrayOutputStream.close();
        dataOutputStream.close();

        return data;
    }

    @Override
    public String toString() {
        return "StoreChunkRequest{" +
            "fileName='" + fileName + '\'' +
            ", chunkSequence=" + chunkSequence +
            ", data.length=" + data.length +
            '}';
    }

    public String getFileName() {
        return fileName;
    }

    public int getChunkSequence() {
        return chunkSequence;
    }

    public byte[] getData() {
        return data;
    }
}
