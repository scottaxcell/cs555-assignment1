package cs555.dfs.wireformats;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class StoreChunkRequest implements Event {
    private final String fileName;
    private final int chunkIdx;
    private final byte[] data;

    public StoreChunkRequest(String fileName, int chunkIdx, byte[] data) {
        this.fileName = fileName;
        this.chunkIdx = chunkIdx;
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
        dataOutputStream.writeInt(chunkIdx);
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
            ", chunkIdx=" + chunkIdx +
            ", data.length=" + data.length +
            '}';
    }

    public String getFileName() {
        return fileName;
    }

    public int getChunkIdx() {
        return chunkIdx;
    }

    public byte[] getData() {
        return data;
    }
}
