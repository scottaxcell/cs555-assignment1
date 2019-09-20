package cs555.dfs.wireformats;

import java.io.*;

public class StoreSlice implements Message {
    private MessageHeader messageHeader;
    private Chunk chunk;
    private int slice;
    private byte[] sliceBytes;

    public StoreSlice(String serverAddress, String sourceAddress, Chunk chunk, int slice, byte[] sliceBytes) {
        this.messageHeader = new MessageHeader(getProtocol(), serverAddress, sourceAddress);
        this.chunk = chunk;
        this.slice = slice;
        this.sliceBytes = sliceBytes;
    }

    @Override
    public int getProtocol() {
        return Protocol.STORE_SLICE;
    }

    @Override
    public byte[] getBytes() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

            messageHeader.serialize(dataOutputStream);
            chunk.serialize(dataOutputStream);
            WireformatUtils.serializeInt(dataOutputStream, slice);
            WireformatUtils.serializeBytes(dataOutputStream, sliceBytes);

            dataOutputStream.flush();

            byte[] data = byteArrayOutputStream.toByteArray();

            byteArrayOutputStream.close();
            dataOutputStream.close();

            return data;
        }
        catch (IOException e) {
            e.printStackTrace();
            return new byte[0];
        }
    }

    public StoreSlice(byte[] bytes) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

            messageHeader = MessageHeader.deserialize(dataInputStream);
            chunk = Chunk.deserialize(dataInputStream);
            slice = WireformatUtils.deserializeInt(dataInputStream);
            sliceBytes = WireformatUtils.deserializeBytes(dataInputStream);

            byteArrayInputStream.close();
            dataInputStream.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "StoreSlice{" +
            "messageHeader=" + messageHeader +
            ", chunk=" + chunk +
            ", slice=" + slice +
            ", sliceBytes.length=" + sliceBytes.length +
            '}';
    }

    public String getFileName() {
        return chunk.getFileName();
    }

    public int getSequence() {
        return chunk.getSequence();
    }

    public int getSlice() {
        return slice;
    }

    public byte[] getFileData() {
        return sliceBytes;
    }

    public int getVersion() {
        return chunk.getVersion();
    }

    public int getSize() {
        return chunk.getSize();
    }
}
