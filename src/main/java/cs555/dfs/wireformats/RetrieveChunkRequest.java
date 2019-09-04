package cs555.dfs.wireformats;

import java.io.*;

public class RetrieveChunkRequest implements Message {
    private MessageHeader messageHeader;
    private Chunk chunk;

    public RetrieveChunkRequest(String serverAddress, String sourceAddress, Chunk chunk) {
        this.messageHeader = new MessageHeader(getProtocol(), serverAddress, sourceAddress);
        this.chunk = chunk;
    }

    @Override
    public int getProtocol() {
        return Protocol.RETRIEVE_CHUNK_REQUEST;
    }

    @Override
    public byte[] getBytes() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

            messageHeader.serialize(dataOutputStream);
            chunk.serialize(dataOutputStream);

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

    public RetrieveChunkRequest(byte[] bytes) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

            messageHeader = MessageHeader.deserialize(dataInputStream);
            chunk = Chunk.deserialize(dataInputStream);

            byteArrayInputStream.close();
            dataInputStream.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "RetrieveChunkRequest{" +
            "messageHeader=" + messageHeader +
            ", chunk=" + chunk +
            '}';
    }

    public String getFileName() {
        return chunk.getFileName();
    }

    public int getSequence() {
        return chunk.getSequence();
    }

    public String getServerAddress() {
        return messageHeader.getServerAddress();
    }
}
