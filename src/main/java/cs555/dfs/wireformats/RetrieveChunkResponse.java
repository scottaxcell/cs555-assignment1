package cs555.dfs.wireformats;

import java.io.*;

public class RetrieveChunkResponse implements Message {
    private MessageHeader messageHeader;
    private Chunk chunk;
    private byte[] fileData;

    public RetrieveChunkResponse(String serverAddress, String sourceAddress, Chunk chunk, byte[] fileData) {
        this.messageHeader = new MessageHeader(getProtocol(), serverAddress, sourceAddress);
        this.chunk = chunk;
        this.fileData = fileData;
    }

    @Override
    public int getProtocol() {
        return Protocol.RETRIEVE_CHUNK_RESPONSE;
    }

    @Override
    public byte[] getBytes() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

            messageHeader.serialize(dataOutputStream);
            chunk.serialize(dataOutputStream);
            WireformatUtils.serializeBytes(dataOutputStream, fileData);

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

    public RetrieveChunkResponse(byte[] bytes) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

            messageHeader = MessageHeader.deserialize(dataInputStream);
            chunk = Chunk.deserialize(dataInputStream);
            fileData = WireformatUtils.deserializeBytes(dataInputStream);

            byteArrayInputStream.close();
            dataInputStream.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "RetrieveChunkResponse{" +
            "messageHeader=" + messageHeader +
            ", chunk=" + chunk +
            ", data.length=" + fileData.length +
            '}';
    }

    public String getFileName() {
        return chunk.getFileName();
    }

    public int getSequence() {
        return chunk.getSequence();
    }

    public byte[] getFileData() {
        return fileData;
    }
}
