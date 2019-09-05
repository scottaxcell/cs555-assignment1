package cs555.dfs.wireformats;

import java.io.*;

public class ReplicateChunk implements Message {
    private MessageHeader messageHeader;
    private Chunk chunk;
    private String destinationAddress;

    public ReplicateChunk(String serverAddress, String sourceAddress, Chunk chunk, String destinationAddress) {
        this.messageHeader = new MessageHeader(getProtocol(), serverAddress, sourceAddress);
        this.chunk = chunk;
        this.destinationAddress = destinationAddress;
    }

    @Override
    public int getProtocol() {
        return Protocol.REPLICATE_CHUNK;
    }

    @Override
    public byte[] getBytes() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

            messageHeader.serialize(dataOutputStream);
            chunk.serialize(dataOutputStream);
            WireformatUtils.serializeString(dataOutputStream, destinationAddress);

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

    public ReplicateChunk(byte[] bytes) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

            messageHeader = MessageHeader.deserialize(dataInputStream);
            chunk = Chunk.deserialize(dataInputStream);
            destinationAddress = WireformatUtils.deserializeString(dataInputStream);

            byteArrayInputStream.close();
            dataInputStream.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "ReplicateChunk{" +
            "messageHeader=" + messageHeader +
            ", chunk=" + chunk +
            ", destinationAddress='" + destinationAddress + '\'' +
            '}';
    }

    public String getFileName() {
        return chunk.getFileName();
    }

    public int getSequence() {
        return chunk.getSequence();
    }

    public String getSourceAddress() {
        return messageHeader.getSourceAddress();
    }

    public String getDestinationAddress() {
        return destinationAddress;
    }
}
