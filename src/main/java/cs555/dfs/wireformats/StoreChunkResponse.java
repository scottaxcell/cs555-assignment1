package cs555.dfs.wireformats;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class StoreChunkResponse implements Message {
    private MessageHeader messageHeader;
    private Chunk chunk;
    private List<String> chunkServerAddresses = new ArrayList<>();

    public StoreChunkResponse(String serverAddress, String sourceAddress, Chunk chunk, List<String> chunkServerAddresses) {
        this.messageHeader = new MessageHeader(getProtocol(), serverAddress, sourceAddress);
        this.chunk = chunk;
        this.chunkServerAddresses = chunkServerAddresses;
    }

    @Override
    public int getProtocol() {
        return Protocol.STORE_CHUNK_RESPONSE;
    }

    @Override
    public byte[] getBytes() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

            messageHeader.serialize(dataOutputStream);
            chunk.serialize(dataOutputStream);
            int numAddresses = chunkServerAddresses.size();
            WireformatUtils.serializeInt(dataOutputStream, numAddresses);
            for (String chunkServerAddress : chunkServerAddresses)
                WireformatUtils.serializeString(dataOutputStream, chunkServerAddress);

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

    public StoreChunkResponse(byte[] bytes) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

            messageHeader = MessageHeader.deserialize(dataInputStream);
            chunk = Chunk.deserialize(dataInputStream);
            int numAddresses = WireformatUtils.deserializeInt(dataInputStream);
            for (int i = 0; i < numAddresses; i++)
                chunkServerAddresses.add(WireformatUtils.deserializeString(dataInputStream));

            byteArrayInputStream.close();
            dataInputStream.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "StoreChunkResponse{" +
            "messageHeader=" + messageHeader +
            ", chunk=" + chunk +
            ", chunkServerAddresses=" + chunkServerAddresses +
            '}';
    }

    public String getFileName() {
        return chunk.getFileName();
    }

    public int getChunkSequence() {
        return chunk.getSequence();
    }

    public String getSourceAddress() {
        return messageHeader.getSourceAddress();
    }

    public List<String> getChunkServerAddresses() {
        return chunkServerAddresses;
    }
}
