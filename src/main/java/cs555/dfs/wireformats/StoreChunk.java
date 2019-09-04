package cs555.dfs.wireformats;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class StoreChunk implements Message {
    private MessageHeader messageHeader;
    private Chunk chunk;
    private byte[] fileData;
    private List<String> nextServers = new ArrayList<>();

    public StoreChunk(String serverAddress, String sourceAddress, Chunk chunk, byte[] fileData, List<String> nextServers) {
        this.messageHeader = new MessageHeader(getProtocol(), serverAddress, sourceAddress);
        this.chunk = chunk;
        this.fileData = fileData;
        this.nextServers = nextServers;
    }

    public StoreChunk(byte[] bytes) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

            messageHeader = MessageHeader.deserialize(dataInputStream);
            chunk = Chunk.deserialize(dataInputStream);

            fileData = WireformatUtils.deserializeBytes(dataInputStream);
            int numServers = WireformatUtils.deserializeInt(dataInputStream);
            for (int i = 0; i < numServers; i++) {
                String server = WireformatUtils.deserializeString(dataInputStream);
                nextServers.add(server);
            }

            byteArrayInputStream.close();
            dataInputStream.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int getProtocol() {
        return Protocol.STORE_CHUNK;
    }

    @Override
    public byte[] getBytes() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

            messageHeader.serialize(dataOutputStream);
            chunk.serialize(dataOutputStream);
            WireformatUtils.serializeBytes(dataOutputStream, fileData);
            WireformatUtils.serializeInt(dataOutputStream, nextServers.size());
            for (String server : nextServers)
                WireformatUtils.serializeString(dataOutputStream, server);

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

    @Override
    public String toString() {
        return "StoreChunk{" +
            "messageHeader=" + messageHeader +
            ", chunk=" + chunk +
            ", fileData.length=" + fileData.length +
            ", nextServers=" + nextServers +
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

    public List<String> getNextServers() {
        return nextServers;
    }
}
