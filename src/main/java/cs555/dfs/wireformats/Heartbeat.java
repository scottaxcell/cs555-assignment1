package cs555.dfs.wireformats;

import cs555.dfs.node.Chunk;

import java.io.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class Heartbeat implements Message {
    private MessageHeader messageHeader;
    private long usableSpace;
    private int totalNumberOfChunks;
    private List<Chunk> chunks = new ArrayList<>();

    public Heartbeat(int protocol, String serverAddress, String sourceAddress, long usableSpace, int totalNumberOfChunks, List<Chunk> chunks) {
        this.messageHeader = new MessageHeader(protocol, serverAddress, sourceAddress);
        this.usableSpace = usableSpace;
        this.totalNumberOfChunks = totalNumberOfChunks;
        this.chunks = chunks;
    }

    @Override
    public int getProtocol() {
        return messageHeader.getProtocol();
    }

    @Override
    public byte[] getBytes() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

            messageHeader.serialize(dataOutputStream);
            WireformatUtils.serializeLong(dataOutputStream, usableSpace);
            WireformatUtils.serializeInt(dataOutputStream, totalNumberOfChunks);
            WireformatUtils.serializeInt(dataOutputStream, chunks.size());
            for (Chunk chunk : chunks) {
                WireformatUtils.serializeString(dataOutputStream, chunk.getFileName());
                WireformatUtils.serializeInt(dataOutputStream, chunk.getVersion());
                WireformatUtils.serializeInt(dataOutputStream, chunk.getSequence());
                WireformatUtils.serializeLong(dataOutputStream, chunk.getTimeStamp().getEpochSecond());
            }

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

    public Heartbeat(byte[] bytes) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

            this.messageHeader = MessageHeader.deserialize(dataInputStream);

            usableSpace = WireformatUtils.deserializeLong(dataInputStream);
            totalNumberOfChunks = WireformatUtils.deserializeInt(dataInputStream);

            int numChunks = WireformatUtils.deserializeInt(dataInputStream);
            for (int i = 0; i < numChunks; i++) {
                String fileName = WireformatUtils.deserializeString(dataInputStream);
                int version = WireformatUtils.deserializeInt(dataInputStream);
                int sequence = WireformatUtils.deserializeInt(dataInputStream);
                long timeStampEpochSecond = WireformatUtils.deserializeLong(dataInputStream);
                chunks.add(new Chunk(fileName, version, sequence, Instant.ofEpochSecond(timeStampEpochSecond)));
            }

            byteArrayInputStream.close();
            dataInputStream.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "Heartbeat{" +
            "messageHeader=" + messageHeader +
            ", usableSpace=" + usableSpace +
            ", totalNumberOfChunks=" + totalNumberOfChunks +
            ", chunks=" + chunks +
            '}';
    }

    public String getServerAddress() {
        return messageHeader.getServerAddress();
    }

    public long getUsableSpace() {
        return usableSpace;
    }

    public int getNumberOfChunks() {
        return totalNumberOfChunks;
    }

    public List<Chunk> getChunks() {
        return chunks;
    }
}
