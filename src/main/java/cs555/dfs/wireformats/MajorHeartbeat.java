package cs555.dfs.wireformats;

import cs555.dfs.node.Chunk;

import java.io.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class MajorHeartbeat implements Message {
    private MessageHeader messageHeader;
    private long usableSpace;
    private int totalNumberOfChunks;
    private List<Chunk> chunks = new ArrayList<>();

    public MajorHeartbeat(String serverAddress, String sourceAddress, long usableSpace, int totalNumberOfChunks, List<Chunk> chunks) {
        this.messageHeader = new MessageHeader(getProtocol(), serverAddress, sourceAddress);
        this.usableSpace = usableSpace;
        this.totalNumberOfChunks = totalNumberOfChunks;
        this.chunks = chunks;
    }

    public MajorHeartbeat(byte[] bytes) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

            this.messageHeader = MessageHeader.deserialize(dataInputStream);

            usableSpace = WireformatUtils.deserializeLong(dataInputStream);
            totalNumberOfChunks = WireformatUtils.deserializeInt(dataInputStream);

            int numNewChunks = WireformatUtils.deserializeInt(dataInputStream);
            // todo -- cleanup chunk deserialization
            for (int i = 0; i < numNewChunks; i++) {
                String fileName = WireformatUtils.deserializeString(dataInputStream);
                int version = dataInputStream.readInt();
                int sequence = dataInputStream.readInt();
                long timeStampEpochSecond = dataInputStream.readLong();
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
    public int getProtocol() {
        return Protocol.MAJOR_HEART_BEAT;
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
            // todo -- cleanup chunk serialization
            for (Chunk chunk : chunks) {
                dataOutputStream.writeInt(chunk.getFileName().length());
                dataOutputStream.write(chunk.getFileName().getBytes());

                dataOutputStream.writeInt(chunk.getVersion());

                dataOutputStream.writeInt(chunk.getSequence());

                dataOutputStream.writeLong(chunk.getTimeStamp().getEpochSecond());
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

    @Override
    public String toString() {
        return "MajorHeartbeat{" +
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
