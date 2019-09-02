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

    public MajorHeartbeat(byte[] bytes) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

        this.messageHeader = MessageHeader.deserialize(dataInputStream);

        usableSpace = dataInputStream.readLong();
        totalNumberOfChunks = dataInputStream.readInt();

        int numNewChunks = dataInputStream.readInt();
        for (int i = 0; i < numNewChunks; i++) {
            int fileNameLength = dataInputStream.readInt();
            byte[] fileNameBytes = new byte[fileNameLength];
            dataInputStream.readFully(fileNameBytes, 0, fileNameLength);
            String fileName = new String(fileNameBytes);

            int version = dataInputStream.readInt();

            int sequence = dataInputStream.readInt();

            long timeStampEpochSecond = dataInputStream.readLong();

            chunks.add(new Chunk(fileName, version, sequence, Instant.ofEpochSecond(timeStampEpochSecond)));
        }

        byteArrayInputStream.close();
        dataInputStream.close();
    }

    @Override
    public int getProtocol() {
        return Protocol.MAJOR_HEART_BEAT;
    }

    @Override
    public byte[] getBytes() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

        dataOutputStream.write(messageHeader.getBytes());

        dataOutputStream.writeLong(usableSpace);

        dataOutputStream.writeInt(totalNumberOfChunks);

        dataOutputStream.writeInt(chunks.size());
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
