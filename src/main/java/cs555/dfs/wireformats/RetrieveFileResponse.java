package cs555.dfs.wireformats;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class RetrieveFileResponse implements Message {
    private MessageHeader messageHeader;
    private String fileName;
    private List<WireChunk> wireChunks = new ArrayList<>();

    public RetrieveFileResponse(String serverAddress, String sourceAddress, String fileName, List<WireChunk> wireChunks) {
        this.messageHeader = new MessageHeader(getProtocol(), serverAddress, sourceAddress);
        this.fileName = fileName;
        this.wireChunks = wireChunks;
    }

    public RetrieveFileResponse(byte[] bytes) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

        this.messageHeader = MessageHeader.deserialize(dataInputStream);

        int fileNameLength = dataInputStream.readInt();
        byte[] fileNameBytes = new byte[fileNameLength];
        dataInputStream.readFully(fileNameBytes, 0, fileNameLength);
        fileName = new String(fileNameBytes);

        int numWireChunks = dataInputStream.readInt();
        for (int i = 0; i < numWireChunks; i++) {
            WireChunk wireChunk = WireChunk.deserialize(dataInputStream);
            wireChunks.add(wireChunk);
        }

        byteArrayInputStream.close();
        dataInputStream.close();
    }

    @Override
    public int getProtocol() {
        return Protocol.RETRIEVE_FILE_RESPONSE;
    }

    @Override
    public byte[] getBytes() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

        dataOutputStream.write(messageHeader.getBytes());

        dataOutputStream.writeInt(fileName.length());
        dataOutputStream.write(fileName.getBytes());

        dataOutputStream.writeInt(wireChunks.size());
        for (WireChunk wireChunk : wireChunks) {
            wireChunk.serialize(dataOutputStream);
        }

        dataOutputStream.flush();

        byte[] data = byteArrayOutputStream.toByteArray();

        byteArrayOutputStream.close();
        dataOutputStream.close();

        return data;
    }

    @Override
    public String toString() {
        return "RetrieveFileResponse{" +
            "messageHeader=" + messageHeader +
            ", fileName='" + fileName + '\'' +
            ", wireChunks=" + wireChunks +
            '}';
    }

    public String getFileName() {
        return fileName;
    }

    public static class WireChunk {
        public final String fileName;
        public final int sequence;
        public final String serverAddress;

        public WireChunk(String fileName, int sequence, String serverAddress) {
            this.fileName = fileName;
            this.sequence = sequence;
            this.serverAddress = serverAddress;
        }

        public static WireChunk deserialize(DataInputStream dataInputStream) throws IOException {
            int fileNameLength = dataInputStream.readInt();
            byte[] fileNameBytes = new byte[fileNameLength];
            dataInputStream.readFully(fileNameBytes);
            String fileName = new String(fileNameBytes);

            int sequence = dataInputStream.readInt();

            int serverLength = dataInputStream.readInt();
            byte[] serverBytes = new byte[serverLength];
            dataInputStream.readFully(serverBytes);
            String server = new String(serverBytes);

            return new WireChunk(fileName, sequence, server);
        }

        public void serialize(DataOutputStream dataOutputStream) throws IOException {
            dataOutputStream.writeInt(fileName.length());
            dataOutputStream.write(fileName.getBytes());
            dataOutputStream.writeInt(sequence);
            dataOutputStream.writeInt(serverAddress.length());
            dataOutputStream.write(serverAddress.getBytes());
        }

        @Override
        public String toString() {
            return "WireChunk{" +
                "fileName='" + fileName + '\'' +
                ", sequence=" + sequence +
                ", serverAddress='" + serverAddress + '\'' +
                '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WireChunk wireChunk = (WireChunk) o;
            return sequence == wireChunk.sequence &&
                fileName.equals(wireChunk.fileName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fileName, sequence);
        }
    }
}
