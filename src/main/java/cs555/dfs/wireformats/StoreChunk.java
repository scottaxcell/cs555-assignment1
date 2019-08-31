package cs555.dfs.wireformats;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class StoreChunk implements Message {
    private MessageHeader messageHeader;
    private String fileName;
    private int chunkSequence;
    private byte[] fileData;
    private List<String> nextServers = new ArrayList<>();

    public StoreChunk(String serverAddress, String sourceAddress, String fileName, int chunkSequence, byte[] fileData, List<String> nextServers) {
        this.messageHeader = new MessageHeader(getProtocol(), serverAddress, sourceAddress);
        this.fileName = fileName;
        this.chunkSequence = chunkSequence;
        this.fileData = fileData;
        this.nextServers = nextServers;
    }

    public StoreChunk(byte[] bytes) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

        this.messageHeader = MessageHeader.deserialize(dataInputStream);

        chunkSequence = dataInputStream.readInt();

        int fileNameLength = dataInputStream.readInt();
        byte[] fileNameBytes = new byte[fileNameLength];
        dataInputStream.readFully(fileNameBytes, 0, fileNameLength);
        fileName = new String(fileNameBytes);

        int bytesLength = dataInputStream.readInt();
        fileData = new byte[bytesLength];
        dataInputStream.readFully(fileData, 0, bytesLength);

        int numServers = dataInputStream.readInt();
        for (int i = 0; i < numServers; i++) {
            int serverLength = dataInputStream.readInt();
            byte[] serverBytes = new byte[serverLength];
            dataInputStream.readFully(serverBytes, 0, serverLength);
            String server = new String(serverBytes);
            nextServers.add(server);
        }

        byteArrayInputStream.close();
        dataInputStream.close();
    }

    @Override
    public int getProtocol() {
        return Protocol.STORE_CHUNK;
    }

    @Override
    public byte[] getBytes() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

        dataOutputStream.write(messageHeader.getBytes());

        dataOutputStream.writeInt(chunkSequence);

        dataOutputStream.writeInt(fileName.length());
        dataOutputStream.write(fileName.getBytes());

        dataOutputStream.writeInt(fileData.length);
        dataOutputStream.write(fileData);

        dataOutputStream.writeInt(nextServers.size());
        for (String server : nextServers) {
            dataOutputStream.writeInt(server.length());
            dataOutputStream.write(server.getBytes());
        }

        dataOutputStream.flush();

        byte[] data = byteArrayOutputStream.toByteArray();

        byteArrayOutputStream.close();
        dataOutputStream.close();

        return data;
    }

    @Override
    public String toString() {
        return "StoreChunk{" +
            "messageHeader=" + messageHeader +
            ", fileName='" + fileName + '\'' +
            ", chunkSequence=" + chunkSequence +
            ", fileData.length=" + fileData.length +
            ", nextServers=" + nextServers +
            '}';
    }

    public String getFileName() {
        return fileName;
    }

    public int getChunkSequence() {
        return chunkSequence;
    }

    public byte[] getFileData() {
        return fileData;
    }

    public List<String> getNextServers() {
        return nextServers;
    }
}
