package cs555.dfs.wireformats;

import java.io.*;

public class StoreChunk implements Message {
    private MessageHeader messageHeader;
    private String fileName;
    private int chunkSequence;
    private byte[] fileData;
    // todo -- add support for additional chunk servers

    public StoreChunk(String serverAddress, String sourceAddress, String fileName, int chunkSequence, byte[] fileData) {
        this.messageHeader = new MessageHeader(getProtocol(), serverAddress, sourceAddress);
        this.fileName = fileName;
        this.chunkSequence = chunkSequence;
        this.fileData = fileData;
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

        byteArrayInputStream.close();
        dataInputStream.close();
    }

    @Override
    public int getProtocol() {
        return Protocol.STORE_CHUNK_REQUEST;
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

        dataOutputStream.flush();

        byte[] data = byteArrayOutputStream.toByteArray();

        byteArrayOutputStream.close();
        dataOutputStream.close();

        return data;
    }

    @Override
    public String toString() {
        return "StoreChunkRequest{" +
            "messageHeader=" + messageHeader +
            ", fileName='" + fileName + '\'' +
            ", chunkSequence=" + chunkSequence +
            ", fileData.length=" + fileData.length +
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
}
