package cs555.dfs.wireformats;

import cs555.dfs.transport.TcpConnection;

import java.io.*;

public class StoreChunkRequest implements Message {
    private MessageHeader messageHeader;
    private String fileName;
    private int chunkSequence;
    private byte[] fileData;
    // todo -- add support for additional chunk servers

    public StoreChunkRequest(TcpConnection tcpConnection, String fileName, int chunkSequence, byte[] fileData) {
        this.messageHeader = new MessageHeader(getProtocol(), tcpConnection);
        this.fileName = fileName;
        this.chunkSequence = chunkSequence;
        this.fileData = fileData;
    }

    public StoreChunkRequest(byte[] bytes) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        DataInputStream fileDataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

        this.messageHeader = MessageHeader.deserialize(fileDataInputStream);

        chunkSequence = fileDataInputStream.readInt();

        int fileNameLength = fileDataInputStream.readInt();
        byte[] fileNameBytes = new byte[fileNameLength];
        fileDataInputStream.readFully(fileNameBytes, 0, fileNameLength);
        fileName = new String(fileNameBytes);

        int bytesLength = fileDataInputStream.readInt();
        fileData = new byte[bytesLength];
        fileDataInputStream.readFully(fileData, 0, bytesLength);

        byteArrayInputStream.close();
        fileDataInputStream.close();
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
