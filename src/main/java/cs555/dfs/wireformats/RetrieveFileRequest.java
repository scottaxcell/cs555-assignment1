package cs555.dfs.wireformats;

import java.io.*;

public class RetrieveFileRequest implements Message {
    private MessageHeader messageHeader;
    private String fileName;

    public RetrieveFileRequest(String serverAddress, String sourceAddress, String fileName) {
        this.messageHeader = new MessageHeader(getProtocol(), serverAddress, sourceAddress);
        this.fileName = fileName;
    }

    public RetrieveFileRequest(byte[] bytes) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

        this.messageHeader = MessageHeader.deserialize(dataInputStream);

        int fileNameLength = dataInputStream.readInt();
        byte[] fileNameBytes = new byte[fileNameLength];
        dataInputStream.readFully(fileNameBytes, 0, fileNameLength);
        fileName = new String(fileNameBytes);

        byteArrayInputStream.close();
        dataInputStream.close();
    }

    @Override
    public int getProtocol() {
        return Protocol.RETRIEVE_FILE_REQUEST;
    }

    @Override
    public byte[] getBytes() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

        dataOutputStream.write(messageHeader.getBytes());

        dataOutputStream.writeInt(fileName.length());
        dataOutputStream.write(fileName.getBytes());

        dataOutputStream.flush();

        byte[] data = byteArrayOutputStream.toByteArray();

        byteArrayOutputStream.close();
        dataOutputStream.close();

        return data;
    }

    @Override
    public String toString() {
        return "RetrieveFileRequest{" +
            "messageHeader=" + messageHeader +
            ", fileName='" + fileName + '\'' +
            '}';
    }

    public String getFileName() {
        return fileName;
    }

    public String getSourceAddress() {
        return messageHeader.getSourceAddress();
    }
}
