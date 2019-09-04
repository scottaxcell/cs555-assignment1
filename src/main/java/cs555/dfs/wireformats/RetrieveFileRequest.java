package cs555.dfs.wireformats;

import java.io.*;

public class RetrieveFileRequest implements Message {
    private MessageHeader messageHeader;
    private String fileName;

    public RetrieveFileRequest(String serverAddress, String sourceAddress, String fileName) {
        this.messageHeader = new MessageHeader(getProtocol(), serverAddress, sourceAddress);
        this.fileName = fileName;
    }

    @Override
    public int getProtocol() {
        return Protocol.RETRIEVE_FILE_REQUEST;
    }

    @Override
    public byte[] getBytes() {
        try {
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
        catch (IOException e) {
            e.printStackTrace();
            return new byte[0];
        }
    }

    public RetrieveFileRequest(byte[] bytes) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

            this.messageHeader = MessageHeader.deserialize(dataInputStream);
            fileName = WireformatUtils.deserializeString(dataInputStream);

            byteArrayInputStream.close();
            dataInputStream.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
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
