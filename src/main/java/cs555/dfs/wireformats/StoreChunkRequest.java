package cs555.dfs.wireformats;

import java.io.*;

public class StoreChunkRequest implements Message {
    private MessageHeader messageHeader;
    private String fileName;
    private int chunkSequence;

    public StoreChunkRequest(String serverAddress, String sourceAddress, String fileName, int chunkSequence) {
        this.messageHeader = new MessageHeader(getProtocol(), serverAddress, sourceAddress);
        this.fileName = fileName;
        this.chunkSequence = chunkSequence;
    }

    public StoreChunkRequest(byte[] bytes) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

            this.messageHeader = MessageHeader.deserialize(dataInputStream);
            chunkSequence = WireformatUtils.deserializeInt(dataInputStream);
            fileName = WireformatUtils.deserializeString(dataInputStream);

            byteArrayInputStream.close();
            dataInputStream.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int getProtocol() {
        return Protocol.STORE_CHUNK_REQUEST;
    }

    @Override
    public byte[] getBytes() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

            dataOutputStream.write(messageHeader.getBytes());

            dataOutputStream.writeInt(chunkSequence);

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

    @Override
    public String toString() {
        return "StoreChunkRequest{" +
            "messageHeader=" + messageHeader +
            ", fileName='" + fileName + '\'' +
            ", chunkSequence=" + chunkSequence +
            '}';
    }

    public String getFileName() {
        return fileName;
    }

    public int getChunkSequence() {
        return chunkSequence;
    }

    public String getSourceAddress() {
        return messageHeader.getSourceAddress();
    }
}
