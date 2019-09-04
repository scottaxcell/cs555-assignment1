package cs555.dfs.wireformats;

import java.io.*;

public class RetrieveChunkRequest implements Message {
    private MessageHeader messageHeader;
    private String fileName;
    private int sequence;

    public RetrieveChunkRequest(String serverAddress, String sourceAddress, String fileName, int sequence) {
        this.messageHeader = new MessageHeader(getProtocol(), serverAddress, sourceAddress);
        this.fileName = fileName;
        this.sequence = sequence;
    }

    public RetrieveChunkRequest(byte[] bytes) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

            this.messageHeader = MessageHeader.deserialize(dataInputStream);
            sequence = WireformatUtils.deserializeInt(dataInputStream);
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
        return Protocol.RETRIEVE_CHUNK_REQUEST;
    }

    @Override
    public byte[] getBytes() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

            messageHeader.serialize(dataOutputStream);
            WireformatUtils.serializeInt(dataOutputStream, sequence);
            WireformatUtils.serializeString(dataOutputStream, fileName);

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
        return "RetrieveChunkRequest{" +
            "messageHeader=" + messageHeader +
            ", fileName='" + fileName + '\'' +
            ", sequence=" + sequence +
            '}';
    }

    public String getFileName() {
        return fileName;
    }

    public int getSequence() {
        return sequence;
    }

    public String getSourceAddess() {
        return messageHeader.getSourceAddress();
    }

    public String getServerAddress() {
        return messageHeader.getServerAddress();
    }
}
