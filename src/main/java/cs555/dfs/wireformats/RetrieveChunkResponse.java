package cs555.dfs.wireformats;

import java.io.*;

public class RetrieveChunkResponse implements Message {
    private MessageHeader messageHeader;
    private String fileName;
    private int sequence;
    private byte[] fileData;

    public RetrieveChunkResponse(String serverAddress, String sourceAddress, String fileName, int sequence, byte[] fileData) {
        this.messageHeader = new MessageHeader(getProtocol(), serverAddress, sourceAddress);
        this.fileName = fileName;
        this.sequence = sequence;
        this.fileData = fileData;
    }

    public RetrieveChunkResponse(byte[] bytes) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

            this.messageHeader = MessageHeader.deserialize(dataInputStream);
            sequence = WireformatUtils.deserializeInt(dataInputStream);
            fileName = WireformatUtils.deserializeString(dataInputStream);
            fileData = WireformatUtils.deserializeBytes(dataInputStream);

            byteArrayInputStream.close();
            dataInputStream.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int getProtocol() {
        return Protocol.RETRIEVE_CHUNK_RESPONSE;
    }

    @Override
    public byte[] getBytes() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

            messageHeader.serialize(dataOutputStream);
            WireformatUtils.serializeInt(dataOutputStream, sequence);
            WireformatUtils.serializeString(dataOutputStream, fileName);
            WireformatUtils.serializeBytes(dataOutputStream, fileData);

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
        return "RetrieveChunkResponse{" +
            "messageHeader=" + messageHeader +
            ", fileName='" + fileName + '\'' +
            ", sequence=" + sequence +
            ", fileData.length=" + fileData.length +
            '}';
    }

    public String getFileName() {
        return fileName;
    }

    public int getSequence() {
        return sequence;
    }

    public byte[] getFileData() {
        return fileData;
    }
}
