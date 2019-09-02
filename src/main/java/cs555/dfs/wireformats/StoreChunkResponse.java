package cs555.dfs.wireformats;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class StoreChunkResponse implements Message {
    private MessageHeader messageHeader;
    private String fileName;
    private int chunkSequence;
    private List<String> chunkServerAddresses = new ArrayList<>();

    public StoreChunkResponse(String serverAddress, String sourceAddress, String fileName, int chunkSequence, List<String> chunkServerAddresses) {
        this.messageHeader = new MessageHeader(getProtocol(), serverAddress, sourceAddress);
        this.fileName = fileName;
        this.chunkSequence = chunkSequence;
        this.chunkServerAddresses = chunkServerAddresses;
    }

    public StoreChunkResponse(byte[] bytes) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

            this.messageHeader = MessageHeader.deserialize(dataInputStream);

            chunkSequence = dataInputStream.readInt();
            fileName = WireformatUtils.deserializeString(dataInputStream);
            int numAddresses = WireformatUtils.deserializeInt(dataInputStream);
            for (int i = 0; i < numAddresses; i++) {
                String address = WireformatUtils.deserializeString(dataInputStream);
                chunkServerAddresses.add(address);
            }

            byteArrayInputStream.close();
            dataInputStream.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int getProtocol() {
        return Protocol.STORE_CHUNK_RESPONSE;
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

            int numAddresses = chunkServerAddresses.size();
            dataOutputStream.writeInt(numAddresses);
            for (String chunkServerAddress : chunkServerAddresses) {
                dataOutputStream.writeInt(chunkServerAddress.length());
                dataOutputStream.write(chunkServerAddress.getBytes());
            }

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
        return "StoreChunkResponse{" +
            "messageHeader=" + messageHeader +
            ", fileName='" + fileName + '\'' +
            ", chunkSequence=" + chunkSequence +
            ", chunkServerAddresses=" + chunkServerAddresses +
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

    public List<String> getChunkServerAddresses() {
        return chunkServerAddresses;
    }
}
