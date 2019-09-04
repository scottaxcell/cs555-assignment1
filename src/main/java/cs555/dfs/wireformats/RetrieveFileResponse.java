package cs555.dfs.wireformats;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class RetrieveFileResponse implements Message {
    private MessageHeader messageHeader;
    private String fileName;
    private List<ChunkLocation> chunkLocations = new ArrayList<>();

    public RetrieveFileResponse(String serverAddress, String sourceAddress, String fileName, List<ChunkLocation> chunkLocations) {
        this.messageHeader = new MessageHeader(getProtocol(), serverAddress, sourceAddress);
        this.fileName = fileName;
        this.chunkLocations = chunkLocations;
    }

    public RetrieveFileResponse(byte[] bytes) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

            this.messageHeader = MessageHeader.deserialize(dataInputStream);
            fileName = WireformatUtils.deserializeString(dataInputStream);
            int numWireChunks = WireformatUtils.deserializeInt(dataInputStream);
            for (int i = 0; i < numWireChunks; i++) {
                ChunkLocation chunkLocation = ChunkLocation.deserialize(dataInputStream);
                chunkLocations.add(chunkLocation);
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
        return Protocol.RETRIEVE_FILE_RESPONSE;
    }

    @Override
    public byte[] getBytes() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

            messageHeader.serialize(dataOutputStream);
            WireformatUtils.serializeString(dataOutputStream, fileName);
            WireformatUtils.serializeInt(dataOutputStream, chunkLocations.size());
            for (ChunkLocation chunkLocation : chunkLocations)
                chunkLocation.serialize(dataOutputStream);

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
        return "RetrieveFileResponse{" +
            "messageHeader=" + messageHeader +
            ", fileName='" + fileName + '\'' +
            ", chunkLocations=" + chunkLocations +
            '}';
    }

    public String getFileName() {
        return fileName;
    }

    public List<ChunkLocation> getChunkLocations() {
        return chunkLocations;
    }
}
