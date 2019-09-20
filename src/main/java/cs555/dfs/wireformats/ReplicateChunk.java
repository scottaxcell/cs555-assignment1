package cs555.dfs.wireformats;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class ReplicateChunk implements Message {
    private MessageHeader messageHeader;
    private Chunk chunk;
    private List<Integer> corruptSlices = new ArrayList<>();
    private String destinationAddress;

    public ReplicateChunk(String serverAddress, String sourceAddress, Chunk chunk, List<Integer> corruptSlices, String destinationAddress) {
        this.messageHeader = new MessageHeader(getProtocol(), serverAddress, sourceAddress);
        this.chunk = chunk;
        this.corruptSlices = corruptSlices;
        this.destinationAddress = destinationAddress;
    }

    @Override
    public int getProtocol() {
        return Protocol.REPLICATE_CHUNK;
    }

    @Override
    public byte[] getBytes() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

            messageHeader.serialize(dataOutputStream);
            chunk.serialize(dataOutputStream);
            WireformatUtils.serializeString(dataOutputStream, destinationAddress);
            WireformatUtils.serializeInt(dataOutputStream, corruptSlices.size());
            for (Integer corruptSlice : corruptSlices)
                WireformatUtils.serializeInt(dataOutputStream, corruptSlice);

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

    public ReplicateChunk(byte[] bytes) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

            messageHeader = MessageHeader.deserialize(dataInputStream);
            chunk = Chunk.deserialize(dataInputStream);
            destinationAddress = WireformatUtils.deserializeString(dataInputStream);
            int numCorruptSlices = WireformatUtils.deserializeInt(dataInputStream);
            for (int i = 0; i < numCorruptSlices; i++)
                corruptSlices.add(WireformatUtils.deserializeInt(dataInputStream));

            byteArrayInputStream.close();
            dataInputStream.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "ReplicateChunk{" +
            "messageHeader=" + messageHeader +
            ", chunk=" + chunk +
            ", corruptSlices=" + corruptSlices +
            ", destinationAddress='" + destinationAddress + '\'' +
            '}';
    }

    public String getFileName() {
        return chunk.getFileName();
    }

    public int getSequence() {
        return chunk.getSequence();
    }

    public String getSourceAddress() {
        return messageHeader.getSourceAddress();
    }

    public String getDestinationAddress() {
        return destinationAddress;
    }

    public List<Integer> getCorruptSlices() {
        return corruptSlices;
    }
}
