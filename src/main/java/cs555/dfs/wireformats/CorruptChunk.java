package cs555.dfs.wireformats;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class CorruptChunk implements Message {
    private MessageHeader messageHeader;
    private Chunk chunk;
    private List<Integer> corruptSlices = new ArrayList<>();

    public CorruptChunk(String serverAddress, String sourceAddress, Chunk chunk, List<Integer> corruptSlices) {
        this.messageHeader = new MessageHeader(getProtocol(), serverAddress, sourceAddress);
        this.chunk = chunk;
        this.corruptSlices = corruptSlices;
    }

    @Override
    public int getProtocol() {
        return Protocol.CORRUPT_CHUNK;
    }

    @Override
    public byte[] getBytes() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

            messageHeader.serialize(dataOutputStream);
            chunk.serialize(dataOutputStream);
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

    public CorruptChunk(byte[] bytes) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

            messageHeader = MessageHeader.deserialize(dataInputStream);
            chunk = Chunk.deserialize(dataInputStream);
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
        return "CorruptChunk{" +
            "messageHeader=" + messageHeader +
            ", chunk=" + chunk +
            ", corruptSlices=" + corruptSlices +
            '}';
    }

    public String getFileName() {
        return chunk.getFileName();
    }

    public int getSequence() {
        return chunk.getSequence();
    }

    public String getServerAddress() {
        return messageHeader.getServerAddress();
    }
}
