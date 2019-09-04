package cs555.dfs.wireformats;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class ChunkCorruption implements Message {
    private MessageHeader messageHeader;
    private String fileName;
    private int sequence;
    private List<Integer> corruptSlices = new ArrayList<>();

    public ChunkCorruption(String serverAddress, String sourceAddress, String fileName, int sequence, List<Integer> corruptSlices) {
        this.messageHeader = new MessageHeader(getProtocol(), serverAddress, sourceAddress);
        this.fileName = fileName;
        this.sequence = sequence;
        this.corruptSlices = corruptSlices;
    }

    public ChunkCorruption(byte[] bytes) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

            this.messageHeader = MessageHeader.deserialize(dataInputStream);
            sequence = WireformatUtils.deserializeInt(dataInputStream);
            fileName = WireformatUtils.deserializeString(dataInputStream);
            int numCorruptSlices = WireformatUtils.deserializeInt(dataInputStream);
            for (int i = 0; i < numCorruptSlices; i++) {
                int corruptSlice = WireformatUtils.deserializeInt(dataInputStream);
                corruptSlices.add(corruptSlice);
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
        return Protocol.CHUNK_CORRUPTION;
    }

    @Override
    public byte[] getBytes() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

            messageHeader.serialize(dataOutputStream);
            WireformatUtils.serializeInt(dataOutputStream, sequence);
            WireformatUtils.serializeString(dataOutputStream, fileName);
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

    @Override
    public String toString() {
        return "ChunkCorruption{" +
            "messageHeader=" + messageHeader +
            ", fileName='" + fileName + '\'' +
            ", sequence=" + sequence +
            ", corruptSlices=" + corruptSlices +
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
