package cs555.dfs.wireformats.erasure;

import cs555.dfs.wireformats.Message;
import cs555.dfs.wireformats.MessageHeader;
import cs555.dfs.wireformats.Protocol;
import cs555.dfs.wireformats.WireformatUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class RetrieveFileResponseErasure implements Message {
    private MessageHeader messageHeader;
    private String fileName;
    private List<ShardLocation> shardLocations = new ArrayList<>();

    public RetrieveFileResponseErasure(String serverAddress, String sourceAddress, String fileName, List<ShardLocation> shardLocations) {
        this.messageHeader = new MessageHeader(getProtocol(), serverAddress, sourceAddress);
        this.fileName = fileName;
        this.shardLocations = shardLocations;
    }

    @Override
    public int getProtocol() {
        return Protocol.RETRIEVE_FILE_RESPONSE_ERASURE;
    }

    @Override
    public byte[] getBytes() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

            messageHeader.serialize(dataOutputStream);
            WireformatUtils.serializeString(dataOutputStream, fileName);
            WireformatUtils.serializeInt(dataOutputStream, shardLocations.size());
            for (ShardLocation shardLocation : shardLocations)
                shardLocation.serialize(dataOutputStream);

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

    public RetrieveFileResponseErasure(byte[] bytes) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

            this.messageHeader = MessageHeader.deserialize(dataInputStream);
            fileName = WireformatUtils.deserializeString(dataInputStream);
            int numShards = WireformatUtils.deserializeInt(dataInputStream);
            for (int i = 0; i < numShards; i++) {
                ShardLocation shardLocation = ShardLocation.deserialize(dataInputStream);
                shardLocations.add(shardLocation);
            }

            byteArrayInputStream.close();
            dataInputStream.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "RetrieveFileResponseErasure{" +
            "messageHeader=" + messageHeader +
            ", fileName='" + fileName + '\'' +
            ", shardLocations=" + shardLocations +
            '}';
    }

    public String getFileName() {
        return fileName;
    }

    public List<ShardLocation> getShardLocations() {
        return shardLocations;
    }
}
