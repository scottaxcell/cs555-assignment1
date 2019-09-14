package cs555.dfs.wireformats;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class ShardHeartbeat implements Message {
    private MessageHeader messageHeader;
    private List<cs555.dfs.node.Shard> shards = new ArrayList<>();

    public ShardHeartbeat(String serverAddress, String sourceAddress, List<cs555.dfs.node.Shard> shards) {
        this.messageHeader = new MessageHeader(getProtocol(), serverAddress, sourceAddress);
        this.shards = shards;
    }

    @Override
    public int getProtocol() {
        return Protocol.SHARD_HEARTBEAT;
    }

    @Override
    public byte[] getBytes() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

            messageHeader.serialize(dataOutputStream);
            WireformatUtils.serializeInt(dataOutputStream, shards.size());
            for (cs555.dfs.node.Shard shard : shards) {
                WireformatUtils.serializeString(dataOutputStream, shard.getFileName());
                WireformatUtils.serializeInt(dataOutputStream, shard.getSequence());
                WireformatUtils.serializeInt(dataOutputStream, shard.getFragment());
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

    public ShardHeartbeat(byte[] bytes) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

            this.messageHeader = MessageHeader.deserialize(dataInputStream);

            int numShards = WireformatUtils.deserializeInt(dataInputStream);
            for (int i = 0; i < numShards; i++) {
                String fileName = WireformatUtils.deserializeString(dataInputStream);
                int sequence = WireformatUtils.deserializeInt(dataInputStream);
                int fragment = WireformatUtils.deserializeInt(dataInputStream);
                shards.add(new cs555.dfs.node.Shard(fileName, sequence, fragment));
            }

            byteArrayInputStream.close();
            dataInputStream.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getServerAddress() {
        return messageHeader.getServerAddress();
    }

    public List<cs555.dfs.node.Shard> getShards() {
        return shards;
    }
}
