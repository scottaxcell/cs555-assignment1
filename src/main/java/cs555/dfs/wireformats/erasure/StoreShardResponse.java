package cs555.dfs.wireformats.erasure;

import cs555.dfs.wireformats.Message;
import cs555.dfs.wireformats.MessageHeader;
import cs555.dfs.wireformats.Protocol;
import cs555.dfs.wireformats.WireformatUtils;

import java.io.*;

public class StoreShardResponse implements Message {
    private MessageHeader messageHeader;
    private Shard shard;
    private String shardServerAddress;

    public StoreShardResponse(String serverAddress, String sourceAddress, Shard shard, String shardServerAddress) {
        this.messageHeader = new MessageHeader(getProtocol(), serverAddress, sourceAddress);
        this.shard = shard;
        this.shardServerAddress = shardServerAddress;
    }

    @Override
    public int getProtocol() {
        return Protocol.STORE_SHARD_RESPONSE;
    }

    @Override
    public byte[] getBytes() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

            messageHeader.serialize(dataOutputStream);
            shard.serialize(dataOutputStream);
            WireformatUtils.serializeString(dataOutputStream, shardServerAddress);

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
        return "StoreShardResponse{" +
            "messageHeader=" + messageHeader +
            ", shard=" + shard +
            ", shardServerAddress='" + shardServerAddress + '\'' +
            '}';
    }

    public StoreShardResponse(byte[] bytes) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

            messageHeader = MessageHeader.deserialize(dataInputStream);
            shard = Shard.deserialize(dataInputStream);
            shardServerAddress = WireformatUtils.deserializeString(dataInputStream);

            byteArrayInputStream.close();
            dataInputStream.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getFileName() {
        return shard.getFileName();
    }

    public int getChunkSequence() {
        return shard.getSequence();
    }

    public int getFragment() {
        return shard.getFragment();
    }

    public String getSourceAddress() {
        return messageHeader.getSourceAddress();
    }

    public String getShardServerAddress() {
        return shardServerAddress;
    }
}
