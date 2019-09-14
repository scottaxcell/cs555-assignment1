package cs555.dfs.wireformats;

import java.io.*;
import java.util.List;

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
        return Protocol.STORE_CHUNK_RESPONSE;
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


    public String getSourceAddress() {
        return messageHeader.getSourceAddress();
    }

    public List<String> getChunkServerAddresses() {
        return shardServerAddresses;
    }
}
