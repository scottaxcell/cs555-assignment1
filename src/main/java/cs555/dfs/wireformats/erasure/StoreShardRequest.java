package cs555.dfs.wireformats.erasure;

import cs555.dfs.wireformats.Message;
import cs555.dfs.wireformats.MessageHeader;
import cs555.dfs.wireformats.Protocol;

import java.io.*;

public class StoreShardRequest implements Message {
    private MessageHeader messageHeader;
    private Shard shard;

    public StoreShardRequest(String serverAddress, String sourceAddress, Shard shard) {
        this.messageHeader = new MessageHeader(getProtocol(), serverAddress, sourceAddress);
        this.shard = shard;
    }

    @Override
    public int getProtocol() {
        return Protocol.STORE_SHARD_REQUEST;
    }

    @Override
    public byte[] getBytes() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

            messageHeader.serialize(dataOutputStream);
            shard.serialize(dataOutputStream);

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

    public StoreShardRequest(byte[] bytes) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

            messageHeader = MessageHeader.deserialize(dataInputStream);
            shard = Shard.deserialize(dataInputStream);

            byteArrayInputStream.close();
            dataInputStream.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "StoreShardRequest{" +
            "messageHeader=" + messageHeader +
            ", shard=" + shard +
            '}';
    }

    public String getFileName() {
        return shard.getFileName();
    }

    public int getSequence() {
        return shard.getSequence();
    }

    public int getFragment() {
        return shard.getFragment();
    }

    public String getSourceAddress() {
        return messageHeader.getSourceAddress();
    }
}
