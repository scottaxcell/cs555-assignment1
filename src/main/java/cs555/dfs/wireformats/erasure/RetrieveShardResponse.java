package cs555.dfs.wireformats.erasure;

import cs555.dfs.wireformats.Message;
import cs555.dfs.wireformats.MessageHeader;
import cs555.dfs.wireformats.Protocol;
import cs555.dfs.wireformats.WireformatUtils;

import java.io.*;

public class RetrieveShardResponse implements Message {
    private MessageHeader messageHeader;
    private Shard shard;
    private byte[] fileData;

    public RetrieveShardResponse(String serverAddress, String sourceAddress, Shard Shard, byte[] fileData) {
        this.messageHeader = new MessageHeader(getProtocol(), serverAddress, sourceAddress);
        this.shard = Shard;
        this.fileData = fileData;
    }

    @Override
    public int getProtocol() {
        return Protocol.RETRIEVE_SHARD_RESPONSE;
    }

    @Override
    public byte[] getBytes() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

            messageHeader.serialize(dataOutputStream);
            shard.serialize(dataOutputStream);
            WireformatUtils.serializeBytes(dataOutputStream, fileData);

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

    public RetrieveShardResponse(byte[] bytes) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

            messageHeader = MessageHeader.deserialize(dataInputStream);
            shard = shard.deserialize(dataInputStream);
            fileData = WireformatUtils.deserializeBytes(dataInputStream);

            byteArrayInputStream.close();
            dataInputStream.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "RetrieveShardResponse{" +
            "messageHeader=" + messageHeader +
            ", shard=" + shard +
            ", data.length=" + fileData.length +
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

    public byte[] getFileData() {
        return fileData;
    }
}
