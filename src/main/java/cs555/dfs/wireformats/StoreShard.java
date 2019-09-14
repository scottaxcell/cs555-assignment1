package cs555.dfs.wireformats;

import java.io.*;

public class StoreShard implements Message {
    private MessageHeader messageHeader;
    private Shard shard;
    private byte[] fileData;

    public StoreShard(String serverAddress, String sourceAddress, Shard shard, byte[] fileData) {
        this.messageHeader = new MessageHeader(getProtocol(), serverAddress, sourceAddress);
        this.shard = shard;
        this.fileData = fileData;
    }

    @Override
    public int getProtocol() {
        return Protocol.STORE_SHARD;
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

    public StoreShard(byte[] bytes) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

            messageHeader = MessageHeader.deserialize(dataInputStream);
            shard = Shard.deserialize(dataInputStream);
            fileData = WireformatUtils.deserializeBytes(dataInputStream);

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
