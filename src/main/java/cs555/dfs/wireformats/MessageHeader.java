package cs555.dfs.wireformats;

import java.io.*;

public class MessageHeader implements Message {
    private final int protocol;
    private final String serverAddress;
    private final String sourceAddress;

    public MessageHeader(int protocol, String serverAddress, String sourceAddress) {
        this.protocol = protocol;
        this.serverAddress = serverAddress;
        this.sourceAddress = sourceAddress;
    }

    public static MessageHeader deserialize(DataInputStream dataInputStream) {
        int protocol = WireformatUtils.deserializeInt(dataInputStream);
        String serverAddress = WireformatUtils.deserializeString(dataInputStream);
        String sourceAddress = WireformatUtils.deserializeString(dataInputStream);
        return new MessageHeader(protocol, serverAddress, sourceAddress);
    }

    public String getServerAddress() {
        return serverAddress;
    }

    public String getSourceAddress() {
        return sourceAddress;
    }

    public void serialize(DataOutputStream dataOutputStream) {
        WireformatUtils.serializeInt(dataOutputStream, getProtocol());
        WireformatUtils.serializeString(dataOutputStream, serverAddress);
        WireformatUtils.serializeString(dataOutputStream, sourceAddress);
    }

    @Override
    public int getProtocol() {
        return protocol;
    }

    @Override
    public byte[] getBytes() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

            dataOutputStream.writeInt(getProtocol());
            WireformatUtils.serializeString(dataOutputStream, serverAddress);
            WireformatUtils.serializeString(dataOutputStream, sourceAddress);

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
        return "MessageHeader{" +
            "protocol=" + protocol +
            ", serverAddress='" + serverAddress + '\'' +
            '}';
    }
}
