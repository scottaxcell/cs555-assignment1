package cs555.dfs.wireformats;

import java.io.*;

public class MessageHeader implements Message {
    private final int protocol;
    private final String serverAddress;
    private final String sourceAddress;

    MessageHeader(int protocol, String serverAddress, String sourceAddress) {
        this.protocol = protocol;
        this.serverAddress = serverAddress;
        this.sourceAddress = sourceAddress;
    }

    @Override
    public int getProtocol() {
        return protocol;
    }

    @Override
    public byte[] getBytes() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

        dataOutputStream.writeInt(getProtocol());

        dataOutputStream.writeInt(serverAddress.getBytes().length);
        dataOutputStream.write(serverAddress.getBytes());

        dataOutputStream.writeInt(sourceAddress.getBytes().length);
        dataOutputStream.write(sourceAddress.getBytes());

        dataOutputStream.flush();

        byte[] data = byteArrayOutputStream.toByteArray();

        byteArrayOutputStream.close();
        dataOutputStream.close();

        return data;
    }

    public String getServerAddress() {
        return serverAddress;
    }

    public String getSourceAddress() {
        return sourceAddress;
    }

    public static MessageHeader deserialize(DataInputStream dataInputStream) throws IOException {
        int protocol = dataInputStream.readInt();

        int serverAddressLength = dataInputStream.readInt();
        byte[] serverAddressBytes = new byte[serverAddressLength];
        dataInputStream.readFully(serverAddressBytes);
        String serverAddress = new String(serverAddressBytes);

        int sourceAddressLength = dataInputStream.readInt();
        byte[] sourceAddressBytes = new byte[sourceAddressLength];
        dataInputStream.readFully(sourceAddressBytes);
        String sourceAddress = new String(sourceAddressBytes);

        return new MessageHeader(protocol, serverAddress, sourceAddress);
    }

    @Override
    public String toString() {
        return "MessageHeader{" +
            "protocol=" + protocol +
            ", serverAddress='" + serverAddress + '\'' +
            '}';
    }
}
