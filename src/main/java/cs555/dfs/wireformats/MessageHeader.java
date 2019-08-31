package cs555.dfs.wireformats;

import cs555.dfs.transport.TcpConnection;

import java.io.*;

public class MessageHeader implements Message {
    private final int protocol;
    private final String sourceId;
    private final String destinationId;

    public MessageHeader(int protocol, TcpConnection tcpConnection) {
        this.protocol = protocol;
        this.sourceId = tcpConnection.getLocalSocketAddress();
        this.destinationId = tcpConnection.getRemoteSocketAddress();
    }

    public MessageHeader(int protocol, String sourceId, String destinationId) {
        this.protocol = protocol;
        this.sourceId = sourceId;
        this.destinationId = destinationId;
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

        dataOutputStream.writeInt(sourceId.getBytes().length);
        dataOutputStream.write(sourceId.getBytes());

        dataOutputStream.writeInt(destinationId.getBytes().length);
        dataOutputStream.write(destinationId.getBytes());

        dataOutputStream.flush();

        byte[] data = byteArrayOutputStream.toByteArray();

        byteArrayOutputStream.close();
        dataOutputStream.close();

        return data;
    }

    public String getSourceId() {
        return sourceId;
    }

    public String getDestinationId() {
        return destinationId;
    }

    public static MessageHeader deserialize(DataInputStream dataInputStream) throws IOException {
        int protocol = dataInputStream.readInt();

        int sourceLength = dataInputStream.readInt();
        byte[] sourceBytes = new byte[sourceLength];
        dataInputStream.readFully(sourceBytes);
        String sourceId = new String(sourceBytes);

        int destinationLength = dataInputStream.readInt();
        byte[] destinationBytes = new byte[destinationLength];
        dataInputStream.readFully(destinationBytes);
        String destinationId = new String(destinationBytes);

        return new MessageHeader(protocol, sourceId, destinationId);
    }

    @Override
    public String toString() {
        return "MessageHeader{" +
            "protocol=" + protocol +
            ", sourceId='" + sourceId + '\'' +
            ", destinationId='" + destinationId + '\'' +
            '}';
    }
}
