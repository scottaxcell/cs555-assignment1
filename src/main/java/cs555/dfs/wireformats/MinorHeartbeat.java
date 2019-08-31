package cs555.dfs.wireformats;

import cs555.dfs.transport.TcpConnection;

import java.io.*;

public class MinorHeartbeat implements Message {
    private MessageHeader messageHeader;
    private long usableSpace;
    private int numChunks;

    public MinorHeartbeat(TcpConnection tcpConnection, long usableSpace, int numChunks) {
        this.messageHeader = new MessageHeader(getProtocol(), tcpConnection);
        this.usableSpace = usableSpace;
        this.numChunks = numChunks;
    }

    public MinorHeartbeat(byte[] bytes) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

        this.messageHeader = MessageHeader.deserialize(dataInputStream);

        usableSpace = dataInputStream.readLong();
        numChunks = dataInputStream.readInt();

        byteArrayInputStream.close();
        dataInputStream.close();
    }

    @Override
    public int getProtocol() {
        return Protocol.MINOR_HEART_BEAT;
    }

    @Override
    public byte[] getBytes() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

        dataOutputStream.write(messageHeader.getBytes());

        dataOutputStream.writeLong(usableSpace);

        dataOutputStream.writeInt(numChunks);

        dataOutputStream.flush();

        byte[] data = byteArrayOutputStream.toByteArray();

        byteArrayOutputStream.close();
        dataOutputStream.close();

        return data;
    }

    @Override
    public String toString() {
        return "MinorHeartbeat{" +
            "messageHeader=" + messageHeader +
            ", usableSpace=" + usableSpace +
            ", numChunks=" + numChunks +
            '}';
    }
}
