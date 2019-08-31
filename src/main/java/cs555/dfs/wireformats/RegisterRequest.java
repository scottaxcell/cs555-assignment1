package cs555.dfs.wireformats;

import cs555.dfs.transport.TcpConnection;

import java.io.*;

public class RegisterRequest implements Message {
    private MessageHeader messageHeader;

    public RegisterRequest(TcpConnection tcpConnection) {
        this.messageHeader = new MessageHeader(getProtocol(), tcpConnection);
    }

    public RegisterRequest(byte[] bytes) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

        this.messageHeader = MessageHeader.deserialize(dataInputStream);

        byteArrayInputStream.close();
        dataInputStream.close();
    }

    @Override
    public int getProtocol() {
        return Protocol.REGISTER_REQUEST;
    }

    @Override
    public byte[] getBytes() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

        dataOutputStream.write(messageHeader.getBytes());
        dataOutputStream.flush();

        byte[] data = byteArrayOutputStream.toByteArray();

        byteArrayOutputStream.close();
        dataOutputStream.close();

        return data;
    }

    @Override
    public String toString() {
        return "RegisterRequest{" +
            "messageHeader=" + messageHeader +
            '}';
    }
}
