package cs555.dfs.wireformats;

import java.io.*;

public class RegisterRequest implements Message {
    private MessageHeader messageHeader;

    public RegisterRequest(String serverAddress, String sourceAdress) {
        messageHeader = new MessageHeader(getProtocol(), serverAddress, sourceAdress);
    }

    @Override
    public int getProtocol() {
        return Protocol.REGISTER_REQUEST;
    }

    @Override
    public byte[] getBytes() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

            messageHeader.serialize(dataOutputStream);

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

    public RegisterRequest(byte[] bytes) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

            messageHeader = MessageHeader.deserialize(dataInputStream);

            byteArrayInputStream.close();
            dataInputStream.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "RegisterRequest{" +
            "messageHeader=" + messageHeader +
            '}';
    }

    public String getServerAddress() {
        return messageHeader.getServerAddress();
    }

    public String getSourceAddress() {
        return messageHeader.getSourceAddress();
    }
}
