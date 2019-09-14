package cs555.dfs.wireformats.erasure;

import cs555.dfs.wireformats.Message;
import cs555.dfs.wireformats.MessageHeader;
import cs555.dfs.wireformats.Protocol;
import cs555.dfs.wireformats.WireformatUtils;

import java.io.*;

public class RetrieveFileRequestErasure implements Message {
    private MessageHeader messageHeader;
    private String fileName;

    public RetrieveFileRequestErasure(String serverAddress, String sourceAddress, String fileName) {
        this.messageHeader = new MessageHeader(getProtocol(), serverAddress, sourceAddress);
        this.fileName = fileName;
    }

    @Override
    public int getProtocol() {
        return Protocol.RETRIEVE_FILE_REQUEST_ERASURE;
    }

    @Override
    public byte[] getBytes() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

            dataOutputStream.write(messageHeader.getBytes());

            dataOutputStream.writeInt(fileName.length());
            dataOutputStream.write(fileName.getBytes());

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

    public RetrieveFileRequestErasure(byte[] bytes) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

            this.messageHeader = MessageHeader.deserialize(dataInputStream);
            fileName = WireformatUtils.deserializeString(dataInputStream);

            byteArrayInputStream.close();
            dataInputStream.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "RetrieveFileRequestErasure{" +
            "messageHeader=" + messageHeader +
            ", fileName='" + fileName + '\'' +
            '}';
    }

    public String getFileName() {
        return fileName;
    }

    public String getSourceAddress() {
        return messageHeader.getSourceAddress();
    }
}
