package cs555.dfs.wireformats;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class FileListResponseErasure implements Message {
    private MessageHeader messageHeader;
    private List<String> fileNames = new ArrayList<>();

    public FileListResponseErasure(String serverAddress, String sourceAdress, List<String> fileNames) {
        messageHeader = new MessageHeader(getProtocol(), serverAddress, sourceAdress);
        this.fileNames = fileNames;
    }

    @Override
    public int getProtocol() {
        return Protocol.FILE_LIST_RESPONSE_ERASURE;
    }

    public FileListResponseErasure(byte[] bytes) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

            messageHeader = MessageHeader.deserialize(dataInputStream);
            int numFiles = WireformatUtils.deserializeInt(dataInputStream);
            for (int i = 0; i < numFiles; i++)
                fileNames.add(WireformatUtils.deserializeString(dataInputStream));

            byteArrayInputStream.close();
            dataInputStream.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public byte[] getBytes() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

            messageHeader.serialize(dataOutputStream);
            WireformatUtils.serializeInt(dataOutputStream, fileNames.size());
            for (String fileName : fileNames)
                WireformatUtils.serializeString(dataOutputStream, fileName);

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
        return "FileListResponseErasure{" +
            "messageHeader=" + messageHeader +
            ", fileNames=" + fileNames +
            '}';
    }

    public String getServerAddress() {
        return messageHeader.getServerAddress();
    }

    public String getSourceAddress() {
        return messageHeader.getSourceAddress();
    }

    public List<String> getFileNames() {
        return fileNames;
    }
}
