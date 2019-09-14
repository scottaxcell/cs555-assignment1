package cs555.dfs.wireformats;

import java.io.*;

public class Shard {
    private String fileName;
    private int sequence;
    private int fragment;

    public Shard(String fileName, int sequence, int fragment) {
        this.fileName = fileName;
        this.sequence = sequence;
        this.fragment = fragment;
    }

    public Shard(byte[] bytes) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

            fileName = WireformatUtils.deserializeString(dataInputStream);
            sequence = WireformatUtils.deserializeInt(dataInputStream);
            fragment = WireformatUtils.deserializeInt(dataInputStream);

            byteArrayInputStream.close();
            dataInputStream.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static cs555.dfs.wireformats.Shard deserialize(DataInputStream dataInputStream) {
        String fileName = WireformatUtils.deserializeString(dataInputStream);
        int sequence = WireformatUtils.deserializeInt(dataInputStream);
        int fragment = WireformatUtils.deserializeInt(dataInputStream);
        return new cs555.dfs.wireformats.Shard(fileName, sequence, fragment);
    }

    public byte[] getBytes() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

            WireformatUtils.serializeString(dataOutputStream, fileName);
            WireformatUtils.serializeInt(dataOutputStream, sequence);
            WireformatUtils.serializeInt(dataOutputStream, fragment);

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
        return "Shard{" +
            "fileName='" + fileName + '\'' +
            ", sequence=" + sequence +
            ", fragment=" + fragment +
            '}';
    }

    public String getFileName() {
        return fileName;
    }

    public int getSequence() {
        return sequence;
    }

    public int getFragment() {
        return fragment;
    }

    public void serialize(DataOutputStream dataOutputStream) {
        WireformatUtils.serializeString(dataOutputStream, fileName);
        WireformatUtils.serializeInt(dataOutputStream, sequence);
        WireformatUtils.serializeInt(dataOutputStream, fragment);
    }
}
