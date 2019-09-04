package cs555.dfs.wireformats;

import java.io.*;

public class Chunk {
    private String fileName;
    private int sequence;

    public Chunk(String fileName, int sequence) {
        this.fileName = fileName;
        this.sequence = sequence;
    }

    public Chunk(byte[] bytes) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

            fileName = WireformatUtils.deserializeString(dataInputStream);
            sequence = WireformatUtils.deserializeInt(dataInputStream);

            byteArrayInputStream.close();
            dataInputStream.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Chunk deserialize(DataInputStream dataInputStream) {
        String fileName = WireformatUtils.deserializeString(dataInputStream);
        int sequence = WireformatUtils.deserializeInt(dataInputStream);
        return new Chunk(fileName, sequence);
    }

    public byte[] getBytes() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

            WireformatUtils.serializeString(dataOutputStream, fileName);
            WireformatUtils.serializeInt(dataOutputStream, sequence);

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
        return "Chunk{" +
            "fileName='" + fileName + '\'' +
            ", sequence=" + sequence +
            '}';
    }

    public String getFileName() {
        return fileName;
    }

    public int getSequence() {
        return sequence;
    }

    public void serialize(DataOutputStream dataOutputStream) {
        WireformatUtils.serializeString(dataOutputStream, fileName);
        WireformatUtils.serializeInt(dataOutputStream, sequence);
    }
}
