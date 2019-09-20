package cs555.dfs.wireformats;

import java.io.*;
import java.util.Objects;

public class Chunk {
    private String fileName;
    private int sequence;
    private int version;
    private int size;

    public Chunk(String fileName, int sequence, int version, int size) {
        this.fileName = fileName;
        this.sequence = sequence;
        this.version = version;
        this.size = size;
    }

    public Chunk(byte[] bytes) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

            fileName = WireformatUtils.deserializeString(dataInputStream);
            sequence = WireformatUtils.deserializeInt(dataInputStream);
            version = WireformatUtils.deserializeInt(dataInputStream);
            size = WireformatUtils.deserializeInt(dataInputStream);

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
        int version = WireformatUtils.deserializeInt(dataInputStream);
        int size = WireformatUtils.deserializeInt(dataInputStream);
        return new Chunk(fileName, sequence, version, size);
    }

    public byte[] getBytes() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

            WireformatUtils.serializeString(dataOutputStream, fileName);
            WireformatUtils.serializeInt(dataOutputStream, sequence);
            WireformatUtils.serializeInt(dataOutputStream, version);

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
            ", version=" + version +
            ", size=" + size +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Chunk chunk = (Chunk) o;
        return sequence == chunk.sequence &&
            Objects.equals(fileName, chunk.fileName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileName, sequence);
    }

    public String getFileName() {
        return fileName;
    }

    public int getSequence() {
        return sequence;
    }

    public int getVersion() {
        return version;
    }

    public void serialize(DataOutputStream dataOutputStream) {
        WireformatUtils.serializeString(dataOutputStream, fileName);
        WireformatUtils.serializeInt(dataOutputStream, sequence);
        WireformatUtils.serializeInt(dataOutputStream, version);
        WireformatUtils.serializeInt(dataOutputStream, size);
    }

    public int getSize() {
        return size;
    }
}
