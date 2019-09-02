package cs555.dfs.wireformats;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Objects;

public class WireChunk {
    private final String fileName;
    private final int sequence;
    private final String serverAddress;

    public WireChunk(String fileName, int sequence, String serverAddress) {
        this.fileName = fileName;
        this.sequence = sequence;
        this.serverAddress = serverAddress;
    }

    public static WireChunk deserialize(DataInputStream dataInputStream) {
        String fileName = WireformatUtils.deserializeString(dataInputStream);
        int sequence = WireformatUtils.deserializeInt(dataInputStream);
        String server = WireformatUtils.deserializeString(dataInputStream);
        return new WireChunk(fileName, sequence, server);
    }

    public void serialize(DataOutputStream dataOutputStream) {
        WireformatUtils.serializeString(dataOutputStream, fileName);
        WireformatUtils.serializeInt(dataOutputStream, sequence);
        WireformatUtils.serializeString(dataOutputStream, serverAddress);
    }

    @Override
    public String toString() {
        return "WireChunk{" +
            "fileName='" + fileName + '\'' +
            ", sequence=" + sequence +
            ", serverAddress='" + serverAddress + '\'' +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WireChunk wireChunk = (WireChunk) o;
        return sequence == wireChunk.sequence &&
            fileName.equals(wireChunk.fileName);
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

    public String getServerAddress() {
        return serverAddress;
    }
}
