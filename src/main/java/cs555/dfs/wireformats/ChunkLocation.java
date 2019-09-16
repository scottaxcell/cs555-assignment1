package cs555.dfs.wireformats;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Objects;

public class ChunkLocation {
    private final Chunk chunk;
    private final String serverAddress;

    public ChunkLocation(Chunk chunk, String serverAddress) {
        this.chunk = chunk;
        this.serverAddress = serverAddress;
    }

    public static ChunkLocation deserialize(DataInputStream dataInputStream) {
        Chunk chunk = Chunk.deserialize(dataInputStream);
        String server = WireformatUtils.deserializeString(dataInputStream);
        return new ChunkLocation(chunk, server);
    }

    public void serialize(DataOutputStream dataOutputStream) {
        chunk.serialize(dataOutputStream);
        WireformatUtils.serializeString(dataOutputStream, serverAddress);
    }

    @Override
    public String toString() {
        return "ChunkLocation{" +
            "chunk=" + chunk +
            ", serverAddress='" + serverAddress + '\'' +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ChunkLocation that = (ChunkLocation) o;
        return Objects.equals(chunk, that.chunk);
    }

    @Override
    public int hashCode() {
        return Objects.hash(chunk);
    }

    public String getFileName() {
        return chunk.getFileName();
    }

    public int getSequence() {
        return chunk.getSequence();
    }

    public String getServerAddress() {
        return serverAddress;
    }
}
