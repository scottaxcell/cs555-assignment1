package cs555.dfs.util;

import java.util.Objects;

public class ChunkData {
    public final String fileName;
    public final int sequence;
    public byte[] data;

    public ChunkData(String fileName, int sequence, byte[] data) {
        this.fileName = fileName;
        this.sequence = sequence;
        this.data = data;
    }

    public ChunkData(String fileName, int sequence) {
        this.fileName = fileName;
        this.sequence = sequence;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sequence);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ChunkData that = (ChunkData) o;
        return sequence == that.sequence;
    }

    public int getSequence() {
        return sequence;
    }

    public byte[] getData() {
        return data;
    }
}
