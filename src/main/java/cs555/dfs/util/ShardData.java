package cs555.dfs.util;

import java.util.Objects;

public class ShardData {
    public final String fileName;
    public final int sequence;
    public final int fragment;
    public byte[] data;

    public ShardData(String fileName, int sequence, int fragment, byte[] data) {
        this.fileName = fileName;
        this.sequence = sequence;
        this.fragment = fragment;
        this.data = data;
    }

    public ShardData(String fileName, int sequence, int fragment) {
        this.fileName = fileName;
        this.sequence = sequence;
        this.fragment = fragment;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardData shardData = (ShardData) o;
        return sequence == shardData.sequence &&
            fragment == shardData.fragment &&
            Objects.equals(fileName, shardData.fileName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileName, sequence, fragment);
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

    public byte[] getData() {
        return data;
    }
}
