package cs555.dfs.node;

import cs555.dfs.util.Utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public class Shard {
    private final String fileName;
    private final int sequence;
    private final int fragment;
    private Path path;

    public Shard(String fileName, int sequence, int fragment) {
        this.fileName = fileName;
        this.sequence = sequence;
        this.fragment = fragment;
    }

    public Shard(String fileName, int sequence, int fragment, Path path) {
        this.fileName = fileName;
        this.sequence = sequence;
        this.fragment = fragment;
        this.path = path;
    }

    public void writeShard(byte[] bytes) {
        try {
            Utils.debug("writing " + path);
            Files.createDirectories(path.getParent());
            Files.write(path, bytes);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public byte[] readShard() {
        byte[] bytes = new byte[0];
        try {
            bytes = Files.readAllBytes(path);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return bytes;
    }

    @Override
    public String toString() {
        return "Shard{" +
            "fileName='" + fileName + '\'' +
            ", sequence=" + sequence +
            ", fragment=" + fragment +
            ", path=" + path +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Shard shard = (Shard) o;
        return sequence == shard.sequence &&
            fragment == shard.fragment &&
            fileName.equals(shard.fileName);
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
}
