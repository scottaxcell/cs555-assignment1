package cs555.dfs.node;

import cs555.dfs.util.Utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

public class Chunk {
    private final String fileName;
    private final int sequence;
    private Path path;
    private int version;
    private Instant timeStamp;
    private List<String> checksums;

    public Chunk(String fileName, int sequence, Path path) {
        this.fileName = fileName;
        this.sequence = sequence;
        this.path = path;
    }

    public Chunk(String fileName, int version, int sequence, Instant timeStamp) {
        this.fileName = fileName;
        this.version = version;
        this.sequence = sequence;
        this.timeStamp = timeStamp;
    }

    public Path getPath() {
        return path;
    }

    public void writeChunk(byte[] bytes) {
        try {
            Utils.debug("writing " + path);
            Files.createDirectories(path.getParent());
            Files.write(path, bytes);
            incrementVersion();
            updateTimestamp();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void incrementVersion() {
        version++;
    }

    private void updateTimestamp() {
        timeStamp = Instant.now();
    }

    public byte[] readChunk() {
        byte[] bytes = new byte[0];
        try {
            bytes = Files.readAllBytes(path);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return bytes;
    }

    public String getFileName() {
        return fileName;
    }

    public int getVersion() {
        return version;
    }

    public int getSequence() {
        return sequence;
    }

    public Instant getTimeStamp() {
        // todo this can return null if chunk not written yet, fix this
        return timeStamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileName, sequence);
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
    public String toString() {
        return "Chunk{" +
            "fileName=" + fileName +
            ", path=" + path +
            ", version=" + version +
            ", sequence=" + sequence +
            ", timeStamp=" + timeStamp +
            '}';
    }

    public void setChecksum(List<String> checksums) {
        this.checksums = checksums;
    }

    public List<String> getChecksums() {
        return checksums;
    }
}
