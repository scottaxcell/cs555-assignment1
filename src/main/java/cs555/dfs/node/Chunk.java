package cs555.dfs.node;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

public class Chunk {
    private final Path path;
    private int version;
    private int sequence;
    private Instant timeStamp;

    public Chunk(Path path) {
        this.path = path;
    }

    public Path getPath() {
        return path;
    }

    @Override
    public String toString() {
        return "Chunk{" +
            "path=" + path +
            ", version=" + version +
            ", sequence=" + sequence +
            ", timeStamp=" + timeStamp +
            '}';
    }

    public void writeChunk(byte[] bytes) {
        try {
            Files.write(path, bytes);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
