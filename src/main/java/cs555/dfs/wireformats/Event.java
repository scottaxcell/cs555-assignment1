package cs555.dfs.wireformats;

import java.io.IOException;

public interface Event {
    int getProtocol();

    byte[] getBytes() throws IOException;
}
