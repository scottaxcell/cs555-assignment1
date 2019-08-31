package cs555.dfs.wireformats;

import java.io.IOException;

public interface Message {
    int getProtocol();

    byte[] getBytes() throws IOException;
}
