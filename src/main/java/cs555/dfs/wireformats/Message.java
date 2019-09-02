package cs555.dfs.wireformats;

public interface Message {
    int getProtocol();

    byte[] getBytes();
}
