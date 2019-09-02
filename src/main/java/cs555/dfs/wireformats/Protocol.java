package cs555.dfs.wireformats;

public class Protocol {
    public static final int REGISTER_REQUEST = 300;
    public static final int STORE_CHUNK_REQUEST = 301;
    public static final int STORE_CHUNK_RESPONSE = 302;
    public static final int STORE_CHUNK = 303;
    public static final int MINOR_HEART_BEAT = 304;
    public static final int MAJOR_HEART_BEAT = 305;
}
