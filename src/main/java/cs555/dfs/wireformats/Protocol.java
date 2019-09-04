package cs555.dfs.wireformats;

public class Protocol {
    public static final int REGISTER_REQUEST = 300;
    public static final int STORE_CHUNK_REQUEST = 301;
    public static final int STORE_CHUNK_RESPONSE = 302;
    public static final int STORE_CHUNK = 303;
    public static final int MINOR_HEART_BEAT = 304;
    public static final int MAJOR_HEART_BEAT = 305;
    public static final int RETRIEVE_FILE_REQUEST = 306;
    public static final int RETRIEVE_FILE_RESPONSE = 307;
    public static final int RETRIEVE_CHUNK_REQUEST = 308;
    public static final int RETRIEVE_CHUNK_RESPONSE = 309;
    public static final int CORRUPT_CHUNK = 310;
}
