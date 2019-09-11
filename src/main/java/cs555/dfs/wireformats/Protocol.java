package cs555.dfs.wireformats;

public class Protocol {
    public static final int REGISTER_REQUEST = 300;
    public static final int STORE_CHUNK_REQUEST = 301;
    public static final int STORE_CHUNK_RESPONSE = 302;
    public static final int STORE_CHUNK = 303;
    public static final int MINOR_HEARTBEAT = 304;
    public static final int MAJOR_HEARTBEAT = 305;
    public static final int RETRIEVE_FILE_REQUEST = 306;
    public static final int RETRIEVE_FILE_RESPONSE = 307;
    public static final int RETRIEVE_CHUNK_REQUEST = 308;
    public static final int RETRIEVE_CHUNK_RESPONSE = 309;
    public static final int CORRUPT_CHUNK = 310;
    public static final int REPLICATE_CHUNK = 311;
    public static final int ALIVE_HEARTBEAT = 312;
    public static final int FILE_LIST_REQUEST = 313;
    public static final int FILE_LIST_RESPONSE = 314;
}
