package cs555.dfs.wireformats;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class MessageFactory {
    public static Message getMessageFromData(byte[] data) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
        DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

        int protocol = dataInputStream.readInt();
        switch (protocol) {
            case Protocol.REGISTER_REQUEST:
                return new RegisterRequest(data);
            case Protocol.STORE_CHUNK_REQUEST:
                return new StoreChunkRequest(data);
            case Protocol.STORE_CHUNK_RESPONSE:
                return new StoreChunkResponse(data);
            case Protocol.STORE_CHUNK:
                return new StoreChunk(data);
            case Protocol.MINOR_HEARTBEAT:
                return new Heartbeat(data);
            case Protocol.MAJOR_HEARTBEAT:
                return new Heartbeat(data);
            case Protocol.RETRIEVE_FILE_REQUEST:
                return new RetrieveFileRequest(data);
            case Protocol.RETRIEVE_FILE_RESPONSE:
                return new RetrieveFileResponse(data);
            case Protocol.RETRIEVE_CHUNK_REQUEST:
                return new RetrieveChunkRequest(data);
            case Protocol.RETRIEVE_CHUNK_RESPONSE:
                return new RetrieveChunkResponse(data);
            case Protocol.CORRUPT_CHUNK:
                return new CorruptChunk(data);
            case Protocol.REPLICATE_CHUNK:
                return new ReplicateChunk(data);
            case Protocol.ALIVE_HEARTBEAT:
                return new AliveHeartbeat(data);
            case Protocol.FILE_LIST_REQUEST:
                return new FileListRequest(data);
            case Protocol.FILE_LIST_RESPONSE:
                return new FileListResponse(data);
            case Protocol.STORE_SHARD_REQUEST:
                return new StoreShardRequest(data);
            case Protocol.STORE_SHARD_RESPONSE:
                return new StoreShardResponse(data);
            case Protocol.STORE_SHARD:
                return new StoreShard(data);
            default:
                throw new RuntimeException(String.format("received an unknown message with protocol %d", protocol));
        }
    }
}
