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
            case Protocol.MINOR_HEART_BEAT:
                return new MinorHeartbeat(data);
            case Protocol.MAJOR_HEART_BEAT:
                return new MajorHeartbeat(data);
            case Protocol.RETRIEVE_FILE_REQUEST:
                return new RetrieveFileRequest(data);
            default:
                throw new RuntimeException(String.format("received an unknown message with protocol %d", protocol));
        }
    }
}
