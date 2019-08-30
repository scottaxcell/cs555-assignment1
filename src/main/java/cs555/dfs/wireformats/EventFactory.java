package cs555.dfs.wireformats;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;

public class EventFactory {
    private static final int SIZE_OF_INT = 4;
    private static final int SIZE_OF_BYTE = 1;

    public static Event getMessageFromData(byte[] data, Socket socket) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
        DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

        int protocol = dataInputStream.readInt();
        switch (protocol) {
            case Protocol.REGISTER_REQUEST:
                return createRegisterRequest(data.length, dataInputStream, socket);
            case Protocol.STORE_CHUNK_REQUEST:
                return createStoreRegisterRequest(data.length, dataInputStream);
            default:
                throw new RuntimeException(String.format("received an unknown event with protocol %d", protocol));
        }
    }

    private static StoreChunkRequest createStoreRegisterRequest(int dataLength, DataInputStream dataInputStream) throws IOException {
        /**
         * Event Type (int): STORE_CHUNK_REQUEST
         * Filename size (int)
         * Filename (String)
         * Chunk index (int)
         * Chunk size (int)
         * Chunk data (byte[])
         */
        int chunkSequence = dataInputStream.readInt();

        int fileNameLength = dataInputStream.readInt();
        byte[] fileNameBytes = new byte[fileNameLength];
        dataInputStream.readFully(fileNameBytes, 0, fileNameLength);
        String fileName = new String(fileNameBytes);

        int bytesLength = dataInputStream.readInt();
        byte[] bytes = new byte[bytesLength];
        dataInputStream.readFully(bytes, 0, bytesLength);

        return new StoreChunkRequest(fileName, chunkSequence, bytes);
    }

    private static RegisterRequest createRegisterRequest(int dataLength, DataInputStream dataInputStream, Socket socket) throws IOException {
        /**
         * Event Type (int): REGISTER_REQUEST
         * IP address (String)
         * Port number (int)
         */
        int ipLength = dataLength - SIZE_OF_INT * 2;
        byte[] ipBytes = new byte[ipLength];
        dataInputStream.readFully(ipBytes, 0, ipLength);
        String ip = new String(ipBytes);

        int port = dataInputStream.readInt();

        return new RegisterRequest(ip, port, socket);
    }
}
