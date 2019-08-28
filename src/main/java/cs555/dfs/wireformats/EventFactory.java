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
            default:
                throw new RuntimeException(String.format("received an unknown event with protocol %d", protocol));
        }
    }
}
