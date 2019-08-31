package cs555.dfs.wireformats;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class MinorHeartbeat implements Event {
    private final long usableSpace;
    private final int numChunks;

    public MinorHeartbeat(long usableSpace, int numChunks) {
        this.usableSpace = usableSpace;
        this.numChunks = numChunks;
    }

    @Override
    public int getProtocol() {
        return Protocol.MINOR_HEART_BEAT;
    }

    @Override
    public byte[] getBytes() throws IOException {
        /**
         * Event Type (int): MINOR_HEARTBEAT
         * Usable space (long)
         * Number of chunks (int)
         */
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

        dataOutputStream.writeInt(getProtocol());
        dataOutputStream.writeLong(usableSpace);
        dataOutputStream.writeInt(numChunks);
        dataOutputStream.flush();

        byte[] data = byteArrayOutputStream.toByteArray();

        byteArrayOutputStream.close();
        dataOutputStream.close();

        return data;
    }

    @Override
    public String toString() {
        return "MinorHeartbeat{" +
            "usableSpace=" + usableSpace +
            ", numChunks=" + numChunks +
            '}';
    }
}
