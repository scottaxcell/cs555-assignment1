package cs555.dfs.transport;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class TcpSender {
    private Socket socket;
    private DataOutputStream dataOutputStream;

    public TcpSender(Socket socket) {
        this.socket = socket;
        try {
            dataOutputStream = new DataOutputStream(socket.getOutputStream());
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized void send(byte[] data) throws IOException {
        int dataLength = data.length;
        dataOutputStream.writeInt(dataLength);
        dataOutputStream.write(data, 0, dataLength);
        dataOutputStream.flush();
    }

    public Socket getSocket() {
        return socket;
    }
}
