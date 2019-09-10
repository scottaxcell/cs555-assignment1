package cs555.dfs.transport;

import cs555.dfs.util.Utils;

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

    public static TcpSender of(String remoteServerAddress) {
        String[] splitServerAddress = Utils.splitServerAddress(remoteServerAddress);
        try {
            Socket socket = new Socket(splitServerAddress[0], Integer.valueOf(splitServerAddress[1]));
            return new TcpSender(socket);
        }
        catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public synchronized void send(byte[] data) {
        try {
            int dataLength = data.length;
            dataOutputStream.writeInt(dataLength);
            dataOutputStream.write(data, 0, dataLength);
            dataOutputStream.flush();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized void sendNoCatch(byte[] data) throws IOException {
        int dataLength = data.length;
        dataOutputStream.writeInt(dataLength);
        dataOutputStream.write(data, 0, dataLength);
        dataOutputStream.flush();
    }

    public Socket getSocket() {
        return socket;
    }

    public String getLocalSocketAddress() {
        return socket.getLocalSocketAddress().toString();
    }
}
