package cs555.dfs.wireformats;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class WireformatUtils {
    public static String deserializeString(DataInputStream dataInputStream) {
        try {
            int stringLength = dataInputStream.readInt();
            byte[] stringBytes = new byte[stringLength];
            dataInputStream.readFully(stringBytes);
            return new String(stringBytes);
        }
        catch (IOException e) {
            e.printStackTrace();
            return "";
        }
    }

    public static int deserializeInt(DataInputStream dataInputStream) {
        try {
            int i = dataInputStream.readInt();
            return i;
        }
        catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
    }

    public static long deserializeLong(DataInputStream dataInputStream) {
        try {
            long l = dataInputStream.readLong();
            return l;
        }
        catch (IOException e) {
            e.printStackTrace();
            return -1L;
        }
    }

    public static byte[] deserializeBytes(DataInputStream dataInputStream) {
        try {
            int bytesLength = dataInputStream.readInt();
            byte[] bytes = new byte[bytesLength];
            dataInputStream.readFully(bytes, 0, bytesLength);
            return bytes;
        }
        catch (IOException e) {
            return new byte[0];
        }
    }

    public static void serializeString(DataOutputStream dataOutputStream, String string) {
        try {
            dataOutputStream.writeInt(string.getBytes().length);
            dataOutputStream.write(string.getBytes());
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void serializeInt(DataOutputStream dataOutputStream, int i) {
        try {
            dataOutputStream.writeInt(i);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void serializeLong(DataOutputStream dataOutputStream, long l) {
        try {
            dataOutputStream.writeLong(l);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void serializeBytes(DataOutputStream dataOutputStream, byte[] bytes) {
        try {
            dataOutputStream.writeInt(bytes.length);
            dataOutputStream.write(bytes);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
