package cs555.dfs.wireformats.erasure;

import cs555.dfs.wireformats.WireformatUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Objects;

public class ShardLocation {
    private final Shard shard;
    private final String serverAddress;

    public ShardLocation(Shard shard, String serverAddress) {
        this.shard = shard;
        this.serverAddress = serverAddress;
    }

    public static ShardLocation deserialize(DataInputStream dataInputStream) {
        Shard shard = Shard.deserialize(dataInputStream);
        String server = WireformatUtils.deserializeString(dataInputStream);
        return new ShardLocation(shard, server);
    }

    public void serialize(DataOutputStream dataOutputStream) {
        shard.serialize(dataOutputStream);
        WireformatUtils.serializeString(dataOutputStream, serverAddress);
    }

    @Override
    public String toString() {
        return "ShardLocation{" +
            "shard=" + shard +
            ", serverAddress='" + serverAddress + '\'' +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardLocation that = (ShardLocation) o;
        return Objects.equals(shard, that.shard);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shard);
    }

    public String getFileName() {
        return shard.getFileName();
    }

    public int getSequence() {
        return shard.getSequence();
    }

    public String getServerAddress() {
        return serverAddress;
    }

    public int getFragment() {
        return shard.getFragment();
    }
}
