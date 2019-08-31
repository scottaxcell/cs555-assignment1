package cs555.dfs.node.chunkserver;

import cs555.dfs.util.Utils;
import cs555.dfs.wireformats.MinorHeartbeat;

import java.util.TimerTask;

public class MinorHeartbeatTimerTask extends TimerTask {
    private final ChunkServer chunkServer;

    public MinorHeartbeatTimerTask(ChunkServer chunkServer) {
        this.chunkServer = chunkServer;
    }

    @Override
    public void run() {
        Utils.debug("usableSpace: " + chunkServer.getUsableSpace());
        MinorHeartbeat heartbeat = new MinorHeartbeat(chunkServer.getUsableSpace(), chunkServer.getNumberOfChunks());
        chunkServer.sendEventToController(heartbeat);
    }
}
