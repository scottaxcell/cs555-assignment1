package cs555.dfs.node.client;

import cs555.dfs.wireformats.FileListResponse;

import java.util.concurrent.atomic.AtomicBoolean;

public class FileLister {
    private final Client client;
    private AtomicBoolean isRunning = new AtomicBoolean();

    public FileLister(Client client) {
        this.client = client;
    }

    public boolean isRunning() {
        return isRunning.get();
    }

    public void setIsRunning(boolean isRunning) {
        this.isRunning.set(isRunning);
    }

    public void handleFileListResponse(FileListResponse response) {
        setIsRunning(false);
    }
}
