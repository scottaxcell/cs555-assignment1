package cs555.dfs.node.client;

import cs555.dfs.util.Utils;
import cs555.dfs.wireformats.FileListResponse;

import java.util.List;
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
        List<String> fileNames = response.getFileNames();
        if (fileNames.isEmpty())
            Utils.info("No files currently available for download\n");
        else {
            StringBuilder stringBuilder = new StringBuilder("Files available for download:\n");
            int i = 0;
            for (String fileName : fileNames) {
                stringBuilder.append("\t");
                stringBuilder.append(++i);
                stringBuilder.append(".\t");
                stringBuilder.append(fileName);
                stringBuilder.append("\n");
            }
            Utils.info(stringBuilder.toString());
        }
    }
}
