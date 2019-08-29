package cs555.dfs.node;

import cs555.dfs.wireformats.Event;

public interface Node {
    void onEvent(Event event);

    String getNodeTypeAsString();
}
