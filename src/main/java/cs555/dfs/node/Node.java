package cs555.dfs.node;

import cs555.dfs.transport.TcpConnection;
import cs555.dfs.wireformats.Message;

public interface Node {
    void onMessage(Message message);

    String getNodeTypeAsString();

    void registerNewTcpConnection(TcpConnection tcpConnection);

    String getServerAddress();
}
