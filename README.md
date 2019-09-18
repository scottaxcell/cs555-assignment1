# CS 555: Distributed Systems -- Assignment 1
Building a distributed, replicated, and fault tolerant path system

## TODO
### Miscellaneous
* detect server going down and cleanup shards
DEBUG: got all 169 expected shards
Exception in thread "Thread-170" java.lang.ArrayIndexOutOfBoundsException: 8
        at cs555.dfs.util.ErasureEncoderDecoder.decode(ErasureEncoderDecoder.java:87)
        at cs555.dfs.node.client.FileReader.writeFileErasure(FileReader.java:129)
        at cs555.dfs.node.client.FileReader.handleRetrieveShardResponse(FileReader.java:73)
        at cs555.dfs.node.client.Client.handleRetrieveShardResponse(Client.java:276)
        at cs555.dfs.node.client.Client.onMessage(Client.java:224)
        at cs555.dfs.transport.TcpReceiver.run(TcpReceiver.java:39)
        at java.lang.Thread.run(Thread.java:748)

* choose random server based on disk space percentage
* cleanup up status printouts
* ensure all classes are thread safe
* ~~store shards on random servers, not just one which is happening now~~
* ~~erasure printout not printing any shards~~
* ~~can i remove the socket from the messages and create constructors that take serialized data~~
* ~~create run script to launch controller, chunk servers, and client for easier testing~~
* ~~add temp testing command line handling for easier testing~~
* ~~figure out storing of already connected remote hosts, is this needed?~~
* ~~list available files from client, select one and prompt for dir to write to~~
* ~~remove tcp connection from connections when it dies~~
* ~~do not change dir when running executables, just point to build dir~~
* ~~remove progress bar from list look up~~

#### Code cleanup
* ~~standardize serialization and deserialization api to be shared among wireformats~~
* ~~improve small classes for grouping chunk info together FileDataChunk, WireChunk, etc.~~
* ~~improve try catch blocks by pushing them down~~

### Controller
* ~~use chunk server free-space to determine applicable nodes~~
* ~~heartbeat to chunk server to detect failures~~
* ~~find 3 chunk servers for a chunk write~~

### Chunk server
* ~~minor heartbeat -- notify of newly added chunks~~
* ~~major heartbeat -- metadata about all the chunks~~
* ~~all heartbeats -- total number of chunks and free-space~~
* ~~report path corruption to controller~~
* ~~add error detection by hashing on reads and writes~~
* chunk replication should acknowledge the version level instead of incrementing on every single write

### Client
* ~~ask controller for chunk servers to write chunk to~~
* ~~chunkify path~~
* ~~send chunk to first chunk server -- append next chunk servers~~
* ~~read in path from command line~~
* ~~reed solomonize chunks~~

## Notes
* the client reads the entire path into memory, could improve by streaming path a chunk at a time
* chunk server does not recover after shutdown, starts a clean slate
