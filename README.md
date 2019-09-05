# CS 555: Distributed Systems -- Assignment 1
Building a distributed, replicated, and fault tolerant path system

## TODO
### Miscellaneous
* ~~can i remove the socket from the messages and create constructors that take serialized data~~
* ~~create run script to launch controller, chunk servers, and client for easier testing~~
* ~~add temp testing command line handling for easier testing~~
* figure out storing of already connected remote hosts, is this needed?
* list available files from client, select one and prompt for dir to write to

#### Code cleanup
* standardize serialization and deserialization api to be shared among wireformats
* improve small classes for grouping chunk info together FileDataChunk, WireChunk, etc.
* improve try catch blocks by pushing them down

### Controller
* use chunk server free-space to determine applicable nodes
* heartbeat to chunk server to detect failures
* find 3 chunk servers for a chunk write


### Chunk server
* ~~minor heartbeat -- notify of newly added chunks~~
* ~~major heartbeat -- metadata about all the chunks~~
* ~~all heartbeats -- total number of chunks and free-space~~
* ~~report path corruption to controller~~
* add error detection by hashing on reads and writes
* write metadata to header of file data being written per chunk ?
* refresh state by reading from disk on initialization ?


### Client
* ~~ask controller for chunk servers to write chunk to~~
* ~~chunkify path~~
* ~~send chunk to first chunk server -- append next chunk servers~~
* ~~read in path from command line~~
* reed solomonize chunks

## Notes
* the client reads the entire path into memory, could improve by streaming path a chunk at a time
* chunk server does not recover after shutdown, starts a clean slate
