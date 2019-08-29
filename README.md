# CS 555: Distributed Systems -- Assignment 1
Building a distributed, replicated, and fault tolerant file system

## TODO
### Controller
* use chunk server free-space to determine applicable nodes
* heartbeat to chunk server to detect failures
* find 3 chunk servers for a chunk write


### Chunk server
* minor heartbeat -- notify of newly added chunks
* major heartbeat -- metadata about all the chunks
* all heartbeats -- total number of chunks and free-space
* report file corruption to controller

### Client
* ask controller for chunk servers to write chunk to
* chunkify file
* send chunk to first chunk server -- append next chunk servers
* read in file from command line
* reed solomonize chunks