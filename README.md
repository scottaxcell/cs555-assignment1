# CS 555: Distributed Systems -- Assignment 1
Building a distributed, replicated, and fault tolerant path system

## Build
`$ gradle assemble`
## Run

### Automated Scripts
In the first terminal, launch the Controller.

`$ ./controller.sh`

***Note** This script should be launched from `tokyo.cs.colostate.edu`.*

In the second terminal, launch 20 Chunk Servers and a Client.

`$ ./chunk_servers_and_client.sh`

***Note** This script assumes that the Controller has been launched on `tokyo.cs.colostate.edu:50321`.*


### Manual
Controller

`$ java -cp build/classes/java/main cs555.dfs.node.controller.Controller <port>`

Client

`$ java -cp build/classes/java/main:lib/* cs555.dfs.node.client.Client <controller-host> <controller-port>`

Chunk Server

`$ java -cp build/classes/java/main cs555.dfs.node.chunkserver.ChunkServer <port> <controller-host> <controller-port>`
