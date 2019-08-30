#!/bin/env bash
#
CLASSES_DIR=$(pwd)/out/production/classes
#CLASSES_DIR=$(pwd)/build/classes/java/main

START_CHUNK_SERVER="cd $CLASSES_DIR; java -cp . cs555.dfs.node.ChunkServer tokyo 50321"

for chunk_server in $(cat chunk_servers.txt)
do
  echo 'logging into '$chunk_server
  COMMAND="xterm -e 'ssh -t $chunk_server \"$START_CHUNK_SERVER\"'"
  #echo $COMMAND
  eval $COMMAND &
done

