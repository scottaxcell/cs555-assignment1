#!/bin/env bash
#
CLASSES_DIR=$(pwd)/build/classes/java/main
LIB_DIR=$(pwd)/lib

java -cp $CLASSES_DIR:$LIB_DIR cs555.dfs.node.client.Client tokyo 50321
