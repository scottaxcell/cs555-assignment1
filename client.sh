#!/bin/env bash
#
#CLASSES_DIR=$(pwd)/out/production/classes
CLASSES_DIR=$(pwd)/build/classes/java/main
LIB_DIR=$(pwd)/lib

cd $CLASSES_DIR
java -cp .:$LIB_DIR/* cs555.dfs.node.client.Client tokyo 50321
