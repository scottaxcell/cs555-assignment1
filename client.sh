#!/bin/env bash
#
#CLASSES_DIR=$(pwd)/out/production/classes
CLASSES_DIR=$(pwd)/build/classes/java/main

cd $CLASSES_DIR
java -cp . cs555.dfs.node.Client tokyo 50321
