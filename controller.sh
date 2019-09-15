#!/bin/env bash
#
CLASSES_DIR=$(pwd)/build/classes/java/main

java -cp ${CLASSES_DIR} cs555.dfs.node.controller.Controller 50321
