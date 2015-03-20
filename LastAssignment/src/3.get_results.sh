#!/bin/sh

HFS_OUTPUT_FOLDER=/outputLastAssignment
LOCAL_OUTPUT_FOLDER=outputLastAssignment

echo "Cleaning up..."
rm -rf *.class
rm -rf $LOCAL_OUTPUT_FOLDER
hadoop fs -rm -r $HFS_OUTPUT_FOLDER

echo "Compiling..."
javac -cp "../lib/hadoop/*" LastAssignment.java
jar cvf LastAssignment.jar *.class

echo "Running jobs..."
hadoop jar LastAssignment.jar LastAssignment outputPig $HFS_OUTPUT_FOLDER
hadoop fs -get $HFS_OUTPUT_FOLDER $LOCAL_OUTPUT_FOLDER

echo "MapReduce completed !"
cat $LOCAL_OUTPUT_FOLDER/part-*
