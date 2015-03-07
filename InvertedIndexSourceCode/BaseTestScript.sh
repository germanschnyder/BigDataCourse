#!/bin/sh
if [ -z "$1" ]
  then
    echo "No Test specified"
    exit
fi
if [ -z "$2" ]
  then
    echo "No input regex specified"
    exit

fi

TEST="$1"

echo "Cleaning up..."
rm -rf *.class
rm -rf output$TEST
hadoop fs -rm -r /input$TEST
hadoop fs -rm -r /output$TEST

echo "Compiling..."
javac -cp "lib/*" InvertedIndex.java
jar cvf InvertedIndex.jar *.class
hadoop fs -mkdir /input$TEST

echo "Uploading files..."
hadoop fs -put $2 /input$TEST

echo "Running jobs..."
hadoop jar InvertedIndex.jar InvertedIndex /input$TEST /output$TEST
hadoop fs -get /output$TEST output$TEST

echo "Test completed !"
echo "And the winners are:"
cat output$TEST/part-*
