EST="Test10"
INPUT_REGEX="??"

echo "Cleaning up..."
rm -rf *.class
rm -rf outputTest100
hadoop fs -rm -r /input$TEST
hadoop fs -rm -r /output$TEST

echo "Compiling..."
javac -cp "lib/*" TopTen.java
jar cvf TopTen.jar *.class
hadoop fs -mkdir /input$TEST

echo "Uploading files..."
hadoop fs -put Data/TopTenUsersChallenge_$INPUT_REGEX /input$TEST

echo "Running jobs..."
hadoop jar TopTen.jar TopTen /input$TEST /output$TEST
hadoop fs -get /output$TEST output$TEST

echo "Test completed !"
echo "And the winners are:"
cat output$TEST/part-*
