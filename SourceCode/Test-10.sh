echo "Cleaning up..."
rm -rf *.class
rm -rf outputTest10
hadoop fs -rm -r /inputTest10
hadoop fs -rm -r /outputTest10

echo "Compiling..."
javac -cp "lib/*" TopTen.java
jar cvf TopTen.jar *.class
hadoop fs -mkdir /inputTest10

echo "Uploading files..."
hadoop fs -put Data/TopTenUsersChallenge_?? /inputTest10

echo "Running jobs..."
hadoop jar TopTen.jar TopTen /inputTest10 /outputTest10
hadoop fs -get /outputTest10 outputTest10

echo "Test completed !"
