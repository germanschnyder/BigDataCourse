echo "Cleaning up..."
rm -rf *.class
rm -rf outputTest100
hadoop fs -rm -r /inputTest100
hadoop fs -rm -r /outputTest100

echo "Compiling..."
javac -cp hadoop-core-1.0.4.jar:jackson-*.jar TopTen.java
jar cvf TopTen.jar *.class
hadoop fs -mkdir /inputTest100

echo "Uploading files..."
hadoop fs -put Data/TopTenUsersChallenge_1* /inputTest100

echo "Running jobs..."
hadoop jar TopTen.jar TopTen /inputTest100 /outputTest100
hadoop fs -get /outputTest100 outputTest100

echo "Test completed !"
