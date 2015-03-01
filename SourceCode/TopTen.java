import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Iterator;


public class TopTen extends Configured implements Tool{


  static class TopTenMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>
  {
      //hadoop supported data types
      private final static IntWritable one = new IntWritable(1);
      private Text word = new Text();

      //map method that performs the tokenizer job and framing the initial key value pairs
      public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
      {
          //taking one line at a time and tokenizing the same
          String line = value.toString();
          StringTokenizer tokenizer = new StringTokenizer(line);

          //iterating through all the words available in that line and forming the key value pair
          while (tokenizer.hasMoreTokens())
          {
              word.set(tokenizer.nextToken());
              //sending to output collector which inturn passes the same to reducer
              output.collect(word, one);
          }
      }
  }


  static class TopTenReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>
  {
      //reduce method accepts the Key Value pairs from mappers, do the aggregation based on keys and produce the final out put
      public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
      {
          int sum = 0;
          /*iterates through all the values available with a key and add them together and give the
           final result as the key and sum of its values*/
          while (values.hasNext())
          {
              sum += values.next().get();
          }
          output.collect(key, new IntWritable(sum));
      }
  }


    public int run(String[] args) throws Exception
    {
        //creating a JobConf object and assigning a job name for identification purposes
        JobConf conf = new JobConf(getConf(), TopTen.class);
        conf.setJobName("TopTen");

        //Setting configuration object with the Data Type of output Key and Value
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        //Providing the mapper and reducer class names

        conf.setJar("TopTen.jar");
        conf.setMapperClass(TopTenMapper.class);
        conf.setReducerClass(TopTenReducer.class);


        //the hdfs input and output directory to be fetched from the command line
        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new TopTen(),args);
        System.exit(res);
    }
}
