import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.Map;
import org.codehaus.jackson.map.*;
import org.codehaus.jackson.*;


public class TopTen extends Configured implements Tool{


  static class TopTenMapper extends MapReduceBase implements Mapper<Object, Text, NullWritable, Text>
  {
    private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

    private static ObjectMapper mapper = new ObjectMapper();

    //map method that performs the tokenizer job and framing the initial key value pairs
    public void map(Object key, Text value, OutputCollector<NullWritable, Text> output, Reporter reporter) throws  IOException
    {
      //taking one line at a time and tokenizing the same
      String line = value.toString();

      Map<String, String> parsed = mapper.readValue(value.toString(), Map.class);

      System.out.println(parsed);

      String userId = parsed.get("ip_address");
      String reputation = parsed.get("rating");

      repToRecordMap.put(Integer.parseInt(reputation), new Text(value) );

      if (repToRecordMap.size() > 10) {
        repToRecordMap.remove(repToRecordMap.firstKey());
      }

      for (Text t : repToRecordMap.values()) {
        output.collect(NullWritable.get(), t);
      }

    }

    //TODO Cleanup

  }


  static class TopTenReducer extends MapReduceBase implements Reducer<NullWritable, Text, NullWritable, Text>
  {

    private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();
    private static ObjectMapper mapper = new ObjectMapper();

    //reduce method accepts the Key Value pairs from mappers, do the aggregation based on keys and produce the final out put
    public void reduce(NullWritable key, Iterator<Text> values, OutputCollector<NullWritable, Text> output, Reporter reporter) throws IOException
    {

      while (values.hasNext())
      {
        Text value = values.next();
        Map<String, String> parsed = mapper.readValue(value.toString(), Map.class);
        repToRecordMap.put(Integer.parseInt(parsed.get("rating")), new Text(value));
        // If we have more than ten records, remove the one with the lowest rep // As this tree map is sorted in descending order, the user with
        // the lowest reputation is the last key.
        if (repToRecordMap.size() > 10) {
          repToRecordMap.remove(repToRecordMap.firstKey());
        }
      }

      for (Text t : repToRecordMap.descendingMap().values()) {
        // Output our ten records to the file system with a null key
        output.collect(NullWritable.get(), t);
      }
    }
  }


  public int run(String[] args) throws Exception
  {
    //creating a JobConf object and assigning a job name for identification purposes
    JobConf conf = new JobConf(getConf(), TopTen.class);
    conf.setJobName("TopTen");

    //Setting configuration object with the Data Type of output Key and Value
    //conf.setOutputKeyClass(Text.class);
    //conf.setOutputValueClass(IntWritable.class);

    conf.setMapOutputKeyClass(NullWritable.class);
    conf.setMapOutputValueClass(Text.class);
    conf.setOutputKeyClass(NullWritable.class);
    conf.setOutputValueClass(Text.class);

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
