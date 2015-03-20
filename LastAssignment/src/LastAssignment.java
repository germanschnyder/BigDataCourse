import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.Map;

import org.codehaus.jackson.map.*;


public class LastAssignment extends Configured implements Tool {


    /**
     * Mapper class, will receive all the files and output a top ten list
     */
    static class LastAssignmentMapper extends MapReduceBase implements Mapper<Object, Text, NullWritable, Text> {

        //Treemap for easy appending new entries and sorting by default
        private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

        //Json parser
        private static ObjectMapper parser = new ObjectMapper();

        //Reference to OutputCollector so we can write output once the maps have finished
        private OutputCollector<NullWritable, Text> output2 = null;

        public void map(Object key, Text value, OutputCollector<NullWritable, Text> output, Reporter reporter) throws IOException {

            //save the reference to the output collector
            output2 = output;

            //Parse json
            Map<String, String> parsed = parser.readValue(value.toString(), Map.class);

            //Add every entry we read as a <reputation,user_data> tuple
            repToRecordMap.put(Integer.parseInt(parsed.get("WordCount")), new Text(value));

            //Key step, if treemap goes over 10, get rid of lowest value
            if (repToRecordMap.size() > 10) {
                repToRecordMap.remove(repToRecordMap.firstKey());
            }

            System.out.println("Processed entry " + value.toString());
        }

        /*
        Close method allows us to execute some tasks after all the mappings have finished in this mapper.
        For instance, we write the reemap content to the output collector
         */
        @Override
        public void close() throws IOException {

            for (Text t : repToRecordMap.values()) {
                output2.collect(NullWritable.get(), t);
            }
        }


    }


    /**
     * Reducer class, will receive partial top ten lists from mappers and generate the final top ten list.
     */
    static class LastAssignmentReducer extends MapReduceBase implements Reducer<NullWritable, Text, NullWritable, Text> {

        //Treemap for easy appending new entries and sorting by default
        private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

        //Json parser
        private static ObjectMapper parser = new ObjectMapper();

        public void reduce(NullWritable key, Iterator<Text> values, OutputCollector<NullWritable, Text> output, Reporter reporter) throws IOException {

            while (values.hasNext()) {
                Text value = values.next();

                //Parse json
                Map<String, String> parsed = parser.readValue(value.toString(), Map.class);

                //Add every entry we read as a <reputation,user_data> tuple
                repToRecordMap.put(Integer.parseInt(parsed.get("WordCount")), new Text(value));

                //Key step, if treemap goes over 10, get rid of lowest value
                if (repToRecordMap.size() > 10) {
                    repToRecordMap.remove(repToRecordMap.firstKey());

                }
            }

            //Get the final top ten and output to collector
            for (Text t : repToRecordMap.descendingMap().values()) {
                output.collect(NullWritable.get(), t);
            }
        }
    }


    public int run(String[] args) throws Exception {
        //creating a JobConf object and assigning a job name for identification purposes
        JobConf conf = new JobConf(getConf(), LastAssignment.class);
        conf.setJobName("LastAssignment");

        //Setting configuration object with the Data Type of output Key and Value
        conf.setMapOutputKeyClass(NullWritable.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setOutputKeyClass(NullWritable.class);
        conf.setOutputValueClass(Text.class);

        //Providing the mapper and reducer class names
        conf.setJar("LastAssignment.jar");
        conf.setMapperClass(LastAssignmentMapper.class);
        conf.setReducerClass(LastAssignmentReducer.class);

        //We want only one list, so we must use only one reducer
        conf.setNumReduceTasks(1);


        //the hdfs input and output directory to be fetched from the command line
        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new LastAssignment(), args);
        System.exit(res);
    }
}
