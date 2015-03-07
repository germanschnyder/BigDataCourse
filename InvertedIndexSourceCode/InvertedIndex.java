import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;


public class InvertedIndex extends Configured implements Tool {

    // This helper function parses the stackoverflow into a Map for us.
    static Map<String, String> transformXmlToMap(String xml) {
        Map<String, String> map = new HashMap<String, String>();
        try {
            String[] tokens = xml.trim().substring(5, xml.trim().length() - 3)
                    .split("\"");

            for (int i = 0; i < tokens.length - 1; i += 2) {
                String key = tokens[i].trim();
                String val = tokens[i + 1];

                map.put(key.substring(0, key.length() - 1), val);
            }
        } catch (StringIndexOutOfBoundsException e) {
            System.err.println(xml);
        }

        return map;
    }

    // Takes in a String of unescaped HTML and returns a Wikipedia URL if found, or null otherwise
    static String getWikipediaURL(String txt) {

        //lets find any url...
        Matcher matcher = Pattern.compile("[https]+://[^ \n]*").matcher(txt);

        while(matcher.find())
        {
            String url = matcher.group(0);

            //if the url is from wikipedia
            if (url.contains("wikipedia.org"))
                //just exit...
                return url;
        }

        return null;
    }

    /**
     * Mapper class, will receive all the files and output Wikipedia URLs
     */
    static class WikipediaExtractor extends MapReduceBase implements Mapper<Object, Text, Text, Text> {

        private Text link = new Text();
        private Text outkey = new Text();


        public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            Map<String, String> parsed = transformXmlToMap(value.toString());

            // Grab the necessary XML attributes
            String txt = parsed.get("Text");
            //String postType = parsed.get("PostTypeId");
            String row_id = parsed.get("Id");

            // if the body is null, or the post is a question (1), skip
            if (txt == null /*|| (postType != null && postType.equals("1"))*/) {
                return;
            }

            // Unescape the HTML because the SO data is escaped.
            txt = StringEscapeUtils.unescapeHtml(txt.toLowerCase());
            String url = getWikipediaURL(txt);
            if (url != null) {
                link.set(url);
                outkey.set(row_id);
                output.collect(link, outkey);
            }
        }
    }


    /**
     * Reducer class, will receive Wikipedia URL from mappers and generate the final list.
     */
    static class Concatenator extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            StringBuilder sb = new StringBuilder();
            boolean first = true;

            while (values.hasNext()) {
                Text value = values.next();

                if (first) {
                    first = false;
                } else {
                    sb.append(" ");
                }
                sb.append(value.toString());

                result.set(sb.toString());
                output.collect(key, result);
            }
        }
    }


    public int run(String[] args) throws Exception {
        //creating a JobConf object and assigning a job name for identification purposes
        JobConf conf = new JobConf(getConf(), InvertedIndex.class);
        conf.setJobName("InvertedIndex");

        //Setting configuration object with the Data Type of output Key and Value
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        //Providing the mapper and reducer class names
        conf.setJar("InvertedIndex.jar");
        conf.setMapperClass(WikipediaExtractor.class);
        conf.setReducerClass(Concatenator.class);

        //the hdfs input and output directory to be fetched from the command line
        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new InvertedIndex(), args);
        System.exit(res);
    }
}
