import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCount {
    static class WordCountMapper extends Mapper<LongWritable, Text, Text, Text> {
        private javax.xml.soap.Text word = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            line = line.replaceAll("[^a-zA-Z0-9+]", " ");
            Text fileName = new Text(((FileSplit) context.getInputSplit()).getPath().getName());
            StringTokenizer tokenizer = new StringTokenizer(line);
            
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken().toLowerCase());
                context.write(word, fileName);
            }
        }
    }

    static class WordCountReducer extends Reducer<Text, Text, Text, IntWritable> {
        public void reduce(Text key, Iterable<Text> docs, Context context) throws IOException, InterruptedException {
            HashMap<Text, Integer> docMap = new HashMap<Text, Integer>();
            // Hashmap flips the key/val pairs, creating a word count for every word within a file

            // For each document in the list (from word, DocumentList) place it in a hash map
            for (Text docId : docs) {
                Text k = new Text(key.toString() + "\t" + docId.toString());
                if (docMap.containsKey(k)) {
                    Integer x = docCount.get(k);
                    docMap.put(k, x + 1);
                } else {
                    docMap.put(k, 1);
                }
            }

            // Iterate through and write the results to the context (docId, wordcount)
            docMap.forEach((Text k, Integer v) -> {
                try {
                    context.write(k, new IntWritable(v));
                } catch (Exception e) {
                    System.out.println("An error occurred in mapper writing to context");
                }
            });
        }
    }
    
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.err.println("Usage: java WordCount <input path> <output path>");
            System.exit(-1);
        }
        Job job = new Job(new Configuration(), "wordcount");
        job.setJarByClass(WordCount.class);
        job.setJobName("WordCount");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }
}