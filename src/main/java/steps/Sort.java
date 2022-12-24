package steps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import utilities.Names;
import utilities.TextDoubleWritablePair;

import java.io.IOException;

public class Sort {
    private static double totals = 0;
    public static class SortMap extends Mapper<LongWritable, Text, TextDoubleWritablePair, Text> {

        @Override
        public void map(LongWritable file, Text line, Context context) throws InterruptedException, IOException {
            String[] split;
            split = line.toString().split("\t");
            context.write(
                    new TextDoubleWritablePair(
                            new Text(split[0]),
                            new DoubleWritable(Double.parseDouble(split[1]))
                    ),
                    new Text("")
            );
        }
    }

    public static class SortReducer extends Reducer<TextDoubleWritablePair, Text, Text, DoubleWritable> {

        @Override
        public void reduce(TextDoubleWritablePair key, Iterable<Text> values, Context context) throws InterruptedException, IOException {
            context.write(key.first, key.second);
            totals += key.second.get();
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String randomID = args[0];
        Path input = new Path(Names.BUCKET + randomID + "/formula-reduce");
        Path output = new Path(Names.BUCKET + randomID + "/sort-reduce");

        Job job = Job.getInstance(conf, "Sort");
        job.setJarByClass(Sort.class);
        job.setMapperClass(Sort.SortMap.class);
        job.setReducerClass(Sort.SortReducer.class);
        job.setMapOutputKeyClass(TextDoubleWritablePair.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        boolean done = job.waitForCompletion(true);
        System.out.println(totals);
        System.exit(done ? 0 : 1);
    }

}
