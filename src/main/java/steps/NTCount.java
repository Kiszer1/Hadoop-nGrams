package steps;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import utilities.LongTextWritablePair;
import utilities.Names;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


public class NTCount {

    private static final Text[] nZero = {new Text("N0")};
    private static final Text[] nOne = {new Text("N1")};
    private static final Text[] tZero = {new Text("T0")};
    private static final Text[] tOne = {new Text("T1")};
    private static final LongWritable one = new LongWritable(1);

    public static class NTCountMap extends Mapper<LongWritable, Text, LongTextWritablePair, LongTextWritablePair> {


        @Override
        public void map(LongWritable file, Text line, Context context) throws InterruptedException, IOException {
            String[] split;
            split = line.toString().split("\t");

            Text[] nGram = {new Text(split[0])};
            LongWritable n0 = new LongWritable(Long.parseLong(split[1]));
            LongWritable n1 = new LongWritable(Long.parseLong(split[2]));

            context.write(new LongTextWritablePair(n0, nZero), new LongTextWritablePair(one, nGram));
            context.write(new LongTextWritablePair(n1, nOne), new LongTextWritablePair(one, nGram));

            context.write(new LongTextWritablePair(n0, tZero), new LongTextWritablePair(n1, nGram));
            context.write(new LongTextWritablePair(n1, tOne), new LongTextWritablePair(n0, nGram));
        }

    }


    public static class NTCountReducer extends Reducer<LongTextWritablePair, LongTextWritablePair, Text, LongTextWritablePair> {

        @Override
        public void reduce(LongTextWritablePair key, Iterable<LongTextWritablePair> values, Context context) throws InterruptedException, IOException {
            long sum = 0;
            Set<Text> nGrams = new HashSet<>();
            for (LongTextWritablePair pair : values) {
                sum += pair.first.get();
                for (Text nGram : pair.second) {
                    nGrams.add(nGram);
                }
            }
            for (Text nGram : nGrams) {
                context.write(nGram, new LongTextWritablePair(new LongWritable(sum), key.second));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        String randomID = args[0];
        Path input = new Path(Names.BUCKET + randomID + "/count-reduce");
        Path output = new Path(Names.BUCKET + randomID + "/ntCount-reduce");
        Job job = Job.getInstance(config, "NTCount");
        job.setJarByClass(NTCount.class);
        job.setMapperClass(NTCountMap.class);
        job.setReducerClass(NTCountReducer.class);
        job.setMapOutputKeyClass(LongTextWritablePair.class);
        job.setMapOutputValueClass(LongTextWritablePair.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
