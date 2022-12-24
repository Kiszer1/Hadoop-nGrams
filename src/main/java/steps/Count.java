package steps;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import utilities.LongWritablePair;
import utilities.Names;

import java.io.*;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;


public class Count {

    public enum N {
        Counter
    }

    public static class CountMap extends Mapper<LongWritable, Text, Text, LongWritablePair> {

        private Set<String> stopWords = new HashSet<>();
        private final Pattern chars = Pattern.compile("[^a-zA-Z ]");

        private boolean checkSplit(String[] split) {
            if (split.length < 3 || chars.matcher(split[0]).find() || split[0].split(" ").length < 3)
                return false;
            String[] words = split[0].split(" ");
            for (String word : words) {
                if (stopWords.contains(word.toLowerCase()))
                    return false;
            }
            return true;


        }

        private void stopWords() throws IOException {
            AmazonS3 s3 = AmazonS3ClientBuilder
                    .standard()
                    .withRegion(Names.REGION)
                    .build();
            GetObjectRequest request = new GetObjectRequest(Names.NAME, "eng-stopwords.txt");
            S3Object object = s3.getObject(request);

            InputStreamReader streamReader = new InputStreamReader(object.getObjectContent());
            BufferedReader reader = new BufferedReader(streamReader);

            String line = reader.readLine();
            while (line != null) {
                stopWords.add(line);
                line = reader.readLine();
            }
        }

        @Override
        public void setup(Context context) throws IOException {
            stopWords();
        }

        @Override
        public void map(LongWritable file, Text line, Context context) throws InterruptedException, IOException {
            String[] split;
            split = line.toString().split("\t");
            if (!checkSplit(split))
                return;

            long occurrences = Long.parseLong(split[2]);
            context.getCounter(N.Counter).increment(occurrences);

            LongWritable part = new LongWritable(Math.round(Math.random()));
            context.write(new Text(split[0].toLowerCase()), new LongWritablePair(part, new LongWritable(occurrences)));
        }
    }


    public static class CountCombiner extends Reducer<Text, LongWritablePair, Text, LongWritablePair> {

        @Override
        public void reduce(Text key, Iterable<LongWritablePair> values,  Context context) throws InterruptedException, IOException {
            long n0 = 0;
            long n1 = 0;
            for(LongWritablePair value : values) {
                if (value.first.get() == 0) {
                    n0 += value.second.get();
                } else {
                    n1 += value.second.get();
                }
            }
            context.write(key, new LongWritablePair(new LongWritable(0), new LongWritable(n0)));
            context.write(key, new LongWritablePair(new LongWritable(1), new LongWritable(n1)));
        }

    }

    public static class CountReducer extends Reducer<Text, LongWritablePair, Text, LongWritablePair> {

        @Override
        public void reduce(Text key, Iterable<LongWritablePair> values, Context context) throws InterruptedException, IOException {
            long n0 = 0;
            long n1 = 0;
            for(LongWritablePair value : values) {
                if (value.first.get() == 0) {
                    n0 += value.second.get();
                } else {
                    n1 += value.second.get();
                }
            }
            context.write(key, new LongWritablePair(new LongWritable(n0), new LongWritable(n1)));
        }

    }
    private static void uploadCount(long count, String randomID) {
        InputStream input = new ByteArrayInputStream((count + "").getBytes());
        ObjectMetadata data = new ObjectMetadata();
        data.setContentLength((count + "").getBytes().length);

        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withRegion(Names.REGION)
                .build();
        PutObjectRequest putRequest = new PutObjectRequest(Names.NAME, randomID + "/total-count", input, data);
        s3.putObject(putRequest);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String randomID = args[0];
        Path input = new Path(Names.INPUT);
        Path output = new Path(Names.BUCKET + randomID + "/count-reduce" );
        boolean combiner = true;

        Job job = Job.getInstance(conf, "Count");
        job.setJarByClass(Count.class);
        job.setMapperClass(CountMap.class);

        if (combiner)
            job.setCombinerClass(CountCombiner.class);
        job.setReducerClass(CountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritablePair.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        boolean done = job.waitForCompletion(true);
        uploadCount(job.getCounters().findCounter(N.Counter).getValue(), randomID);
        System.exit(done ? 0 : 1);
    }
}
