package steps;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
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
import utilities.LongTextWritablePair;
import utilities.Names;

import java.io.BufferedReader;

import java.io.IOException;
import java.io.InputStreamReader;



public class Formula {

    public static class FormulaMap extends Mapper<LongWritable, Text, Text, LongTextWritablePair> {


        @Override
        public void map(LongWritable file, Text line, Context context) throws InterruptedException, IOException {
            String[] split;
            split = line.toString().split("\t");

            Text nGram = new Text(split[0]);
            LongWritable sum = new LongWritable(Long.parseLong(split[1]));
            Text[] part = {new Text(split[2])};
            context.write(nGram, new LongTextWritablePair(sum, part));
        }

    }


    public static class FormulaReducer extends Reducer<Text, LongTextWritablePair, Text, DoubleWritable> {
        private static Double total;

        @Override
        public void setup(Context context) throws IOException {

            AmazonS3 s3 = AmazonS3ClientBuilder
                    .standard()
                    .withRegion(Names.REGION)
                    .build();
            String randomID = context.getConfiguration().get("ID");
            GetObjectRequest request = new GetObjectRequest(Names.NAME,randomID + "/total-count");
            S3Object object = s3.getObject(request);

            InputStreamReader streamReader = new InputStreamReader(object.getObjectContent());
            BufferedReader reader = new BufferedReader(streamReader);

            total = Double.parseDouble(reader.readLine());
        }

        @Override
        public void reduce(Text key, Iterable<LongTextWritablePair> values, Context context) throws InterruptedException, IOException {
            long n0, n1, t0, t1;
            n0 = n1 = t0 = t1 = 0;
            for (LongTextWritablePair pair : values) {
                switch (pair.second[0].toString()) {
                    case "N0":
                        n0 = pair.first.get();
                        break;
                    case "N1":
                        n1 = pair.first.get();
                        break;
                    case "T0":
                        t0 = pair.first.get();
                        break;
                    case "T1":
                        t1 = pair.first.get();
                }
            }

            double calc = (t0 + t1) / ((n0 + n1) * total);
            context.write(key, new DoubleWritable(calc));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        String randomID = args[0];
        config.set("ID", randomID);
        Path input = new Path(Names.BUCKET + randomID + "/ntCount-reduce");
        Path output = new Path(Names.BUCKET + randomID + "/formula-reduce");
        Job job = Job.getInstance(config, "Formula");
        job.setJarByClass(Formula.class);
        job.setMapperClass(FormulaMap.class);
        job.setReducerClass(FormulaReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongTextWritablePair.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
