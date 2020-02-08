
import java.io.IOException;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step10 {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        private Text _val = new Text();
        private Text _key = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");

            while (itr.hasMoreTokens()) {
                String[] parts = itr.nextToken().split("\t");
                _key.set(parts[0]);
                _val.set(parts[1]);

                context.write(_key, _val);
            }
        }
    }

    public static class ReducerClass extends Reducer<Text,Text,Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                String[] valParts = value.toString().split(" ");
                int N3 = Integer.parseInt(valParts[0]);
                int N2 = Integer.parseInt(valParts[1]);
                int N1 = Integer.parseInt(valParts[2]);
                int C2 = Integer.parseInt(valParts[3]);
                int C1 = Integer.parseInt(valParts[4]);
                long C0 = Long.parseLong(valParts[5]);
                double probability = calcProb(N3, N2, N1, C2, C1, C0);
                if (probability >= 0 && probability <= 1) {
                    context.write(key, new DoubleWritable(probability));
                }

            }
        }

        private double calcProb(double n3, double n2, double n1, double c2, double c1, double c0) {
            double k3 = (java.lang.Math.log(n3 + 1) + 1) / (java.lang.Math.log(n3 + 1) + 2);
            double k2 = (java.lang.Math.log(n2 + 1) + 1) / (java.lang.Math.log(n2 + 1) + 2);
            return (k3 * (n3 / c2) + (1 - k3) * k2 * (n2 / c1) + (1 - k3) * (1 - k2) * (n1 / c0));
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step 10");
        job.setJarByClass(Step10.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}