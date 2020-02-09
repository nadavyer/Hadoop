import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step4 {
    private static final String SPACE = " ";
    private static final String FIRST_Of_KEYS = "\u0000";

    public static class ThirdsMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text _key = new Text();
        private Text _val = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                String[] parts = itr.nextToken().split("\t");
                String[] key_parts = parts[0].split(" ");
                if (key_parts.length < 3) {
                    continue;
                }
                _key.set(key_parts[1] + SPACE + key_parts[2]);// set w2 w3 as key
                _val.set(parts[0] + "\t" + parts[1]); // set w1 w2 w3 val as new val
                context.write(_key, _val);
            }
        }
    }

    public static class DoublesMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text _key = new Text();
        private Text _val = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                String[] parts = itr.nextToken().split("\t");
                String[] key_parts = parts[0].split(" ");
                if (key_parts.length < 2) {
                    continue;
                }
                _key.set(parts[0]);
                _val.set(FIRST_Of_KEYS + SPACE + parts[1]);
                context.write(_key, _val);
            }
        }
    }

    public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text _key = new Text();
            Text _val = new Text();
            String N2 = "0";
            String N3 = "0";
            for (Text value : values) {
                String[] parts = value.toString().split("\t");
                if (parts.length == 2) {
                    _key.set(parts[0]);
                    N3 = parts[1];
                } else {
                    N2 = value.toString().split(SPACE)[1];
                }
                if (!_key.toString().equals("") && !N2.equals("0")) {
                    _val.set(N3 + SPACE + N2);
                    context.write(_key, _val);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step 4");
        job.setJarByClass(Step4.class);
        job.setReducerClass(ReduceJoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ThirdsMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, DoublesMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        FileInputFormat.addInputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}